#!/usr/bin/env python3
"""
Arabic YouTube Channel Comment Scraper
=======================================
Input:  a text file with one YouTube channel URL per line
Output: <output_dir>/<channel_id>/comments_<channel_id>_<video_id>.jsonl
        One JSON object per line, one file per video.

Resume-safe: progress tracked in <output_dir>/progress.jsonl
             Already-completed videos are skipped on re-run.
             Partial files (from interrupted runs) are discarded and retried.

IP bypass options (same interface as fetch_transcripts.py):
  --webshare-user / --webshare-pass   Webshare rotating residential proxy
  --proxy URL                         Generic HTTP/SOCKS proxy
  --cookies FILE                      Netscape cookies.txt for yt-dlp enumeration

Usage:
    python fetch_comments.py channels.txt
    python fetch_comments.py channels.txt --webshare-user U --webshare-pass P
    python fetch_comments.py channels.txt --proxy http://user:pass@host:port
    python fetch_comments.py channels.txt --max-comments 2000 --workers 3
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------

def check_deps():
    missing = []
    for mod in ["youtube_comment_downloader", "requests"]:
        try:
            __import__(mod)
        except ImportError:
            missing.append(mod.replace("_", "-"))
    try:
        r = subprocess.run(["yt-dlp", "--version"], capture_output=True)
        if r.returncode != 0:
            missing.append("yt-dlp")
    except FileNotFoundError:
        missing.append("yt-dlp")
    if missing:
        print(f"[ERROR] Missing dependencies: {', '.join(missing)}")
        print(f"  pip install {' '.join(missing)} --break-system-packages")
        sys.exit(1)

check_deps()

import requests
from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR, SORT_BY_RECENT

# ---------------------------------------------------------------------------
# Proxy-aware downloader factory
# ---------------------------------------------------------------------------

def build_downloader(args) -> YoutubeCommentDownloader:
    """
    YoutubeCommentDownloader uses an internal requests.Session.
    We inject the proxy by patching that session after construction.
    """
    d = YoutubeCommentDownloader()

    proxy_url = None

    if args.webshare_user and args.webshare_pass:
        # Webshare rotating residential endpoint
        proxy_url = (
            f"http://{args.webshare_user}:{args.webshare_pass}"
            f"@p.webshare.io:80"
        )
    elif args.proxy:
        proxy_url = args.proxy

    if proxy_url:
        d.session.proxies = {"http": proxy_url, "https": proxy_url}

    if args.cookies:
        try:
            from http.cookiejar import MozillaCookieJar
            jar = MozillaCookieJar(args.cookies)
            jar.load(ignore_discard=True, ignore_expires=True)
            d.session.cookies.update(jar)
        except Exception as e:
            print(f"[WARN] Could not load cookies into comment downloader: {e}")

    return d

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(output_dir: Path) -> logging.Logger:
    log_path = output_dir / "errors.log"
    logger = logging.getLogger("comment_scraper")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

PROGRESS_STATUSES = {
    "success",
    "no_comments",    # video exists but comments are disabled or empty
    "unavailable",    # video private / deleted
    "ip_blocked",     # HTTP 429 / proxy error — retriable
    "error",          # unexpected error
}

class ProgressTracker:
    def __init__(self, progress_file: Path):
        self.path = progress_file
        self._lock = Lock()
        self._done: dict[str, dict] = {}
        self._load()

    def _load(self):
        if not self.path.exists():
            return
        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    self._done[rec["video_id"]] = rec
                except (json.JSONDecodeError, KeyError):
                    pass

    def is_done(self, video_id: str) -> bool:
        rec = self._done.get(video_id)
        if rec is None:
            return False
        # ip_blocked is retriable — never skip it
        return rec["status"] != "ip_blocked"

    def mark(self, channel_id: str, video_id: str, status: str,
             detail: str = "", comment_count: int = 0):
        assert status in PROGRESS_STATUSES
        rec = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "channel_id": channel_id,
            "video_id": video_id,
            "status": status,
            "comment_count": comment_count,
            "detail": detail,
        }
        with self._lock:
            self._done[video_id] = rec
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    @property
    def counts(self) -> dict[str, int]:
        from collections import Counter
        return dict(Counter(r["status"] for r in self._done.values()))

    @property
    def total_comments(self) -> int:
        return sum(r.get("comment_count", 0) for r in self._done.values())

# ---------------------------------------------------------------------------
# Channel → video ID enumeration via yt-dlp (shared with transcript scraper)
# ---------------------------------------------------------------------------

YTDLP_LIST_FLAGS = [
    "--flat-playlist",
    "--print", "%(id)s",
    "--no-warnings",
    "--ignore-errors",
    "--extractor-args", "youtube:skip=authcheck",
]

def ytdlp_base_cmd(args) -> list[str]:
    cmd = ["yt-dlp"]
    if args.cookies:
        cmd += ["--cookies", args.cookies]
    if args.proxy:
        cmd += ["--proxy", args.proxy]
    return cmd


def resolve_channel_id(channel_url: str, args, logger: logging.Logger) -> str | None:
    for pattern in [
        r"/(UC[\w-]{22})",
        r"/@([\w.-]+)",
        r"/c/([\w.-]+)",
        r"/user/([\w.-]+)",
    ]:
        m = re.search(pattern, channel_url)
        if m:
            return m.group(1)
    try:
        cmd = ytdlp_base_cmd(args) + [
            "--flat-playlist", "--print", "%(channel_id)s",
            "--playlist-end", "1", "--no-warnings", channel_url,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        ids = [l.strip() for l in result.stdout.splitlines() if l.strip()]
        if ids:
            return ids[0]
    except Exception as e:
        logger.warning(f"Could not resolve channel_id for {channel_url}: {e}")
    return None


def list_channel_videos(channel_url: str, args, logger: logging.Logger) -> list[str]:
    logger.info(f"Enumerating videos: {channel_url}")
    cmd = ytdlp_base_cmd(args) + YTDLP_LIST_FLAGS + [channel_url]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        logger.error(f"yt-dlp timed out enumerating {channel_url}")
        return []

    video_ids, seen = [], set()
    for line in result.stdout.splitlines():
        vid = line.strip()
        vid = re.sub(r".*[?&]v=", "", vid)
        vid = re.sub(r".*youtu\.be/", "", vid).split("?")[0].split("&")[0]
        if vid and re.fullmatch(r"[A-Za-z0-9_-]{11}", vid) and vid not in seen:
            seen.add(vid)
            video_ids.append(vid)

    if result.returncode != 0 and not video_ids:
        logger.error(
            f"yt-dlp exited {result.returncode} for {channel_url}:\n"
            f"{result.stderr[:500]}"
        )

    logger.info(f"  Found {len(video_ids)} unique video IDs")
    return video_ids

# ---------------------------------------------------------------------------
# IP block detection helpers
# ---------------------------------------------------------------------------

_IP_BLOCK_SIGNALS = (
    "429", "too many requests", "proxyerror", "proxy error",
    "connectionerror", "connection error", "refused", "timed out",
    "ipblocked", "requestblocked",
)

def _is_ip_block(err: Exception) -> bool:
    msg = str(err).lower().replace(" ", "")
    return any(s.replace(" ", "") in msg for s in _IP_BLOCK_SIGNALS)

# ---------------------------------------------------------------------------
# Per-video comment fetch
# ---------------------------------------------------------------------------

def fetch_and_save_comments(
    channel_id: str,
    video_id: str,
    out_dir: Path,
    tracker: ProgressTracker,
    downloader: YoutubeCommentDownloader,
    logger: logging.Logger,
    max_comments: int,
    rate_delay: float,
    sort_by: int,
) -> str:
    """Fetch all comments for one video, stream-write to JSONL, update tracker."""

    if tracker.is_done(video_id):
        return "skipped"

    time.sleep(rate_delay)

    video_url = f"https://www.youtube.com/watch?v={video_id}"
    out_path = out_dir / f"comments_{channel_id}_{video_id}.jsonl"

    # Remove any partial file from a previous interrupted run
    if out_path.exists():
        out_path.unlink()

    count = 0
    try:
        gen = downloader.get_comments_from_url(video_url, sort_by=sort_by)

        with open(out_path, "w", encoding="utf-8") as f:
            for comment in gen:
                # Annotate with provenance
                comment["video_id"] = video_id
                comment["channel_id"] = channel_id
                f.write(json.dumps(comment, ensure_ascii=False) + "\n")
                count += 1
                if max_comments and count >= max_comments:
                    break

    except Exception as e:
        # Clean up the partial file
        if out_path.exists():
            out_path.unlink()

        if _is_ip_block(e):
            tracker.mark(channel_id, video_id, "ip_blocked", str(e))
            logger.error(
                f"  [{channel_id}] {video_id}: IP BLOCKED — "
                f"add --webshare-user/pass or --proxy"
            )
            return "ip_blocked"

        err_str = str(e)
        # youtube-comment-downloader returns None generator for disabled comments
        if "failed to set sorting" in err_str.lower() or count == 0:
            tracker.mark(channel_id, video_id, "no_comments", err_str)
            logger.debug(f"  [{channel_id}] {video_id}: no comments / disabled")
            return "no_comments"

        tracker.mark(channel_id, video_id, "error", err_str)
        logger.warning(f"  [{channel_id}] {video_id}: error — {err_str[:120]}")
        return "error"

    if count == 0:
        # Generator returned nothing (comments disabled / empty)
        if out_path.exists():
            out_path.unlink()
        tracker.mark(channel_id, video_id, "no_comments", "", 0)
        logger.debug(f"  [{channel_id}] {video_id}: 0 comments")
        return "no_comments"

    tracker.mark(channel_id, video_id, "success", "", count)
    logger.info(f"  [{channel_id}] {video_id}: ✓  {count} comments")
    return "success"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Arabic YouTube channel comment scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("channels_file",
                        help="Text file — one YouTube channel URL per line")
    parser.add_argument("--output-dir", default="comments",
                        help="Root output directory (default: comments/)")
    parser.add_argument("--max-comments", type=int, default=0,
                        help="Max comments per video (default: 0 = unlimited)")
    parser.add_argument("--sort", choices=["popular", "recent"], default="recent",
                        help="Comment sort order (default: recent)")
    parser.add_argument("--workers", type=int, default=2,
                        help="Parallel workers (default: 2 — be conservative)")
    parser.add_argument("--delay", type=float, default=1.5,
                        help="Sleep seconds between requests per worker (default: 1.5)")

    bypass = parser.add_argument_group(
        "IP bypass (use one when running on a cloud server)"
    )
    bypass.add_argument("--webshare-user", metavar="USER",
                        help="Webshare rotating residential proxy username")
    bypass.add_argument("--webshare-pass", metavar="PASS",
                        help="Webshare rotating residential proxy password")
    bypass.add_argument("--proxy", metavar="URL",
                        help="Generic proxy, e.g. http://user:pass@host:port")
    bypass.add_argument("--cookies", metavar="FILE",
                        help="Netscape cookies.txt (used by yt-dlp for enumeration)")

    args = parser.parse_args()

    channels_path = Path(args.channels_file)
    if not channels_path.exists():
        print(f"[ERROR] File not found: {channels_path}")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(output_dir)
    tracker = ProgressTracker(output_dir / "progress.jsonl")

    sort_by = SORT_BY_POPULAR if args.sort == "popular" else SORT_BY_RECENT

    # Build a single downloader (one session, one proxy config)
    downloader = build_downloader(args)

    if args.webshare_user:
        logger.info("Proxy: Webshare rotating residential (p.webshare.io:80)")
    elif args.proxy:
        logger.info(f"Proxy: {args.proxy}")
    elif args.cookies:
        logger.info(f"Auth: cookies from {args.cookies} (yt-dlp only)")
    else:
        logger.warning(
            "No proxy configured — will likely be blocked on cloud IPs. "
            "Add --webshare-user/pass or --proxy to fix."
        )

    raw_lines = channels_path.read_text(encoding="utf-8").splitlines()
    channel_urls = [
        l.strip() for l in raw_lines if l.strip() and not l.startswith("#")
    ]
    logger.info(f"Loaded {len(channel_urls)} channel URL(s)")

    ip_blocked_total = 0

    for channel_url in channel_urls:
        channel_id = resolve_channel_id(channel_url, args, logger)
        if not channel_id:
            logger.error(f"Could not resolve channel_id for {channel_url} — skipping")
            continue

        logger.info(f"=== Channel: {channel_id} ===")

        video_ids = list_channel_videos(channel_url, args, logger)
        if not video_ids:
            logger.warning(f"No videos found for {channel_url}")
            continue

        new_ids = [v for v in video_ids if not tracker.is_done(v)]
        skip_count = len(video_ids) - len(new_ids)
        logger.info(f"  {len(new_ids)} to fetch, {skip_count} already done")

        ch_dir = output_dir / channel_id
        ch_dir.mkdir(exist_ok=True)

        ch_success = ch_ip_blocked = 0

        # NOTE: workers=2 by default — comment fetching is much heavier per
        # request than transcript fetching (many paginated XHR calls per video).
        # More workers = faster bans. Tune carefully.
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(
                    fetch_and_save_comments,
                    channel_id, vid, ch_dir, tracker,
                    downloader, logger,
                    args.max_comments, args.delay, sort_by,
                ): vid
                for vid in new_ids
            }
            for fut in as_completed(futures):
                vid = futures[fut]
                try:
                    status = fut.result()
                    if status == "success":
                        ch_success += 1
                    elif status == "ip_blocked":
                        ch_ip_blocked += 1
                        ip_blocked_total += 1
                except Exception as exc:
                    logger.error(f"  [{channel_id}] {vid}: unhandled — {exc}")
                    tracker.mark(channel_id, vid, "error", str(exc))

        logger.info(
            f"  Channel done: {ch_success}/{len(new_ids)} saved"
            + (f", {ch_ip_blocked} IP-blocked" if ch_ip_blocked else "")
        )

        if ch_ip_blocked == len(new_ids) > 0:
            logger.error(
                "All requests IP-blocked. Stopping early. "
                "Re-run with --webshare-user/pass or --proxy."
            )
            break

    counts = tracker.counts
    logger.info("=" * 60)
    logger.info("DONE")
    for status in ("success", "no_comments", "unavailable", "ip_blocked", "error"):
        n = counts.get(status, 0)
        if n:
            logger.info(f"  {status:<14}: {n}")
    logger.info(f"  total comments : {tracker.total_comments:,}")
    if ip_blocked_total:
        logger.warning(
            f"\n  {ip_blocked_total} videos were IP-blocked. Re-run with:\n"
            "    --webshare-user USER --webshare-pass PASS\n"
            "  (ip_blocked entries will be retried automatically)"
        )
    logger.info(f"  Progress log : {output_dir / 'progress.jsonl'}")
    logger.info(f"  Error log    : {output_dir / 'errors.log'}")


if __name__ == "__main__":
    main()