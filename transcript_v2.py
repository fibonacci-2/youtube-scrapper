#!/usr/bin/env python3
"""
Arabic YouTube Channel Transcript Scraper
==========================================
Input:  a text file with one YouTube channel URL per line
Output: <output_dir>/<channel_id>/<channel_id>_<video_id>.vtt

Resume-safe: progress is tracked in <output_dir>/progress.jsonl.
Already-completed videos are always skipped on re-run.

IP-BAN WORKAROUNDS (in priority order, use whichever you have):
  1. Webshare rotating residential proxy  --webshare-user / --webshare-pass
  2. Generic HTTP/SOCKS proxy             --proxy http://user:pass@host:port
  3. yt-dlp cookie file (browser export)  --cookies cookies.txt
  4. No option given → bare requests (works locally, fails on most cloud IPs)

Usage examples:
    python fetch_transcripts.py channels.txt
    python fetch_transcripts.py channels.txt --webshare-user U --webshare-pass P
    python fetch_transcripts.py channels.txt --proxy socks5://127.0.0.1:9050
    python fetch_transcripts.py channels.txt --cookies ~/cookies.txt
    python fetch_transcripts.py channels.txt --output-dir /data/ar --workers 4
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
    try:
        import youtube_transcript_api  # noqa: F401
    except ImportError:
        missing.append("youtube-transcript-api")
    try:
        result = subprocess.run(["yt-dlp", "--version"], capture_output=True)
        if result.returncode != 0:
            missing.append("yt-dlp")
    except FileNotFoundError:
        missing.append("yt-dlp")
    if missing:
        print(f"[ERROR] Missing dependencies: {', '.join(missing)}")
        print(f"  pip install {' '.join(missing)} --break-system-packages")
        sys.exit(1)

check_deps()

from youtube_transcript_api import (  # noqa: E402
    YouTubeTranscriptApi,
    NoTranscriptFound,
    TranscriptsDisabled,
    VideoUnavailable,
)

# ---------------------------------------------------------------------------
# Proxy / auth configuration
# ---------------------------------------------------------------------------

def build_ytt_api(args) -> YouTubeTranscriptApi:
    """
    Return a YouTubeTranscriptApi instance configured for the chosen
    bypass strategy. Falls back gracefully if the proxies sub-module
    isn't available (older library version).
    """
    # --- Webshare rotating residential proxy (recommended for cloud) ---
    if args.webshare_user and args.webshare_pass:
        try:
            from youtube_transcript_api.proxies import WebshareProxyConfig
            return YouTubeTranscriptApi(
                proxy_config=WebshareProxyConfig(
                    proxy_username=args.webshare_user,
                    proxy_password=args.webshare_pass,
                )
            )
        except ImportError:
            print("[WARN] WebshareProxyConfig not available in this version of "
                  "youtube-transcript-api. Upgrade: pip install -U youtube-transcript-api")
            sys.exit(1)

    # --- Generic HTTP / SOCKS proxy ---
    if args.proxy:
        try:
            from youtube_transcript_api.proxies import GenericProxyConfig
            return YouTubeTranscriptApi(
                proxy_config=GenericProxyConfig(
                    http_url=args.proxy,
                    https_url=args.proxy,
                )
            )
        except ImportError:
            # Older API: pass proxies dict via requests.Session
            import requests
            session = requests.Session()
            session.proxies = {"http": args.proxy, "https": args.proxy}
            return YouTubeTranscriptApi(http_client=session)

    # --- Cookie-based auth (fallback; risks account ban on heavy use) ---
    # We handle cookies at the yt-dlp layer for enumeration; for transcript
    # fetching we pass them via a requests.Session if provided.
    if args.cookies:
        try:
            import requests
            from http.cookiejar import MozillaCookieJar
            jar = MozillaCookieJar(args.cookies)
            jar.load(ignore_discard=True, ignore_expires=True)
            session = requests.Session()
            session.cookies = jar
            try:
                return YouTubeTranscriptApi(http_client=session)
            except TypeError:
                # Even older API that doesn't accept http_client
                return YouTubeTranscriptApi()
        except Exception as e:
            print(f"[WARN] Could not load cookies for transcript API: {e}")

    # --- No bypass: bare requests (fine locally, blocked on cloud) ---
    return YouTubeTranscriptApi()


def ytdlp_base_cmd(args) -> list[str]:
    """Build the base yt-dlp command with optional cookie / proxy args."""
    cmd = ["yt-dlp"]
    if args.cookies:
        cmd += ["--cookies", args.cookies]
    if args.proxy:
        cmd += ["--proxy", args.proxy]
    return cmd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(output_dir: Path) -> logging.Logger:
    log_path = output_dir / "errors.log"
    logger = logging.getLogger("transcript_scraper")
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
# Progress tracking  (append-only JSONL, one record per video)
# ---------------------------------------------------------------------------

PROGRESS_STATUSES = {
    "success",
    "no_arabic",     # video exists but no Arabic transcript at all
    "disabled",      # transcripts disabled for this video
    "unavailable",   # video unavailable / private / deleted
    "ip_blocked",    # YouTube blocked our IP — needs proxy
    "error",         # unexpected error
}

class ProgressTracker:
    def __init__(self, progress_file: Path):
        self.path = progress_file
        self._lock = Lock()
        self._done: dict[str, str] = {}   # video_id -> status
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
                    self._done[rec["video_id"]] = rec["status"]
                except (json.JSONDecodeError, KeyError):
                    pass

    def is_done(self, video_id: str) -> bool:
        """Return True for any terminal status except ip_blocked (retriable)."""
        status = self._done.get(video_id)
        return status is not None and status != "ip_blocked"

    def mark(self, channel_id: str, video_id: str, status: str, detail: str = ""):
        assert status in PROGRESS_STATUSES, f"Unknown status: {status}"
        rec = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "channel_id": channel_id,
            "video_id": video_id,
            "status": status,
            "detail": detail,
        }
        with self._lock:
            self._done[video_id] = status
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    @property
    def counts(self) -> dict[str, int]:
        from collections import Counter
        return dict(Counter(self._done.values()))

# ---------------------------------------------------------------------------
# Channel → video ID enumeration via yt-dlp
# ---------------------------------------------------------------------------

YTDLP_LIST_FLAGS = [
    "--flat-playlist",
    "--print", "%(id)s",
    "--no-warnings",
    "--ignore-errors",
    "--extractor-args", "youtube:skip=authcheck",
]

def resolve_channel_id(channel_url: str, args, logger: logging.Logger) -> str | None:
    # Fast path from URL
    for pattern in [
        r"/(UC[\w-]{22})",
        r"/@([\w.-]+)",
        r"/c/([\w.-]+)",
        r"/user/([\w.-]+)",
    ]:
        m = re.search(pattern, channel_url)
        if m:
            return m.group(1)

    # Slow path: ask yt-dlp
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
# VTT formatting
# ---------------------------------------------------------------------------

def seconds_to_vtt(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"


def transcript_to_vtt(entries: list[dict]) -> str:
    lines = ["WEBVTT", ""]
    for i, entry in enumerate(entries, 1):
        start = entry.get("start", 0.0)
        duration = entry.get("duration", 2.0)
        end = start + duration
        text = entry.get("text", "").strip()
        if not text:
            continue
        lines += [text]
    return "\n".join(lines)

# ---------------------------------------------------------------------------
# Arabic transcript fetching
# ---------------------------------------------------------------------------

ARABIC_LANG_PRIORITY = [
    "ar",
    "ar-EG", "ar-SA", "ar-MA", "ar-DZ", "ar-TN", "ar-LY",
    "ar-SD", "ar-YE", "ar-SY", "ar-IQ", "ar-JO", "ar-LB",
    "ar-PS", "ar-KW", "ar-AE", "ar-BH", "ar-QA", "ar-OM",
]

_IP_BLOCK_PHRASES = (
    "ipblocked", "requestblocked", "ip_blocked", "blocking requests from your ip",
    "ip belonging to a cloud provider",
)

def _is_ip_block(err: Exception) -> bool:
    msg = str(err).lower()
    return any(p in msg for p in _IP_BLOCK_PHRASES)


def fetch_arabic_transcript(
    video_id: str, ytt: YouTubeTranscriptApi
) -> tuple[list[dict] | None, str]:
    """Returns (entries, lang_code) or (None, status/reason)."""
    try:
        transcript_list = ytt.list(video_id)
    except TranscriptsDisabled:
        return None, "disabled"
    except VideoUnavailable:
        return None, "unavailable"
    except Exception as e:
        if _is_ip_block(e):
            return None, "ip_blocked"
        return None, f"list_error:{e}"

    available: dict[str, object] = {}
    for t in transcript_list:
        available[t.language_code] = t

    def _to_entries(fetched) -> list[dict]:
        return [
            {
                "text": item.text if hasattr(item, "text") else item["text"],
                # "start": item.start if hasattr(item, "start") else item["start"],
                # "duration": item.duration if hasattr(item, "duration") else item["duration"],
            }
            for item in fetched
        ]

    # Try preferred codes in order
    for code in ARABIC_LANG_PRIORITY:
        if code in available:
            try:
                return _to_entries(available[code].fetch()), code
            except Exception as e:
                if _is_ip_block(e):
                    return None, "ip_blocked"
                continue

    # Try any ar-* code not in the explicit list
    for code, t in available.items():
        if code.startswith("ar"):
            try:
                return _to_entries(t.fetch()), code
            except Exception as e:
                if _is_ip_block(e):
                    return None, "ip_blocked"
                continue

    return None, "no_arabic"

# ---------------------------------------------------------------------------
# Per-video worker
# ---------------------------------------------------------------------------

def process_video(
    channel_id: str,
    video_id: str,
    out_dir: Path,
    tracker: ProgressTracker,
    ytt: YouTubeTranscriptApi,
    logger: logging.Logger,
    rate_delay: float,
) -> str:
    if tracker.is_done(video_id):
        return "skipped"

    time.sleep(rate_delay)

    entries, result = fetch_arabic_transcript(video_id, ytt)

    if entries is None:
        status = result if result in PROGRESS_STATUSES else "error"
        tracker.mark(channel_id, video_id, status, result)

        if status == "ip_blocked":
            logger.error(
                f"  [{channel_id}] {video_id}: IP BLOCKED ← add --webshare-user/pass "
                f"or --proxy to bypass"
            )
        elif status == "no_arabic":
            logger.debug(f"  [{channel_id}] {video_id}: no Arabic transcript")
        elif status in ("disabled", "unavailable"):
            logger.debug(f"  [{channel_id}] {video_id}: {status}")
        else:
            logger.warning(f"  [{channel_id}] {video_id}: error — {result}")
        return status

    vtt_text = transcript_to_vtt(entries)
    out_path = out_dir / f"{channel_id}_{video_id}.vtt"
    out_path.write_text(vtt_text, encoding="utf-8")

    tracker.mark(channel_id, video_id, "success", result)
    logger.info(f"  [{channel_id}] {video_id}: ✓  ({result}, {len(entries)} cues)")
    return "success"

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Arabic YouTube channel transcript scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--channels_file",default="channels.txt", help="Text file with one YouTube channel URL per line")
    parser.add_argument("--output-dir", default="transcripts", help="Root output directory (default: transcripts/)")
    parser.add_argument("--workers", type=int, default=3,
                        help="Parallel fetch workers (default: 3)")
    parser.add_argument("--delay", type=float, default=0.8,
                        help="Sleep seconds between requests per worker (default: 0.8)")

    # --- IP bypass options ---
    bypass = parser.add_argument_group(
        "IP bypass (use one of these when running on a cloud server)"
    )
    bypass.add_argument("--webshare-user", metavar="USER",
                        help="Webshare rotating residential proxy username")
    bypass.add_argument("--webshare-pass", metavar="PASS",
                        help="Webshare rotating residential proxy password")
    bypass.add_argument("--proxy", metavar="URL",
                        help="Generic proxy URL, e.g. http://user:pass@host:port "
                             "or socks5://127.0.0.1:9050")
    bypass.add_argument("--cookies", metavar="FILE",
                        help="Netscape/Mozilla cookies.txt exported from a browser "
                             "(passed to both yt-dlp and transcript API)")

    args = parser.parse_args()

    channels_path = Path(args.channels_file)
    if not channels_path.exists():
        print(f"[ERROR] File not found: {channels_path}")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    logger = setup_logging(output_dir)
    tracker = ProgressTracker(output_dir / "progress.jsonl")

    # Build transcript API once (proxy config is global per session)
    ytt = build_ytt_api(args)

    # Announce active bypass
    if args.webshare_user:
        logger.info("Proxy: Webshare rotating residential")
    elif args.proxy:
        logger.info(f"Proxy: {args.proxy}")
    elif args.cookies:
        logger.info(f"Auth: cookies from {args.cookies}")
    else:
        logger.warning(
            "No proxy/cookie configured — will be blocked on cloud IPs. "
            "Add --webshare-user/pass or --proxy to fix."
        )

    raw_lines = channels_path.read_text(encoding="utf-8").splitlines()
    channel_urls = [l.strip() for l in raw_lines if l.strip() and not l.startswith("#")]
    logger.info(f"Loaded {len(channel_urls)} channel URL(s) from {channels_path}")

    ip_blocked_count = 0

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

        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(
                    process_video,
                    channel_id, vid, ch_dir, tracker, ytt, logger, args.delay
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
                        ip_blocked_count += 1
                except Exception as exc:
                    logger.error(f"  [{channel_id}] {vid}: unhandled — {exc}")
                    tracker.mark(channel_id, vid, "error", str(exc))

        logger.info(
            f"  Channel done: {ch_success}/{len(new_ids)} saved"
            + (f", {ch_ip_blocked} IP-blocked" if ch_ip_blocked else "")
        )

        # If every new video was IP-blocked, no point continuing other channels
        if ch_ip_blocked == len(new_ids) > 0:
            logger.error(
                "All requests IP-blocked. Stopping early. "
                "Re-run with --webshare-user/pass or --proxy."
            )
            break

    # Final summary
    counts = tracker.counts
    logger.info("=" * 60)
    logger.info("DONE")
    for status in ("success", "no_arabic", "disabled", "unavailable", "ip_blocked", "error"):
        n = counts.get(status, 0)
        if n:
            logger.info(f"  {status:<12}: {n}")
    if ip_blocked_count:
        logger.warning(
            f"\n  {ip_blocked_count} videos were IP-blocked. Re-run with:\n"
            "    --webshare-user USER --webshare-pass PASS\n"
            "  (ip_blocked entries are NOT marked done, so they'll be retried)"
        )
    logger.info(f"  Progress log : {output_dir / 'progress.jsonl'}")
    logger.info(f"  Error log    : {output_dir / 'errors.log'}")


if __name__ == "__main__":
    main()