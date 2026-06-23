"""
process_movies.py
=================
Clean VTT transcripts, detect near-duplicate videos via MinHash LSH,
and write a deduplicated corpus for pretraining.

Deduplication strategy:
  - Exact duplicates   : matched by SHA-256 of cleaned text
  - Near-duplicates    : MinHash + LSH (Jaccard similarity over 5-grams)
    A pair is flagged as duplicate if Jaccard >= SIMILARITY_THRESHOLD

Usage:
    python process_movies.py \\
        --input  transcripts/ \\
        --output corpus.txt \\
        --threshold 0.8 \\
        --report  duplicates.tsv

Dependencies:
    pip install datasketch tqdm
"""

import argparse
import hashlib
import html
import re
from pathlib import Path

from datasketch import MinHash, MinHashLSH
from tqdm import tqdm


# ─────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────

NUM_PERM            = 128    # MinHash permutations — higher = more accurate
NGRAM_SIZE          = 5      # character n-gram size for shingling
SIMILARITY_THRESHOLD= 0.8    # Jaccard threshold above which videos are near-dupes


# ─────────────────────────────────────────────────────────────────
# VTT cleaning  (unchanged from original)
# ─────────────────────────────────────────────────────────────────

def clean_vtt(text: str) -> list[str]:
    text  = html.unescape(text)
    lines = text.splitlines()
    cleaned, seen_lines = [], set()

    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith(("WEBVTT", "Kind:", "Language:")):
            continue
        if "-->" in line:
            continue
        if re.fullmatch(r"(?:\[[^\]]*\]\s*)+", line):
            continue
        line = re.sub(r"\[[^\]]*\]", "", line)
        line = re.sub(r"<\d{2}:\d{2}:\d{2}\.\d+>", "", line)
        line = re.sub(r"</?c>", "", line)
        line = re.sub(r"^>>\s*", "", line)
        line = re.sub(r"<[^>]+>", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        if len(line) < 2 or line in seen_lines:
            continue
        cleaned.append(line)
        seen_lines.add(line)

    return cleaned


# ─────────────────────────────────────────────────────────────────
# Fingerprinting helpers
# ─────────────────────────────────────────────────────────────────

def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def make_minhash(text: str) -> MinHash:
    """Character n-gram MinHash signature of a cleaned transcript."""
    m = MinHash(num_perm=NUM_PERM)
    for i in range(len(text) - NGRAM_SIZE + 1):
        m.update(text[i : i + NGRAM_SIZE].encode())
    return m


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────

def main(input_dir: str, output_file: str,
         threshold: float, report_file: str) -> None:

    files = sorted(Path(input_dir).rglob("*"))
    files = [f for f in files if f.is_file()]
    print(f"Files found: {len(files):,}")

    # ── Pass 1: clean all files ──
    records = []   # (path, cleaned_text, sha)
    read_errors = 0

    for file in tqdm(files, desc="Cleaning"):
        try:
            raw     = file.read_text(encoding="utf-8", errors="ignore")
            cleaned = clean_vtt(raw)
            if not cleaned:
                continue
            text = " ".join(cleaned)
            records.append((file, text, sha256(text)))
        except Exception as e:
            print(f"[ERROR] {file}: {e}")
            read_errors += 1

    print(f"Non-empty transcripts: {len(records):,}  |  read errors: {read_errors}")

    # ── Pass 2: exact deduplication by SHA ──
    seen_sha: dict[str, Path] = {}
    exact_dupes: list[tuple[Path, Path]] = []
    unique_records = []

    for path, text, sha in records:
        if sha in seen_sha:
            exact_dupes.append((path, seen_sha[sha]))
        else:
            seen_sha[sha] = path
            unique_records.append((path, text))

    print(f"Exact duplicates removed : {len(exact_dupes):,}")
    print(f"Remaining after exact    : {len(unique_records):,}")

    # ── Pass 3: near-duplicate detection via MinHash LSH ──
    lsh        = MinHashLSH(threshold=threshold, num_perm=NUM_PERM)
    minhashes  = {}
    near_dupes : list[tuple[Path, Path]] = []
    keep_flags : dict[Path, bool] = {}

    for path, text in tqdm(unique_records, desc="MinHash"):
        mh  = make_minhash(text)
        key = str(path)

        # query before inserting — finds already-indexed near-neighbours
        neighbours = lsh.query(mh)
        if neighbours:
            # flag this file as duplicate of the first match
            near_dupes.append((path, Path(neighbours[0])))
            keep_flags[path] = False
        else:
            lsh.insert(key, mh)
            minhashes[key]   = mh
            keep_flags[path] = True

    kept_records = [(p, t) for p, t in unique_records if keep_flags[p]]
    print(f"Near-duplicates removed  : {len(near_dupes):,}  (threshold={threshold})")
    print(f"Unique videos kept       : {len(kept_records):,}")

    # ── Write corpus ──
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n\n".join(text for _, text in kept_records))
    print(f"\nCorpus written → {output_file}")

    # ── Write duplicate report ──
    all_dupes = (
        [(a, b, "exact")  for a, b in exact_dupes] +
        [(a, b, "near")   for a, b in near_dupes]
    )
    if all_dupes and report_file:
        with open(report_file, "w", encoding="utf-8") as f:
            f.write("duplicate\toriginal\ttype\n")
            for dup, orig, kind in all_dupes:
                f.write(f"{dup}\t{orig}\t{kind}\n")
        print(f"Duplicate report → {report_file}  ({len(all_dupes)} entries)")

    # ── Summary ──
    print(f"\n===== SUMMARY =====")
    print(f"Files scanned            : {len(files):,}")
    print(f"Non-empty transcripts    : {len(records):,}")
    print(f"Exact duplicates removed : {len(exact_dupes):,}")
    print(f"Near-duplicates removed  : {len(near_dupes):,}")
    print(f"Final unique videos      : {len(kept_records):,}")
    print(f"Output                   : {output_file}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--input",     default="transcripts/")
    p.add_argument("--output",    default="corpus.txt")
    p.add_argument("--threshold", type=float, default=SIMILARITY_THRESHOLD,
                   help="Jaccard similarity threshold (0–1). Lower = stricter.")
    p.add_argument("--report",    default="duplicates.tsv",
                   help="TSV file listing all duplicate pairs")
    args = p.parse_args()
    main(args.input, args.output, args.threshold, args.report)