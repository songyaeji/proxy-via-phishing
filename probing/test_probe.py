# -*- coding: utf-8 -*-
"""
Quick probe tester for second_page_url logic.

Usage:
  python test_probe.py

Edit the `TEST_URLS` list below (or pass a file path via CLI) to try a few URLs before
running the full pipeline. This imports the probe helpers from your pipeline module so
behavior matches production (HEAD→GET with redirects; 2xx/3xx = success).

실행방법: 
    python probing/test_probe.py
"""

import argparse
from typing import Dict, Any
import importlib.util, pathlib, sys

# pipelines/03_04_extract_and_probe_recent_subpages.py 경로 계산
mod_path = (
    pathlib.Path(__file__).resolve().parent.parent / "pipelines" /
    "03_04_extract_and_probe_recent_subpages.py"
)

spec = importlib.util.spec_from_file_location("probe_mod", mod_path)
probe_mod = importlib.util.module_from_spec(spec)
sys.modules["probe_mod"] = probe_mod
assert spec.loader is not None
spec.loader.exec_module(probe_mod)

# 원하는 심볼 바인딩
is_target_row = probe_mod.is_target_row
http_probe = probe_mod.http_probe

# ----------------------
# Default test fixtures
# ----------------------
TEST_URLS = [
    {"id": 1, "second_page_url": "https://sekjlnourishmentself.sa.com/?s1=ser10"},  #접속가능
    {"id": 2, "second_page_url": "https://o1jcikt5dbfo.pages.dev/www.click-sec.com"}, #NXDOMAIN 화면 뜨는 싸이트
    {"id": 3, "second_page_url": "https://kele853c8fes.pages.dev/amazon.co.jp"}, #위험 페이지로 바로 뜨는 url
    {"id": 4, "second_page_url": "https://fdyprnwbs4plb.pages.dev"}, #NXDOMAIN 화면 뜨는 싸이트 
    {"id": 5, "second_page_url": "https://support.apple.com (sub_x)"}, #이미 마커 있음
    {"id": 6, "second_page_url": "https://ibp374enwe2x.pages.dev"}, #NXDOMAIN 화면 뜨는 싸이트
    {"id": 7, "second_page_url": "https://ibp374enwe2x.pages.dev/site4.sbisec.co.jp"}, #위험 페이지로 바로 뜨는 url
    {"id": 8, "second_page_url": "https://blkjessstrabeqxo.sa.com"}, #접속지연
    {"id": 9, "second_page_url": "https://blkjessstrabeqxo.sa.com/FCDoryPY"}, #접속지연
    {"id": 10, "second_page_url": "https://lechanvrierfrancais.com"}, #하위페이지 없어도 접속 가능
]


def read_urls_from_file(path: str) -> list[Dict[str, Any]]:
    """Read newline-separated URLs from a file into the row format the pipeline expects."""
    rows: list[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            url = line.strip()
            if not url or url.startswith("#"):
                continue
            rows.append({"id": i, "second_page_url": url})
    return rows


def main():
    ap = argparse.ArgumentParser(description="Lightweight tester for http_probe/is_target_row")
    ap.add_argument("--file", dest="file", default=None, help="Optional: path to a text file with one URL per line")
    ap.add_argument("--timeout", type=int, default=4, help="HTTP timeout seconds (default: 4)")
    ap.add_argument("--user-agent", dest="ua", default=None, help="Custom User-Agent (optional)")
    args = ap.parse_args()

    rows = TEST_URLS if not args.file else read_urls_from_file(args.file)

    print("\n[TEST] Probing URLs...\n")
    for row in rows:
        spu = row.get("second_page_url")
        rid = row.get("id")

        # The pipeline only processes rows that pass is_target_row
        target = is_target_row(row)
        if not target:
            print(f"id={rid} SKIP (not a target) -> {spu}")
            continue

        ok, code = http_probe(spu, timeout=args.timeout, ua=args.ua)
        status = "OK" if ok else "FAIL"
        print(f"id={rid} {status} {code or 'ERR'} -> {spu}")

    print("\n[TEST] Done. If results look good, run the full pipeline.\n")


if __name__ == "__main__":
    main()
