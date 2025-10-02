# pipelines/02_extract_second_urls.py
"""
파이프라인 02: extract_second_urls 실행기

역할
- 프록시별 DB(여러 개)를 순회하며 extract 단계(extract/extract_second_urls.py)를 수행
- 각 DB에서 urls.second_page_url 이 NULL 또는 '' 인 레코드에 대해
  translate.goog 등 프록시 규칙 기반으로 second_page_url / base_domain 업데이트
  (추출 실패 시 page_url fallback 적용)

사용 예
- 기본(권장) 프록시 DB 목록 사용:
    python -m pipelines.02_extract_second_urls
- 특정 DB만 지정:
    python -m pipelines.02_extract_second_urls --db db/translate_goog_urls.db
- 여러 DB 지정:
    python -m pipelines.02_extract_second_urls --db db/translate_goog_urls.db --db db/yandex_translate.db
- 배치 제한(샘플 실행):
    python -m pipelines.02_extract_second_urls --limit 100
- skip-when-empty: second_page_url NULL/빈 문자열이 없는 DB는 건너뛰기
    python -m pipelines.02_extract_second_urls --skip-when-empty
"""

from __future__ import annotations
import argparse
import os
import sqlite3
from typing import List

# extract 모듈의 extract() 함수를 직접 호출
from extract.extract_second_urls import extract as run_extract


# 기본 대상 DB 목록 (필요에 따라 수정/추가)
DEFAULT_PROXY_DBS = [
    "db/translate_goog_urls.db",
]


def _count_null_second_page_rows(db_path: str) -> int:
    """해당 DB에서 second_page_url IS NULL 또는 '' 인 행 수 반환"""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(1) FROM urls WHERE second_page_url IS NULL OR second_page_url = '';")
        (cnt,) = cur.fetchone()
        return int(cnt or 0)
    finally:
        conn.close()


def _check_db_exists(db_path: str) -> bool:
    """파일이 존재하고 sqlite로 열 수 있는지 확인"""
    if not os.path.exists(db_path):
        return False
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;")
        conn.close()
        return True
    except Exception:
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline 02: Run extract_second_urls over proxy-specific DBs")
    parser.add_argument(
        "--db",
        action="append",
        dest="dbs",
        default=None,
        help="대상 DB 경로. 여러 번 지정 가능. 미지정 시 기본 목록 사용.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="각 DB에서 처리할 최대 row 수(배치 제한). 기본 무제한.",
    )
    parser.add_argument(
        "--skip-when-empty",
        action="store_true",
        help="해당 DB에 second_page_url NULL/빈 문자열이 없으면 건너뜀.",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # 대상 DB 목록 결정: --db 없으면 기본 목록 사용
    db_paths: List[str] = args.dbs if args.dbs else DEFAULT_PROXY_DBS

    # 존재하는 DB만 필터링
    filtered: List[str] = []
    for db in db_paths:
        if _check_db_exists(db):
            filtered.append(db)
        else:
            print(f"[warn] DB not found or invalid: {db}")

    if not filtered:
        print("[info] No valid DBs to process. Exiting.")
        return

    grand_total_processed = 0
    grand_total_updated = 0

    for db in filtered:
        if args.skip_when_empty:
            null_count = _count_null_second_page_rows(db)
            if null_count == 0:
                print(f"\n=== {db} ===")
                print("  [skip] No rows with second_page_url NULL or empty")
                continue

        print(f"\n=== Extract for DB: {db} ===")
        processed, updated = run_extract(db, batch_limit=args.limit)

        grand_total_processed += processed
        grand_total_updated += updated

        print(f"  processed={processed}, updated={updated}")

    print("\n=== SUMMARY ===")
    print(f"DBs processed : {len(filtered)}")
    print(f"rows processed: {grand_total_processed}")
    print(f"rows updated  : {grand_total_updated}")


if __name__ == "__main__":
    main()
