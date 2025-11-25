"""
실행방법: 
    python -m pipelines.01_collect_urls
역할
- urlscan.io API를 사용해 translate.goog 도메인 관련 URL 수집
- 수집된 URL을 DB에 저장 (db/init_db.py 참고)
- 이미 DB에 있는 URL은 중복 저장하지 않음
- 매일 실행하여 최신 데이터를 유지하는 용도
"""

# pipelines/01_collect_urls.py
import os
import sys

# collectors.urlscan_collecting에서 collect_and_store 함수를 임포트합니다.
from collectors.urlscan_collecting_today import collect_from_today_to_db_latest
from db.init_db import init_db
from config.urlscan_api import API_KEYS

# API Key 유효성 검사
if not API_KEYS:
    raise RuntimeError("config/urlscan_api.py 에 최소 1개 이상의 API Key가 필요합니다.")

# 수집 개수(페이지/사이즈) 설정
MAX_PAGES = int(os.getenv("URLSCAN_MAX_PAGES", "1000"))
PAGE_SIZE = int(os.getenv("URLSCAN_PAGE_SIZE", "100"))

# 프록시별 쿼리 & DB 매핑
PROXY_TARGETS = {
    "google_translate": {
        # API 기본 정렬(최신순)을 사용하므로 'sort' 조건은 필요 없습니다.
        "query": "domain:translate.goog",
        "db": "db/translate_goog_urls.db",
    },
    # "yandex_translate": { "query": "domain:translate.yandex.net", "db": "db/yandex_translate.db" },
}

def main():
    """
    각 프록시 타겟에 대해: 오늘 → DB 최신 날짜까지 하루 단위로 역방향 수집
    """
    for proxy_type, cfg in PROXY_TARGETS.items():
        print(f"\n{'='*20} Starting Collection for: {proxy_type.upper()} {'='*20}")
        db_path = cfg["db"]
        base_query = cfg["query"]

        # (기존대로) DB 초기화가 필요하다면 먼저 수행
        init_db(db_path)

        collect_from_today_to_db_latest(
            db_path=db_path,
            base_query=base_query,
            api_keys=API_KEYS,
            proxy_type_hint=proxy_type,
            max_pages=MAX_PAGES,
            page_size=PAGE_SIZE,
        )

        print(f"{'='*20} Finished Collection for: {proxy_type.upper()} {'='*20}")

if __name__ == "__main__":
    main()
