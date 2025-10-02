# ==================================================================================
# 파일명: extract_js_html_single_test.py (url_id 연동 버전)
#
# 목적:
#   - 단 하나의 URL을 즉시 분석하고, 'urls' 테이블과 연동하여
#     실제 url_id로 그 결과를 DB에 저장합니다.
#
# 사용법:
#   python extract/extract_js_html_single_test.py "https://j0v4lz-jpdp.idomp-set.workers.dev/https:/www.gosogle.com/"
# ==================================================================================

import argparse
import sys
import sqlite3
from datetime import datetime

import importlib.util
import pathlib

# --- 모듈 동적 로딩 시작 ---
module_path = (
    pathlib.Path(__file__).resolve().parent.parent / "pipelines" / "06_extract_js_html_v2.py"
)
spec = importlib.util.spec_from_file_location("pipeline_module", module_path)
pipeline_module = importlib.util.module_from_spec(spec)
sys.modules["pipeline_module"] = pipeline_module
if spec.loader:
    spec.loader.exec_module(pipeline_module)
else:
    raise ImportError(f"Could not load module from {module_path}")

# 불러온 모듈에서 필요한 함수들을 가져옵니다.
connect = pipeline_module.connect
ensure_schema = pipeline_module.ensure_schema
fetch_with_redirection_tracking = pipeline_module.fetch_with_redirection_tracking
upsert_artifact_v2 = pipeline_module.upsert_artifact_v2
is_unwanted_content = pipeline_module.is_unwanted_content
# --- 모듈 동적 로딩 종료 ---


def get_or_create_url_id(conn: sqlite3.Connection, url: str) -> int:
    """
    주어진 URL이 'urls' 테이블에 있는지 확인하고, 없으면 새로 추가한 후 id를 반환합니다.
    """
    cur = conn.cursor()
    # 1. 기존에 URL이 있는지 확인
    cur.execute("SELECT id FROM urls WHERE second_page_url = ?", (url,))
    result = cur.fetchone()
    
    if result:
        url_id = result[0]
        print(f"\n기존 URL을 'urls' 테이블에서 찾았습니다. (url_id: {url_id})")
        return url_id
    else:
        # 2. 없으면 새로 추가
        print(f"\n새로운 URL이므로 'urls' 테이블에 추가합니다...")
        cur.execute(
            "INSERT INTO urls (first_page_url, second_page_url, collected_at) VALUES (?, ?, datetime('now'))",
            (url, url) # first_page와 second_page에 동일한 URL을 입력
        )
        conn.commit()
        new_url_id = cur.lastrowid
        print(f"-> 추가 완료. (new url_id: {new_url_id})")
        return new_url_id


def run_test(db_path: str, url_to_test: str):
    """지정된 단일 URL을 테스트하고 실제 url_id로 결과를 DB에 저장합니다."""
    
    print(f"DB 경로: {db_path}")
    print(f"테스트 URL: {url_to_test}")
    
    conn = connect(db_path)
    ensure_schema(conn)

    # [수정] 테스트할 URL의 실제 url_id를 가져오거나 생성합니다.
    url_id_to_use = get_or_create_url_id(conn, url_to_test)
    
    print("\n데이터 수집을 시작합니다...")
    start_time = datetime.now()

    collection_data = fetch_with_redirection_tracking(url_to_test, timeout_ms=40000)
    
    end_time = datetime.now()
    print(f"수집 완료! (소요 시간: {end_time - start_time})")

    if collection_data['is_success']:
        print("최종 페이지 내용 필터링 중...")
        is_unwanted, reason = is_unwanted_content(collection_data['final_dom_html'], collection_data['final_http_status'])
        if is_unwanted:
            collection_data['is_success'] = False
            # "| unwanted:" 라는 접두사를 붙여 구체적인 필터링 이유 코드를 기록
            collection_data['error_message'] = (collection_data.get('error_message') or "").strip() + f" | unwanted:{reason}"
            print(f"-> 필터링됨 (이유: {reason})")
        else:
            print("-> 유효한 콘텐츠입니다.")
            
    # [수정] -1 대신 실제 url_id로 DB에 저장합니다.
    print(f"\nDB에 결과를 저장합니다... (url_id: {url_id_to_use})")
    upsert_artifact_v2(conn, url_id_to_use, collection_data)
    
    conn.close()
    
    print("\n--- 최종 수집 결과 요약 ---")
    print(f"사용된 url_id: {url_id_to_use}")
    print(f"최초 URL: {collection_data['initial_url']}")
    print(f"최종 URL: {collection_data['final_url']}")
    print(f"리디렉션 경로: ")
    for i, step in enumerate(collection_data['redirection_chain']):
        print(f"  [{i+1}] {step['url']} (Status: {step.get('status')})")
    print(f"성공 여부: {collection_data['is_success']}")
    if collection_data['error_message']:
        print(f"에러 메시지: {collection_data['error_message']}")
    print(f"\n테스트 완료. DB 뷰어에서 url_id = {url_id_to_use} 인 데이터를 확인하세요.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="단일 URL의 리디렉션과 DOM을 테스트하고 실제 ID로 저장합니다.")
    parser.add_argument("url", type=str, help="테스트할 전체 URL")
    parser.add_argument("--db", default="db/translate_goog_urls.db", help="SQLite DB 경로")
    
    if len(sys.argv) < 2:
        parser.print_help()
        sys.exit(1)
        
    args = parser.parse_args()
    
    run_test(args.db, args.url)