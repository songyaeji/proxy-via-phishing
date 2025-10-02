import sqlite3
import gzip
import argparse # URL ID와 타입을 쉽게 입력받기 위해 argparse 추가

# --- 스크립트 설정 ---
# 1. 커맨드 라인에서 인자(url_id, dom_type)를 받을 수 있도록 설정
# --- 사용 방법 ---
# python extract/extract_html_BLOB_test.py <url_id> [--type initial|final] [--db path/to/db]
# 예: python extract/extract_html_BLOB_test.py 123 --type final --db
parser = argparse.ArgumentParser(
    description="v2 스키마 DB에서 initial 또는 final DOM BLOB을 추출하고 압축 해제합니다."
)
parser.add_argument("url_id", type=int, help="확인하고 싶은 데이터의 url_id")
parser.add_argument(
    "--type", 
    type=str, 
    choices=['initial', 'final'], 
    default='final', 
    help="추출할 DOM 타입 (기본값: final)"
)
parser.add_argument("--db", default="db/translate_goog_urls.db", help="SQLite DB 경로")
args = parser.parse_args()


# --- 메인 로직 ---
# 2. DB에 연결
conn = sqlite3.connect(args.db)
cur = conn.cursor()

# 3. 인자에 따라 조회할 테이블과 컬럼 이름 결정
table_name = "url_artifacts_v2"
column_name = "initial_dom_html_gzip" if args.type == 'initial' else "final_dom_html_gzip"

print(f"테이블 '{table_name}'에서 url_id = {args.url_id}의 '{args.type}' DOM을 추출합니다...")

# 4. DB에서 압축된 데이터(BLOB) 가져오기 (수정된 쿼리)
try:
    query = f"SELECT {column_name} FROM {table_name} WHERE url_id = ?"
    cur.execute(query, (args.url_id,))
    result = cur.fetchone()

    if result and result[0]:
        compressed_html = result[0]
        
        # 5. Gzip 압축 해제 및 UTF-8로 디코딩
        try:
            decompressed_html = gzip.decompress(compressed_html).decode('utf-8')
            
            # 6. 결과 출력
            print("\n--- HTML 내용 ---")
            print(decompressed_html)
            
        except Exception as e:
            print(f"압축 해제 중 오류 발생: {e}")
    else:
        print(f"-> 해당 데이터가 없거나 DOM이 NULL입니다.")

except sqlite3.OperationalError as e:
    print(f"DB 오류 발생: {e}")
    print(f"-> '{table_name}' 테이블 또는 '{column_name}' 컬럼이 존재하는지 확인하세요.")

# 7. 연결 종료
conn.close()