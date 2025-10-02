"""
사용방법:
    python -m pipelines.05_extract_redirection_url
역할
- DB 'urls' 테이블의 'second_page_url'을 분석합니다.
- extract.extract_redirect_urls의 analyze_script_redirects() 함수를 사용합니다.
- 스크립트에서 추출한 URL 목록을 'script_redirect_url' 컬럼에 저장합니다.
- 추출 근거가 되는 코드 스니펫을 'redirect_snippet' 컬럼에 저장합니다.
- [수정] 1건 처리 시마다 결과를 DB에 즉시 저장하여 안정성을 극대화합니다.
- 멀티스레드로 병렬 처리하며, 기존에 값이 있는 행은 건너뜁니다.
- 완료 후 처리 통계를 출력합니다.

"""

# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
import sqlite3
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from extract.extract_redirect_urls import analyze_script_redirects

# tqdm이 없을 경우를 대비한 간단한 대체 클래스
try:
    from tqdm import tqdm as _tqdm
except ImportError:  # pragma: no cover
    class _tqdm: # fallback
        def __init__(self, total=0, initial=0, desc=None):
            self.total, self.n, self.desc = total, initial, desc or "progress"
            print(f"[{self.desc}] 시작: {self.n}/{self.total}")
        def update(self, n=1):
            self.n += n
            if self.n % 100 == 0 or self.n == self.total:
                 print(f"[{self.desc}] 진행: {self.n}/{self.total}")
        def set_postfix(self, **kwargs): pass
        def close(self): print(f"[{self.desc}] 완료: {self.n}/{self.total}")

# --- 설정값 ---
TARGET_DB = "db/translate_goog_urls.db"
TABLE_NAME = "urls"
IN_COL = "second_page_url"
OUT_COL_REDIRECTS = "script_redirect_url"
OUT_COL_SNIPPETS = "redirect_snippet"
# [제거] 배치 처리 로직을 사용하지 않으므로 BATCH_SIZE 제거

def verify_table_columns(conn: sqlite3.Connection, table: str, required_cols: List[str]):
    """지정된 테이블에 필요한 컬럼들이 모두 존재하는지 확인합니다."""
    try:
        all_required = required_cols + ['id']
        cols_query = conn.execute(f"PRAGMA table_info('{table}')")
        cols = {row[1] for row in cols_query}
        missing_cols = [c for c in all_required if c not in cols]
        if missing_cols:
            raise RuntimeError(f"'{table}' 테이블에 필요한 컬럼이 없습니다: {missing_cols}")
        print(f"[i] 테이블 '{table}' 및 필요 컬럼 확인 완료.")
    except sqlite3.OperationalError:
        raise RuntimeError(f"'{table}' 테이블을 찾을 수 없습니다.")


def counts(conn: sqlite3.Connection, table: str) -> Tuple[int, int]:
    """전체 행과 이미 처리된 행의 수를 셉니다."""
    total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    completed = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE TRIM(COALESCE({OUT_COL_REDIRECTS}, '')) <> ''"
    ).fetchone()[0]
    return total, completed

def rows_to_process(conn: sqlite3.Connection, table: str, overwrite: bool) -> List[Tuple[int, str]]:
    """처리해야 할 행(id, url)의 목록을 가져옵니다."""
    if overwrite:
        q = f"SELECT id, {IN_COL} FROM {table}"
    else:
        q = f"""
        SELECT id, {IN_COL}
        FROM {table}
        WHERE TRIM(COALESCE({OUT_COL_REDIRECTS}, '')) = ''
        """
    return conn.execute(q).fetchall()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=TARGET_DB, help="SQLite DB 경로")
    ap.add_argument("--table", default=TABLE_NAME, help="작업을 수행할 테이블 이름")
    ap.add_argument("--workers", type=int, default=10, help="병렬 작업 스레드 수")
    ap.add_argument("--overwrite", action="store_true", help="기존에 값이 있어도 덮어쓰기")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)

    table = args.table
    required = [IN_COL, OUT_COL_REDIRECTS, OUT_COL_SNIPPETS]
    verify_table_columns(conn, table, required)

    total_rows, completed_rows = counts(conn, table)
    to_do = rows_to_process(conn, table, args.overwrite)

    if not to_do:
        print(f"[i] 처리할 데이터가 없습니다. (총 {total_rows}개 중 {completed_rows}개 완료됨)")
        return

    initial_progress = 0 if args.overwrite else completed_rows
    bar = _tqdm(total=total_rows, initial=initial_progress, desc="URL 분석")
    ok_cnt = 0
    err_cnt = 0

    def work(item_id: int, url: str) -> Tuple[int, Optional[str], Optional[str], bool]:
        redirect_urls, snippets, err = analyze_script_redirects(url or "")
        is_ok = err is None
        if is_ok:
            return item_id, redirect_urls, snippets, True
        else:
            return item_id, err or "ERR:unknown", "", False

    # [제거] 결과를 모아두는 리스트 제거
    # results: List[Tuple[int, Optional[str], Optional[str]]] = []
    
    update_query = f"UPDATE {table} SET {OUT_COL_REDIRECTS}=?, {OUT_COL_SNIPPETS}=? WHERE id=?"

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(work, item_id, url): item_id for (item_id, url) in to_do}
        for fut in as_completed(futs):
            # [수정] 1건의 결과가 나올 때마다 즉시 DB에 저장
            item_id = futs[fut]
            try:
                returned_id, redirect_val, snippet_val, ok = fut.result()
                params = (redirect_val, snippet_val, returned_id)
                ok_cnt += 1 if ok else 0
                err_cnt += 1 if not ok else 0
            except Exception as e:
                # 작업 스레드 자체에서 예외 발생 시 에러 처리
                error_msg = f"ERR:worker:{type(e).__name__}"
                params = (error_msg, "", item_id)
                err_cnt += 1
            
            # DB에 1건 업데이트하고 즉시 commit
            conn.execute(update_query, params)
            conn.commit()

            bar.update(1)
            bar.set_postfix(ok=ok_cnt, err=err_cnt)

    # [제거] 루프 종료 후 최종 저장하는 로직 제거
    
    bar.close()
    # [수정] 최종 업데이트 건수는 ok_cnt + err_cnt로 계산
    updated_count = ok_cnt + err_cnt
    print(f"\n[결과] 테이블: {table}, 전체: {total_rows}, 사전완료: {completed_rows}")
    print(f"-> 이번 실행: {len(to_do)}건 처리, {updated_count}건 업데이트 (성공: {ok_cnt}, 실패: {err_cnt})")


if __name__ == "__main__":
    main()