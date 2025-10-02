# extract/extract_second_urls.py
"""
Extract 단계 스크립트 (collect-only 수집기와 호환)

역할
- urls 테이블 중 second_page_url 이 NULL인 행을 대상으로,
  프록시가 씌워진 URL(task_url / page_url / final_redirect_url)에서
  가능한 한 원래 목적지 URL을 유추해 second_page_url로 채움
- 가능한 경우 base_domain(원 도메인)도 함께 채움
- 이 단계는 네트워크 요청/크롤링 없이 문자열/휴리스틱만 사용

전제
- collectors/urlscan_collecting.py 실행 후 urls 테이블에 데이터가 있어야 함
- db/init_db.py 로 urls 스키마가 초기화되어 있어야 함
- DB 파일 경로는 프록시별로 다르며, 실행 시 인자로 받음
  예) python -m extract.extract_second_url --db db/translate_goog_urls.db
"""

from __future__ import annotations
import argparse
import sqlite3
from urllib.parse import urlparse, urlunparse
import re
from typing import List, Optional, Tuple

# ------------------------------------------------------------
# 유틸리티
# ------------------------------------------------------------
def _strip_domain_html_suffix(url: Optional[str]) -> Optional[str]:
    """
    /amazon.co.jp.html, /site4.sbisec.co.jp.html 같은
    '도메인처럼 보이는 베이스네임 + .html' 패턴을 '/도메인'으로 바꾼다.
    쿼리/프래그먼트는 유지.
    일반적인 /index.html 등은 건드리지 않는다.
    """
    if not url:
        return url
    try:
        p = urlparse(url)
    except Exception:
        return url

    path = p.path or "/"
    m = re.fullmatch(r"/((?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,})\.(?:html|htm)", path, flags=re.IGNORECASE)
    if not m:
        return url

    new_path = "/" + m.group(1)
    p = p._replace(path=new_path)
    return urlunparse(p)

def _normalize_host(host: str | None) -> str | None:
    return host.lower() if host else host

def _rebuild_url(scheme: str | None, netloc: str | None, path: str, query: str = "", fragment: str = "") -> str:
    return urlunparse((
        scheme or "https",
        netloc or "",
        path or "/",
        "",
        query or "",
        fragment or "",
    ))

def _pick_best_url(task_url: str | None, page_url: str | None) -> str | None:
    """
    URL 선택 우선순위:
      1) page_url
      2) task_url
    final_redirect_url은 제외 (상관없음)
    """
    return page_url or task_url

# ------------------------------------------------------------
# 프록시별 추출기
# ------------------------------------------------------------

import re
from urllib.parse import urlparse, urlunparse

def _rebuild_url(scheme: str, netloc: str, path: str, query: str, fragment: str) -> str:
    return urlunparse((scheme, netloc, path, "", query, fragment))

def _extract_google_translate(url: str) -> tuple[str | None, str | None]:
    parsed = urlparse(url or "")
    host = (parsed.netloc or "").lower()
    if not host or "translate.goog" not in host:
        return None, None

    core = host.split(".translate.goog", 1)[0]
    if not core:
        return None, None

    # 1) 주변에 다른 하이픈이 없는 "단일 하이픈"만 점(.)으로 교체
    candidate_host = re.sub(r'(?<!-)-(?!-)', '.', core)
    # 2) 두 개 이상 연속된 하이픈은 하이픈 하나로 압축
    candidate_host = re.sub(r'--+', '-', candidate_host)
    # 3) 혹시 모를 앞/뒤 점 정리
    candidate_host = candidate_host.strip('.')

    if "." not in candidate_host:
        return None, None

    # 루트 경로만 있으면 빈 경로로 만들어 슬래시가 안 붙도록
    path = parsed.path or ""
    if path == "/":
        path = ""

    clean_url = _rebuild_url(
        scheme="https",
        netloc=candidate_host,
        path=path,
        query="",      # 쿼리 제거
        fragment=""
    )
    base_domain = candidate_host
    return clean_url, base_domain

def _extract_yandex_translate(url: str): return None, None
def _extract_cloudflare_mirror(url: str): return None, None
def _extract_archiveis(url: str): return None, None

EXTRACTORS = {
    "google_translate": _extract_google_translate,
    "yandex_translate": _extract_yandex_translate,
    "cloudflare_mirror": _extract_cloudflare_mirror,
    "archiveis": _extract_archiveis,
}

# ------------------------------------------------------------
# DB 접근
# ------------------------------------------------------------

def _fetch_candidates(conn: sqlite3.Connection, limit: int | None = None) -> list[tuple]:
    cur = conn.cursor()
    sql = """
        SELECT id, proxy_type, task_url, page_url
        FROM urls
        WHERE second_page_url IS NULL OR second_page_url = ''
    """
    if limit:
        sql += " LIMIT ?"
        cur.execute(sql, (limit,))
    else:
        cur.execute(sql)
    return cur.fetchall()

def _update_row(conn: sqlite3.Connection, row_id: int, second_page_url: str | None, base_domain: str | None):
    if not second_page_url and not base_domain:
        return
    cur = conn.cursor()
    if second_page_url and base_domain:
        cur.execute(
            "UPDATE urls SET second_page_url = ?, base_domain = ? WHERE id = ?",
            (second_page_url, base_domain, row_id),
        )
    elif second_page_url:
        cur.execute(
            "UPDATE urls SET second_page_url = ? WHERE id = ?",
            (second_page_url, row_id),
        )
    else:
        cur.execute(
            "UPDATE urls SET base_domain = ? WHERE id = ?",
            (base_domain, row_id),
        )
    conn.commit()

# ------------------------------------------------------------
# 메인 로직
# ------------------------------------------------------------

def extract(db_path: str, batch_limit: int | None = None) -> tuple[int, int]:
    conn = sqlite3.connect(db_path)
    try:
        rows = _fetch_candidates(conn, limit=batch_limit)
        processed = 0
        updated = 0

        for row in rows:
            processed += 1
            row_id, proxy_type, task_url, page_url = row

            extractor = EXTRACTORS.get(proxy_type)
            base_url = _pick_best_url(task_url, page_url)

            if not base_url:
                continue

            # 1. 프록시 전용 추출기 실행
            second_page_url, base_domain = (extractor(base_url) if extractor else (None, None))

            # 2. 추출 실패 시 fallback: page_url 그대로 채움
            if not second_page_url and not base_domain and page_url:
                parsed = urlparse(page_url)
                second_page_url = page_url
                base_domain = parsed.netloc.lower() if parsed.netloc else None
            
            # 3. 도메인 뒤 .html 제거
            second_page_url = _strip_domain_html_suffix(second_page_url)

            # 4. 업데이트
            if second_page_url or base_domain:
                _update_row(conn, row_id, second_page_url, base_domain)
                updated += 1

        return processed, updated
    finally:
        conn.close()

# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
# ------------------------------------------------------------
# CLI / Self-test
# ------------------------------------------------------------

def _choose_extractor(proxy_type: Optional[str], base_url: Optional[str]):
    """
    proxy_type로 추출기를 고르고, 실패하면 URL 호스트를 검사해 translate.goog면
    google_translate 추출기를 강제로 선택한다(최후 보호장치).
    """
    key = (proxy_type or "").strip().lower()
    extractor = EXTRACTORS.get(key)
    if extractor is None and base_url:
        try:
            host = (urlparse(base_url).netloc or "").lower()
        except Exception:
            host = ""
        if "translate.goog" in host:
            extractor = _extract_google_translate
    return extractor

def _self_test(cases: List[tuple]):
    """
    cases: [(proxy_type, url)]
    각 케이스에 대해 어떤 추출기가 선택됐는지와 단계별 결과를 출력한다.
    """
    for i, (ptype, url) in enumerate(cases, 1):
        print(f"\n[case {i}] ptype={ptype!r}\n  base_url = {url}")
        ex = _choose_extractor(ptype, url)
        print(f"  extractor = {getattr(ex, '__name__', None)}")
        if ex is None:
            print("  -> extractor not selected (will fallback to page_url)\n")
            continue
        # extractor 실행
        out_url, base = ex(url)
        print(f"  raw_result: second_page_url={out_url}, base_domain={base}")
        out_url2 = _strip_domain_html_suffix(out_url)
        print(f"  after .html strip: {out_url2}")

def main():
    parser = argparse.ArgumentParser(
        description="Populate second_page_url/base_domain from proxy-wrapped URLs or run self-test"
    )
    parser.add_argument("--db", help="프록시별 DB 경로 (예: db/translate_goog_urls.db)")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--self-test", action="store_true", help="DB 대신 샘플 URL로 로컬 테스트")
    parser.add_argument(
        "--case", action="append", nargs=2, metavar=("PROXY_TYPE", "URL"),
        help="테스트 케이스 1건 추가 (여러 번 지정 가능)"
    )
    args = parser.parse_args()

    if args.self_test:
        # 사용자가 --case로 케이스를 주면 그걸 쓰고, 아니면 기본 예시 3건
        cases = args.case if args.case else [
            ("goog_translate", "https://z5fkshshf987rp.pages.dev.translate.goog/amazon.co.jp.html?_x_tr_sl=auto&_x_tr_tl=ko"),
            ("goog_translate", "https://k3mmuspercuav.pages.dev.translate.goog/site4.sbisec.co.jp?_x_tr_sl=auto&_x_tr_tl=ko"),
            ("google_translate", "https://support.apple.com.translate.goog/en-us/HT201232?_x_tr_sl=auto&_x_tr_tl=ko"),
        ]
        _self_test(cases)
        return

    # === DB 모드 ===
    if not args.db:
        raise SystemExit("--db 또는 --self-test 중 하나를 지정하세요")

    processed, updated = extract(args.db, batch_limit=args.limit)
    print(f"[extract] processed={processed}, updated={updated} (db={args.db})")

if __name__ == "__main__":
    main()
