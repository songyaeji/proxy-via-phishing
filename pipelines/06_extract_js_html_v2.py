# ==================================================================================
# 파일명: 06_extract_js_html_v2.py (리디렉션 추적 기능 추가 버전)
#
# 목적:
#   - 주어진 URL의 리디렉션 체인을 추적하고, 최초 진입 페이지와 최종 도착 페이지의
#     DOM HTML을 모두 수집합니다.
#   - 전체 리디렉션 경로, 각 페이지의 JS 메타데이터, 네트워크 로그 등을 수집합니다.
#   - 수집된 데이터를 SQLite 데이터베이스의 'url_artifacts_v2' 테이블에 저장합니다.
#
# 주요 변경점 (v1 대비):
#   1. 리디렉션 추적: 페이지가 다른 곳으로 이동할 때, 최대 5회까지 따라가며 전체 경로를 기록합니다.
#   2. 초기/최종 HTML 분리 저장: 최초 접속 페이지(점프 페이지)와 최종 도착 페이지의 HTML을
#      각각 'initial_dom_html_gzip'과 'final_dom_html_gzip'에 나누어 저장합니다.
#   3. DB 스키마 확장: 리디렉션 경로('redirection_chain')와 초기 HTML을 저장하기 위한
#      컬럼이 추가된 새로운 테이블('url_artifacts_v2')을 사용합니다.
#   4. 안정성 강화: 페이지 로드 시 'networkidle' 상태까지 대기하여, 빠르게 리디렉션되는
#      페이지의 데이터 수집 실패 가능성을 줄입니다.
#
# 사용법:
#   python -m pipelines.06_extract_js_html_v2 [--db db_path] [--limit N]
# ==================================================================================

from __future__ import annotations
import argparse, json, re, sqlite3, time, hashlib, gzip
from io import BytesIO
from typing import Dict, List, Tuple, Any
from urllib.parse import urlparse, urljoin

import httpx
import certifi
from tqdm import tqdm
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# (기존 유틸리티 함수들은 변경 없이 그대로 사용되므로 생략합니다)
# --- 기존 유틸리티 함수들 ---
UA_CHROME = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
             "AppleWebKit/537.36 (KHTML, like Gecko) "
             "Chrome/124.0.0.0 Safari/537.36")
MARKER_RE = re.compile(r"\s*(?:\(sub_o\)|\(sub_x\)|\(access\))\s*$", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE  = re.compile(r"\s+")
UNWANTED_PATTERNS = [
    (re.compile(r"page not found|페이지를 찾을 수 없습니다|404 Not Found", re.IGNORECASE), "keyword_not_found"),
    (re.compile(r"server error|서버 오류|500 Internal", re.IGNORECASE), "keyword_server_error"),
    (re.compile(r"phishing|malware|deceptive site ahead|피싱|멀웨어|위험한 사이트", re.IGNORECASE), "keyword_security_warning"),
    (re.compile(r"NXDOMAIN|DNS_PROBE_FINISHED_NXDOMAIN|This site can’t be reached|연결할 수 없습니다", re.IGNORECASE), "keyword_dns_error"),
    (re.compile(r"Just a moment...|Verifying you are human|Checking your browser", re.IGNORECASE), "keyword_bot_protection"),
]
def clean_second_url(u: str) -> str:
    if not u: return u
    return MARKER_RE.sub("", u.strip())
def default_port(scheme: str) -> int:
    return 443 if scheme == "https" else 80
def same_origin(u: str, origin: str) -> bool:
    try:
        pu, po = urlparse(u), urlparse(origin)
        return (pu.scheme, pu.hostname, pu.port or default_port(pu.scheme)) == \
               (po.scheme, po.hostname, po.port or default_port(po.scheme))
    except:
        return False
def gzip_bytes(s: str) -> bytes:
    b = s.encode("utf-8")
    out = BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb") as f:
        f.write(b)
    return out.getvalue()
def make_preview_text(html: str, max_bytes: int) -> str:
    txt = TAG_RE.sub(" ", html)
    txt = WS_RE.sub(" ", txt).strip()
    enc = txt.encode("utf-8")
    if len(enc) > max_bytes:
        txt = enc[:max_bytes].decode("utf-8", errors="ignore")
    return txt
def is_unwanted_content(html: str | None, status: int | None) -> Tuple[bool, str | None]:
    """
    수집할 가치가 없는 페이지인지 종합적으로 판단하고, 구체적인 '이유 코드'를 반환합니다.
    """
    if status and status >= 400:
        return True, f"http_error_{status}"

    if not html or len(html.strip()) < 250:
        return True, "minimal_content"
    
    # 텍스트 길이 필터링 (주석 처리하여 비활성화 가능)
    preview = make_preview_text(html, 500)
    # if len(preview) < 10:
    #     return True, "minimal_text"

    check_text = (html[:4096]).lower() # 검사 범위를 4KB로 늘려 정확도 향상
    for pattern, reason_code in UNWANTED_PATTERNS:
        if pattern.search(check_text):
            return True, reason_code

    return False, None
# -----------------

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def ensure_schema(conn: sqlite3.Connection):
    """'url_artifacts_v2' 테이블과 인덱스를 생성합니다. (스키마 변경됨)"""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS url_artifacts_v2 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url_id INTEGER NOT NULL,
        initial_url TEXT NOT NULL,
        
        -- 최종 도착 페이지 정보
        final_url TEXT,
        final_dom_html_gzip BLOB,
        final_dom_html_size INTEGER,
        final_dom_html_preview TEXT,
        final_http_status INTEGER,
        
        -- 최초 진입 페이지 정보
        initial_dom_html_gzip BLOB,
        
        -- 전체 수집 정보
        redirection_chain TEXT, -- JSON 형태의 리디렉션 경로
        js_external_meta TEXT,  -- 최종 페이지의 외부 JS
        js_inline_full TEXT,    -- 최종 페이지의 인라인 JS
        js_inline_full_lines INTEGER,
        network_post_logs TEXT,
        
        is_success BOOLEAN,
        error_message TEXT,
        collected_at TEXT DEFAULT (DATETIME('now')),
        UNIQUE(url_id)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_url_artifacts_v2_urlid ON url_artifacts_v2(url_id);")
    conn.commit()


def fetch_with_redirection_tracking(
    initial_url: str,
    timeout_ms: int = 40000,
    max_redirects: int = 5,
    same_origin_only: bool = True
) -> Dict[str, Any]:
    """
    [수정된 로직] Playwright를 사용해 URL 리디렉션 체인을 추적하고,
    최초 및 최종 페이지의 정보를 정확하게 분리하여 반환합니다.
    """
    results: Dict[str, Any] = {
        "initial_url": initial_url,
        "final_url": None,
        "initial_dom_html": None,
        "final_dom_html": None,
        "final_http_status": None,
        "redirection_chain": [],
        "js_external_meta": [],
        "js_inline_full": [],
        "js_inline_full_lines": 0,
        "network_post_logs": [],
        "error_message": None,
        "is_success": False
    }
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-features=SafeBrowsing"]  # 세이프 브라우징 기능 비활성화
        )
        ctx = browser.new_context(user_agent=UA_CHROME, java_script_enabled=True)
        page = ctx.new_page()

        try:
            # --- 1단계: 최초 URL로 이동하고 초기 DOM 즉시 수집 ---
            initial_resp = page.goto(initial_url, wait_until="networkidle", timeout=timeout_ms)
            results["initial_dom_html"] = page.content() # 최초 DOM 저장
            status = initial_resp.status if initial_resp else None
            results["redirection_chain"].append({"url": page.url, "status": status})

            # --- 2단계: 모든 리디렉션 추적 ---
            for _ in range(max_redirects):
                last_url = page.url
                try:
                    # URL이 바뀔 때까지 3초 대기
                    page.wait_for_url(lambda url: url != last_url, timeout=3000)
                    
                    # 리디렉션 발생: 새 URL과 상태를 체인에 추가
                    current_resp = page.main_frame.page.response
                    status = current_resp.status if current_resp else None
                    results["redirection_chain"].append({"url": page.url, "status": status})
                
                except PlaywrightTimeoutError:
                    # 더 이상 URL 변경이 없으면 리디렉션 추적 종료
                    break
            
            # --- 3단계: 추적이 모두 끝난 후, 최종 페이지의 DOM 수집 ---
            results["final_url"] = page.url
            results["final_dom_html"] = page.content() # 최종 DOM 저장
            if results["redirection_chain"]:
                results["final_http_status"] = results["redirection_chain"][-1]["status"]
            results["is_success"] = True

        except Exception as e:
            results["error_message"] = f"An error occurred: {e}"
            results["is_success"] = False
            # 에러 발생 시에도 현재까지의 정보는 최대한 기록
            results["final_url"] = page.url
            if not results["redirection_chain"]:
                 results["redirection_chain"].append({"url": initial_url, "status": -3})
        
        # --- 4단계: 최종 페이지의 메타데이터 수집 ---
        if results["is_success"]:
            try:
                js_urls: List[str] = page.eval_on_selector_all("script[src]", "(els) => els.map(e => e.src)") or []
                results["js_inline_full"] = page.eval_on_selector_all("script", "(els)=>els.filter(e=>!e.src).map(e=>(e.textContent||''))") or []
                results["js_inline_full_lines"] = sum(s.count('\n') + 1 for s in results["js_inline_full"] if s)

                with httpx.Client(verify=certifi.where(), follow_redirects=True, headers={"User-Agent": UA_CHROME}, timeout=10) as client:
                    for js_url in js_urls[:20]:
                        absolute_js_url = urljoin(results["final_url"], js_url)
                        if same_origin_only and not same_origin(absolute_js_url, results["final_url"]):
                            continue
                        
                        meta = {"url": absolute_js_url, "status": None, "sha256": None, "mime": None, "size": None}
                        try:
                            resp = client.get(absolute_js_url)
                            meta["status"] = resp.status_code
                            if resp.status_code == 200:
                                content = resp.content[:200_000]
                                meta["sha256"] = hashlib.sha256(content).hexdigest()
                                meta["mime"] = resp.headers.get("content-type", "").split(";")[0].strip()
                                meta["size"] = len(content)
                        except Exception: pass
                        results["js_external_meta"].append(meta)
            except Exception as e:
                results["error_message"] = results.get("error_message") or f"Metadata collection failed: {e}"
                results["is_success"] = False

        ctx.close()
        browser.close()
    return results

def pick_targets(conn: sqlite3.Connection, limit: int | None, update: bool) -> List[Tuple[int, str]]:
    cur = conn.cursor()
    if update:
        q = """SELECT u.id, u.second_page_url FROM urls u WHERE u.second_page_url IS NOT NULL AND TRIM(u.second_page_url) != '' ORDER BY u.id DESC"""
    else:
        # v2 테이블을 기준으로 수집 대상을 선정
        q = """
        SELECT u.id, u.second_page_url
        FROM urls u
        LEFT JOIN url_artifacts_v2 a ON a.url_id = u.id
        WHERE u.second_page_url IS NOT NULL AND TRIM(u.second_page_url) != '' AND a.url_id IS NULL
        ORDER BY u.id DESC
        """
    if limit:
        q += f" LIMIT {int(limit)}"
    rows = cur.execute(q).fetchall()
    cleaned = [(rid, clean_second_url(u)) for rid, u in rows]
    return [(rid, u) for rid, u in cleaned if u and u.startswith(("http://","https://"))]


def upsert_artifact_v2(conn: sqlite3.Connection, url_id: int, data: Dict[str, Any]):
    """수집된 리디렉션 추적 결과를 DB에 저장합니다 (v2 스키마)."""
    
    initial_gzip, final_gzip, final_size, final_preview = None, None, None, None
    if data.get("initial_dom_html"):
        initial_gzip = gzip_bytes(data["initial_dom_html"])
    if data.get("final_dom_html"):
        final_size = len(data["final_dom_html"].encode("utf-8"))
        final_gzip = gzip_bytes(data["final_dom_html"])
        final_preview = make_preview_text(data["final_dom_html"], 20000)

    conn.execute("""
    INSERT INTO url_artifacts_v2
      (url_id, initial_url, final_url, final_dom_html_gzip, final_dom_html_size, final_dom_html_preview, final_http_status,
       initial_dom_html_gzip, redirection_chain, js_external_meta, js_inline_full, js_inline_full_lines,
       network_post_logs, is_success, error_message, collected_at)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME('now'))
    ON CONFLICT(url_id) DO UPDATE SET
      initial_url = excluded.initial_url,
      final_url = excluded.final_url,
      final_dom_html_gzip = excluded.final_dom_html_gzip,
      final_dom_html_size = excluded.final_dom_html_size,
      final_dom_html_preview = excluded.final_dom_html_preview,
      final_http_status = excluded.final_http_status,
      initial_dom_html_gzip = excluded.initial_dom_html_gzip,
      redirection_chain = excluded.redirection_chain,
      js_external_meta = excluded.js_external_meta,
      js_inline_full = excluded.js_inline_full,
      js_inline_full_lines = excluded.js_inline_full_lines,
      network_post_logs = excluded.network_post_logs,
      is_success = excluded.is_success,
      error_message = excluded.error_message,
      collected_at = excluded.collected_at
    """, (
        url_id, data['initial_url'], data['final_url'], final_gzip, final_size, final_preview, data['final_http_status'],
        initial_gzip, json.dumps(data['redirection_chain'], ensure_ascii=False),
        json.dumps(data.get('js_external_meta', []), ensure_ascii=False),
        json.dumps(data.get('js_inline_full', []), ensure_ascii=False),
        data.get('js_inline_full_lines', 0),
        json.dumps(data.get('network_post_logs', []), ensure_ascii=False),
        1 if data['is_success'] else 0,
        (data.get('error_message') or "")[:4000]
    ))
    conn.commit()


def main():
    ap = argparse.ArgumentParser(description="[v2] 렌더링된 DOM과 JS 메타데이터를 수집하고, 리디렉션 체인을 추적하여 저장합니다.")
    ap.add_argument("--db", default="db/translate_goog_urls.db", help="SQLite DB 경로")
    ap.add_argument("--limit", type=int, default=50, help="처리할 최대 URL 개수")
    ap.add_argument("--timeout", type=int, default=40, help="Playwright 페이지 로드 타임아웃 (초)")
    ap.add_argument("--update", action="store_true", help="이미 수집된 URL도 강제로 갱신")

    args = ap.parse_args()

    conn = connect(args.db)
    ensure_schema(conn)

    targets = pick_targets(conn, args.limit, update=args.update)
    if not targets:
        print("처리할 대상이 없습니다.")
        return

    pbar = tqdm(total=len(targets), desc="[v2] Collect DOM + JS meta", unit="url")
    for url_id, initial_url in targets:
        collection_data = fetch_with_redirection_tracking(
            initial_url,
            timeout_ms=args.timeout * 1000,
            same_origin_only=True  # 인자로 직접 전달
        )
        
        # 가치 없는 페이지 필터링 (최종 페이지 기준)
        if collection_data['is_success']:
            is_unwanted, reason = is_unwanted_content(collection_data['final_dom_html'], collection_data['final_http_status'])
            if is_unwanted:
                collection_data['is_success'] = False
                collection_data['error_message'] = (collection_data['error_message'] or "") + f" | Unwanted content: {reason}"

        upsert_artifact_v2(conn, url_id, collection_data)
        pbar.update(1)
    
    pbar.close()
    conn.close()
    print("완료.")

if __name__ == "__main__":
    main()