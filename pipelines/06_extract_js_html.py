# ==================================================================================
# 파일명: 06_extract_js_html.py
#
# 목적:
#   - 주어진 URL 목록을 headless 브라우저(Playwright)를 사용해 렌더링합니다.
#   - 렌더링이 완료된 최종 DOM(Document Object Model) HTML을 수집합니다.
#   - 페이지에 포함된 모든 외부 및 인라인 JavaScript 코드와 메타데이터를 수집합니다.
#   - 수집된 데이터를 SQLite 데이터베이스의 'url_artifacts' 테이블에 저장합니다.
#
# 주요 동작:
#   1. DB의 'urls' 테이블에서 아직 처리되지 않은 URL('second_page_url')을 가져옵니다.
#   2. 각 URL에 대해 Playwright를 실행하여 페이지를 동적으로 렌더링합니다.
#      - JavaScript 실행, 비동기 데이터 로딩(AJAX) 등이 완료된 후의 최종 상태를 확보합니다.
#   3. '가치 없는' 페이지를 필터링합니다.
#      - 404 오류, DNS 오류(NXDOMAIN), 피싱/악성코드 경고 페이지, 내용이 거의 없는 빈 페이지 등은
#        전체 데이터를 저장하지 않고, 필터링된 사유만 기록하여 저장 공간을 효율적으로 사용합니다.
#   4. 유효한 페이지의 경우, 다음 데이터를 수집합니다.
#      - DOM HTML: Gzip으로 압축하여 BLOB 형태로 저장합니다.
#      - 외부 JS: 페이지에 연결된 모든 외부 JS 파일의 URL, 응답 상태, 파일 해시(SHA256), MIME 타입 등을 수집합니다.
#      - 인라인 JS: <script> 태그 내에 직접 작성된 모든 JS 코드의 전체 내용을 수집합니다.
#   5. 수집된 모든 정보를 JSON 형식으로 직렬화하고 DB에 저장(UPSERT)합니다.
#      - '--update' 옵션을 사용하면 이미 처리된 URL도 강제로 다시 수집합니다.
#
# 사용법:
#   python -m pipelines.06_extract_js_html [--db db_path] [--limit N] [--timeout seconds]
#
# 이 스크립트는 정적 분석만으로는 파악하기 어려운, 동적으로 생성되는 웹페이지의
# 최종 모습과 실행 코드를 확보하는 데 핵심적인 역할을 합니다.
# 특히 피싱 사이트 분석 시 난독화된 JS나 동적으로 로드되는 스크립트를 수집하는 데 유용합니다.
# ==================================================================================

from __future__ import annotations
import argparse, json, re, sqlite3, time, hashlib, gzip
from io import BytesIO
from typing import Dict, List, Tuple
from urllib.parse import urlparse

import httpx
import certifi
from tqdm import tqdm
from playwright.sync_api import sync_playwright

# 분석 시 실제 사람의 브라우저처럼 보이게 하기 위한 표준 Chrome User-Agent
UA_CHROME = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
             "AppleWebKit/537.36 (KHTML, like Gecko) "
             "Chrome/124.0.0.0 Safari/537.36")

# URL 뒤에 붙는 불필요한 마커 제거용 정규식
MARKER_RE = re.compile(r"\s*(?:\(sub_o\)|\(sub_x\)|\(access\))\s*$", re.IGNORECASE)
# HTML에서 텍스트만 추출하기 위한 태그 제거용 정규식
TAG_RE = re.compile(r"<[^>]+>")
# 여러 공백(스페이스, 탭, 줄바꿈)을 하나의 스페이스로 정규화하기 위한 정규식
WS_RE  = re.compile(r"\s+")

# --- 수집 제외 필터링을 위한 정규식 목록 ---
# 여기에 새로운 규칙을 (정규식, 사유 메시지) 형태로 추가하여 쉽게 확장 가능
# 예: 특정 광고 플랫폼 페이지, 클라우드플레어 챌린지 페이지 등을 추가할 수 있음
UNWANTED_PATTERNS = [
    (re.compile(r"page not found|페이지를 찾을 수 없습니다|404 Not Found", re.IGNORECASE), "Page not found (404)"),
    (re.compile(r"server error|서버 오류|500 Internal", re.IGNORECASE), "Server error detected"),
    (re.compile(r"phishing|malware|deceptive site ahead|피싱|멀웨어|위험한 사이트", re.IGNORECASE), "Phishing/malware warning detected"),
    (re.compile(r"NXDOMAIN|DNS_PROBE_FINISHED_NXDOMAIN|This site can’t be reached|연결할 수 없습니다", re.IGNORECASE), "Domain/DNS resolution error"),
    (re.compile(r"Just a moment...|Verifying you are human|Checking your browser", re.IGNORECASE), "Bot/DDoS protection page"),
]

# ----------------- 유틸리티 함수 -----------------

def clean_second_url(u: str) -> str:
    """DB에서 가져온 URL의 양 끝 공백과 불필요한 마커를 제거합니다."""
    if not u: return u
    return MARKER_RE.sub("", u.strip())

def default_port(scheme: str) -> int:
    """URL 스킴(http/https)에 따른 기본 포트 번호를 반환합니다."""
    return 443 if scheme == "https" else 80

def same_origin(u: str, origin: str) -> bool:
    """두 URL이 동일 출처(Same-Origin) 정책을 따르는지 확인합니다.
    스킴, 호스트, 포트가 모두 같아야 동일 출처로 판단합니다.
    """
    try:
        pu, po = urlparse(u), urlparse(origin)
        return (pu.scheme, pu.hostname, pu.port or default_port(pu.scheme)) == \
               (po.scheme, po.hostname, po.port or default_port(po.scheme))
    except:
        return False

def gzip_bytes(s: str) -> bytes:
    """문자열을 UTF-8로 인코딩한 후 Gzip으로 압축합니다."""
    b = s.encode("utf-8")
    out = BytesIO()
    with gzip.GzipFile(fileobj=out, mode="wb") as f:
        f.write(b)
    return out.getvalue()

def make_preview_text(html: str, max_bytes: int) -> str:
    """HTML에서 태그와 연속 공백을 제거하여 순수 텍스트 프리뷰를 생성합니다."""
    txt = TAG_RE.sub(" ", html)
    txt = WS_RE.sub(" ", txt).strip()
    enc = txt.encode("utf-8")
    if len(enc) > max_bytes:
        # UTF-8 인코딩된 바이트를 기준으로 자른 후, 디코딩 오류는 무시
        txt = enc[:max_bytes].decode("utf-8", errors="ignore")
    return txt

def is_unwanted_content(html: str | None, status: int | None) -> Tuple[bool, str | None]:
    """
    수집할 가치가 없는 페이지(오류, 경고, 빈 페이지 등)인지 종합적으로 판단합니다.
    이 함수는 수집 효율성을 높이는 데 핵심적인 역할을 합니다.
    """
    # 1. HTTP 상태 코드로 1차 필터링 (4xx: 클라이언트 오류, 5xx: 서버 오류)
    if status and status >= 400:
        return True, f"HTTP Error Status: {status}"

    # 2. HTML 내용이 거의 없는 경우 (e.g., about:blank, 빈 응답)
    if not html or len(html.strip()) < 250: # 250 바이트 미만은 유의미한 콘텐츠가 없을 확률이 높음
        return True, "Page is empty or has minimal content"
    
    # 3. 텍스트 콘텐츠가 극도로 적은 경우
    preview = make_preview_text(html, 500)
    if len(preview) < 50: # 텍스트가 50자 미만인 페이지는 필터링
        return True, "Page has minimal text content"

    # 4. 미리 정의된 키워드 패턴으로 2차 필터링
    # 전체 HTML 대신 페이지 상단 일부(2KB)만 검사하여 성능 확보
    check_text = (html[:2048]).lower()
    for pattern, message in UNWANTED_PATTERNS:
        if pattern.search(check_text):
            return True, message

    return False, None # 모든 필터를 통과하면 수집 대상으로 판단

# ----------------- 데이터베이스 관련 함수 -----------------

def connect(db_path: str) -> sqlite3.Connection:
    """SQLite DB에 연결하고 성능 최적화 PRAGMA를 설정합니다."""
    conn = sqlite3.connect(db_path)
    # WAL 모드: 동시 읽기/쓰기 성능 향상
    conn.execute("PRAGMA journal_mode=WAL;")
    # 동기화 수준을 NORMAL로 설정하여 일반적인 상황에서 성능과 안정성의 균형을 맞춤
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def ensure_schema(conn: sqlite3.Connection):
    """'url_artifacts' 테이블과 인덱스가 없으면 생성합니다."""
    # [수정] js_urls, js_inline_snippets 제거, js_inline_full, js_inline_full_lines 추가
    conn.execute("""
    CREATE TABLE IF NOT EXISTS url_artifacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url_id INTEGER NOT NULL,
        second_page_url TEXT NOT NULL,
        dom_html_gzip BLOB,
        dom_html_size INTEGER,
        dom_html_preview TEXT,
        js_inline_full TEXT,                     -- [변경] 모든 인라인 JS (전체 내용)
        js_inline_full_lines INTEGER,            -- [추가] 인라인 JS 총 라인 수
        js_external_meta TEXT,
        network_post_logs TEXT,
        http_status INTEGER,
        is_success BOOLEAN,
        error_message TEXT,
        collected_at TEXT DEFAULT (DATETIME('now')),
        UNIQUE(url_id)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_url_artifacts_urlid ON url_artifacts(url_id);")
    conn.commit()

# ----------------- Playwright 렌더링 및 데이터 수집 함수 -----------------

# ----------------- Playwright 렌더링 및 데이터 수집 함수 (최종 수정본) -----------------

def fetch_rendered_bundle(url: str,
                          timeout_ms: int = 40000,
                          max_js_files: int = 20,
                          max_js_bytes: int = 200_000,
                          same_origin_only: bool = True,
                          verify_path: str | bool = certifi.where()
                         ) -> Tuple[str | None, List[Dict], List[str], int, int | None, List[Dict], str | None]:
    """
    Playwright를 사용해 URL을 렌더링하고, 최종 DOM과 모든 JS 코드 및 메타데이터를 수집합니다.

    반환 값:
    - dom_html: 렌더링이 완료된 최종 페이지의 HTML 콘텐츠
    - js_external_meta: 외부 JS 파일들의 메타데이터 리스트 (URL, 상태 코드, 해시 등)
    - js_inline_full: 페이지 내의 모든 인라인 스크립트 코드 리스트
    - js_inline_lines: 모든 인라인 스크립트의 총 라인 수
    - http_status: 페이지의 최종 HTTP 상태 코드
    - network_post_logs: 페이지에서 발생한 POST 요청 로그 리스트
    - nav_error_msg: 페이지 이동(goto) 중 발생한 오류 메시지
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(user_agent=UA_CHROME)
        page = ctx.new_page()

        post_logs: List[Dict] = []
        
        def on_request(req):
            if req.method == "POST" and req.post_data:
                try:
                    post_data_str = req.post_data
                    try:
                        post_data_parsed = json.loads(post_data_str)
                    except json.JSONDecodeError:
                        post_data_parsed = post_data_str
                    
                    post_logs.append({
                        "url": req.url,
                        "data": post_data_parsed,
                        "headers": {k: v for k, v in req.headers.items()}
                    })
                except Exception:
                    pass

        page.on("request", on_request)

        http_status = None
        dom_html = None
        nav_error_msg = None  # 내비게이션 오류 메시지를 저장할 변수

        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if resp:
                http_status = resp.status
            
            try:
                page.wait_for_load_state("load", timeout=timeout_ms / 2)
                page.wait_for_load_state("networkidle", timeout=timeout_ms / 3)
            except Exception:
                pass

        except Exception as e:
            nav_error_msg = str(e)
            if "net::ERR_NAME_NOT_RESOLVED" in nav_error_msg:
                http_status = -1 # DNS 조회 실패
            elif "timeout" in nav_error_msg.lower():
                http_status = -2 # 타임아웃
            else:
                http_status = -3 # 기타 내비게이션 오류

        try:
            dom_html = page.content()
        except Exception:
            dom_html = ""

        js_urls: List[str] = page.eval_on_selector_all("script[src]", "(els) => els.map(e => e.src)") or []
        
        js_inline_full: List[str] = page.eval_on_selector_all(
            "script",
            "(els)=>els.filter(e=>!e.src).map(e=>(e.textContent||''))"
        ) or []
        js_inline_lines = sum(s.count('\n') + 1 for s in js_inline_full if s)

        origin = page.url
        meta_list: List[Dict] = []
        with httpx.Client(verify=verify_path, follow_redirects=True, headers={"User-Agent": UA_CHROME}, timeout=10) as client:
            for js_url in js_urls[:max_js_files]:
                if same_origin_only and not same_origin(js_url, origin):
                    continue
                
                meta = {"url": js_url, "status": None, "sha256": None, "mime": None, "size": None}
                try:
                    resp = client.get(js_url)
                    meta["status"] = resp.status_code
                    if resp.status_code == 200:
                        content = resp.content[:max_js_bytes]
                        meta["sha256"] = hashlib.sha256(content).hexdigest()
                        meta["mime"] = resp.headers.get("content-type", "").split(";")[0].strip()
                        meta["size"] = len(content)
                except Exception:
                    pass
                meta_list.append(meta)

        ctx.close()
        browser.close()
        
        return dom_html, meta_list, js_inline_full, js_inline_lines, http_status, post_logs, nav_error_msg
# ----------------- 처리 대상 선택 함수 -----------------

def pick_targets(conn: sqlite3.Connection, limit: int | None, update: bool) -> List[Tuple[int, str]]:
    """DB에서 수집할 URL 목록을 조건에 맞게 가져옵니다."""
    cur = conn.cursor()
    if update:
        # --update: 기존 수집 여부와 상관없이 모든 유효 URL을 대상으로 함
        q = """
        SELECT u.id, u.second_page_url
        FROM urls u
        WHERE u.second_page_url IS NOT NULL AND TRIM(u.second_page_url) != ''
        ORDER BY u.id DESC
        """
    else:
        # 기본 동작: 아직 'url_artifacts'에 없는 URL만 대상으로 함
        q = """
        SELECT u.id, u.second_page_url
        FROM urls u
        LEFT JOIN url_artifacts a ON a.url_id = u.id
        WHERE u.second_page_url IS NOT NULL AND TRIM(u.second_page_url) != '' AND a.url_id IS NULL
        ORDER BY u.id DESC
        """
    if limit:
        q += f" LIMIT {int(limit)}"

    rows = cur.execute(q).fetchall()
    cleaned = [(rid, clean_second_url(u)) for rid, u in rows]
    # 유효한 http/https URL만 최종 타겟으로 선정
    return [(rid, u) for rid, u in cleaned if u and u.startswith(("http://","https://"))]

# ----------------- 데이터베이스 저장(UPSERT) 함수 -----------------

def upsert_artifact(conn: sqlite3.Connection,
                    url_id: int, second_url: str,
                    dom_html: str | None,
                    js_inline_full: List[str] | None, # [변경]
                    js_inline_lines: int,             # [추가]
                    js_external_meta: List[Dict] | None,
                    http_status: int | None, ok: bool, error: str | None,
                    preview_text: str | None,
                    network_logs: List[Dict] | None):

    dom_gzip, dom_len = None, None
    if dom_html:
        dom_len = len(dom_html.encode("utf-8"))
        dom_gzip = gzip_bytes(dom_html)
    
    # [수정] SQL 쿼리 변경
    conn.execute("""
    INSERT INTO url_artifacts
      (url_id, second_page_url,
       dom_html_gzip, dom_html_size, dom_html_preview,
       js_inline_full, js_inline_full_lines, js_external_meta, network_post_logs,
       http_status, is_success, error_message, collected_at)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME('now'))
    ON CONFLICT(url_id) DO UPDATE SET
      second_page_url      = excluded.second_page_url,
      dom_html_gzip        = excluded.dom_html_gzip,
      dom_html_size        = excluded.dom_html_size,
      dom_html_preview     = excluded.dom_html_preview,
      js_inline_full       = excluded.js_inline_full,
      js_inline_full_lines = excluded.js_inline_full_lines,
      js_external_meta     = excluded.js_external_meta,
      network_post_logs    = excluded.network_post_logs,
      http_status          = excluded.http_status,
      is_success           = excluded.is_success,
      error_message        = excluded.error_message,
      collected_at         = excluded.collected_at
    """, (
        url_id, second_url,
        dom_gzip, dom_len, preview_text,
        json.dumps(js_inline_full or [], ensure_ascii=False), # [변경]
        js_inline_lines, # [추가]
        json.dumps(js_external_meta or [], ensure_ascii=False),
        json.dumps(network_logs or [], ensure_ascii=False),
        http_status, 1 if ok else 0, (error or "")[:4000]
    ))
    conn.commit()

# ----------------- 메인 실행 로직 -----------------
# ----------------- 메인 실행 로직 (최종 수정본) -----------------
def main():
    ap = argparse.ArgumentParser(description="[수정됨] 렌더링된 DOM과 JS 메타데이터를 수집하고, 불필요한 페이지는 필터링하여 저장합니다.")
    ap.add_argument("--db", default="db/translate_goog_urls.db", help="SQLite DB 경로")
    ap.add_argument("--limit", type=int, default=50, help="처리할 최대 URL 개수")
    ap.add_argument("--timeout", type=int, default=40, help="Playwright 페이지 로드 타임아웃 (초)")
    ap.add_argument("--update", action="store_true", help="이미 수집된 URL도 강제로 갱신")
    ap.add_argument("--insecure", action="store_true", help="SSL 인증서 검증 해제 (보안 주의)")
    ap.add_argument("--same-origin-only", action="store_true", default=True, help="동일 출처 JS만 메타 수집")
    ap.add_argument("--max-dom-bytes", type=int, default=2_000_000, help="DOM 저장 최대 바이트 (압축 전)")
    ap.add_argument("--max-js-files", type=int, default=20, help="외부 JS 최대 다운로드 개수")
    ap.add_argument("--max-js-bytes", type=int, default=200_000, help="외부 JS 1개당 읽을 최대 바이트")
    ap.add_argument("--preview-bytes", type=int, default=20_000, help="dom_html_preview 최대 바이트")
    args = ap.parse_args()

    verify_path = False if args.insecure else certifi.where()
    conn = connect(args.db)
    ensure_schema(conn)

    targets = pick_targets(conn, args.limit, update=args.update)
    if not targets:
        print("처리할 대상이 없습니다.")
        return

    pbar = tqdm(total=len(targets), desc="Collect DOM + JS meta", unit="url")
    for url_id, second_url in targets:
        dom_html, js_inline_full, js_external_meta, network_logs = None, [], [], []
        js_inline_lines = 0
        http_status, preview_text, err_msg = None, None, None
        ok = False

        try:
            dom_html_raw, js_external_meta, js_inline_full, js_inline_lines, http_status, network_logs, nav_error_msg = fetch_rendered_bundle(
                second_url,
                timeout_ms=args.timeout * 1000,
                max_js_files=args.max_js_files,
                max_js_bytes=args.max_js_bytes,
                same_origin_only=args.same_origin_only,
                verify_path=verify_path
            )

            if nav_error_msg:
                is_unwanted, reason = True, nav_error_msg
            elif http_status is None or http_status < 0:
                is_unwanted, reason = True, f"Initial navigation failed (status: {http_status})"
            else:
                is_unwanted, reason = is_unwanted_content(dom_html_raw, http_status)

            if is_unwanted:
                ok = False
                err_msg = reason
                preview_text = reason
                dom_html, js_inline_full, js_external_meta, network_logs = None, [], [], []
                js_inline_lines = 0
            else:
                ok = bool(dom_html_raw and "Playwright.page.content() Error" not in dom_html_raw)
                
                if ok and dom_html_raw:
                    preview_text = make_preview_text(dom_html_raw, args.preview_bytes)

                    dom_html_encoded = dom_html_raw.encode("utf-8")
                    if len(dom_html_encoded) > args.max_dom_bytes:
                        dom_html = dom_html_encoded[:args.max_dom_bytes].decode("utf-8", errors="ignore")
                    else:
                        dom_html = dom_html_raw
                else:
                    err_msg = err_msg or "Content extraction failed or empty DOM"
                    preview_text = err_msg
                    dom_html = None
                    js_inline_full, js_external_meta, network_logs = [], [], []
                    js_inline_lines = 0

        except Exception as e:
            err_msg = f"critical_render_error: {e}"
            ok = False
            dom_html, js_inline_full, js_external_meta, network_logs = None, [], [], []
            js_inline_lines = 0
            http_status = None
            preview_text = str(e)[:200]

        upsert_artifact(
            conn, url_id, second_url,
            dom_html, js_inline_full, js_inline_lines, js_external_meta,
            http_status, ok, err_msg,
            preview_text=preview_text,
            network_logs=network_logs
        )
        pbar.update(1)
    
    pbar.close()
    conn.close()
    print("완료.")

if __name__ == "__main__":
    main()