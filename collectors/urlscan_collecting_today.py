from __future__ import annotations
import time
import sqlite3
import json
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple, List
from urllib.parse import urlparse
import requests
from tqdm import tqdm

URLSCAN_SEARCH_API = "https://urlscan.io/api/v1/search/"
DB_PATH = "db/translate_goog_urls.db"

# ----------------------------------------------------------------------
# 프록시 유형 식별
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class ProxyPattern:
    proxy_type: str
    host_contains: Tuple[str, ...]

PROXY_PATTERNS = [
    ProxyPattern("google_translate", ("translate.goog",)),
]


def guess_proxy_type_from_host(host: str) -> str:
    host = (host or "").lower()
    for pat in PROXY_PATTERNS:
        if any(hc in host for hc in pat.host_contains):
            return pat.proxy_type
    return "none"

from datetime import datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

def today_kst():
    return datetime.now(KST).date()

def get_latest_db_date(db_path: str, table: str = "urls", date_col: str = "collected_at"):
    """DB에 저장된 가장 최신 날짜(YYYY-MM-DD 문자열)를 date 객체로 반환. 없으면 None."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT DATE(MAX({date_col})) FROM {table}")
        row = cur.fetchone()
        return datetime.fromisoformat(row[0]).date() if row and row[0] else None
    finally:
        conn.close()


# ----------------------------------------------------------------------
# API Key 로테이터
# ----------------------------------------------------------------------
class KeyRotator:
    def __init__(self, keys: List[str]):
        self.keys = [k.strip() for k in keys if k and k.strip()]
        if not self.keys:
            raise ValueError("No valid API keys provided for KeyRotator.")
        self.idx = 0

    def current(self) -> str:
        return self.keys[self.idx]

    def rotate(self) -> str:
        self.idx = (self.idx + 1) % len(self.keys)
        print(f"Rotated to API Key #{self.idx + 1}")
        return self.current()

    def __len__(self):
        return len(self.keys)

# ----------------------------------------------------------------------
# DB 유틸
# ----------------------------------------------------------------------
def _ensure_schema_updated(cur: sqlite3.Cursor):
    # resume 토큰 저장용
    cur.execute("""
        CREATE TABLE IF NOT EXISTS _urlscan_collector_state (
            query TEXT PRIMARY KEY,
            last_search_after TEXT NOT NULL
        );
    """)
    # urls 테이블은 사전에 존재한다고 가정. urlscan_uuid 컬럼만 없으면 추가
    try:
        cur.execute("ALTER TABLE urls ADD COLUMN urlscan_uuid TEXT;")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            raise

def _get_resume_token(cur: sqlite3.Cursor, query: str) -> Optional[List[str]]:
    cur.execute("SELECT last_search_after FROM _urlscan_collector_state WHERE query = ?", (query,))
    row = cur.fetchone()
    if row and row[0]:
        try:
            token = json.loads(row[0])
            # 최소한의 형식 검증
            if _looks_like_ms13(token[0]) and _looks_like_uuid(token[1]):
                return token
        except Exception:
            return None
    return None

def _save_resume_token(cur: sqlite3.Cursor, query: str, search_after: List[str]):
    token_json = json.dumps(search_after)
    cur.execute("""
        INSERT INTO _urlscan_collector_state (query, last_search_after)
        VALUES (?, ?)
        ON CONFLICT(query) DO UPDATE SET last_search_after = excluded.last_search_after;
    """, (query, token_json))

def _clear_resume_token(cur: sqlite3.Cursor, query: str):
    cur.execute("DELETE FROM _urlscan_collector_state WHERE query = ?", (query,))

# ----------------------------------------------------------------------
# DB 행 존재 여부 및 삽입
# uuid 확인 후 중복 방지
# ----------------------------------------------------------------------
def _row_exists(cur: sqlite3.Cursor, urlscan_uuid: Optional[str]) -> bool:
    if not urlscan_uuid:
        return False
    cur.execute("SELECT 1 FROM urls WHERE urlscan_uuid = ? LIMIT 1;", (urlscan_uuid,))
    return cur.fetchone() is not None

def _insert_row(cur: sqlite3.Cursor, row: Dict[str, Any]) -> None:
    cols = ", ".join(row.keys())
    placeholders = ", ".join([":" + k for k in row.keys()])
    cur.execute(f"INSERT INTO urls ({cols}) VALUES ({placeholders});", row)

# ----------------------------------------------------------------------
# 형식 검증/정규화 유틸
# ----------------------------------------------------------------------
def _looks_like_ms13(x: str) -> bool:
    return isinstance(x, str) and x.isdigit() and len(x) == 13

def _normalize_ms13(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, (int,)):
        s = str(x)
        return s if _looks_like_ms13(s) else None
    if isinstance(x, float):
        s = str(int(x))
        return s if _looks_like_ms13(s) else None
    if isinstance(x, str):
        # 소수점 문자열일 수 있으니 정수부만 취급
        if "." in x:
            try:
                s = str(int(float(x)))
            except Exception:
                return None
        else:
            s = x
        return s if _looks_like_ms13(s) else None
    return None

def _looks_like_uuid(x: str) -> bool:
    return isinstance(x, str) and len(x) == 36 and x.count('-') == 4

# ----------------------------------------------------------------------
# URLScan API 호출 (params로 일괄 인코딩)
# ----------------------------------------------------------------------
def _call_urlscan_with_rotation(
    rotator: KeyRotator,
    query: str,
    size: int = 100,
    search_after: Optional[List[str]] = None,
    per_call_max_attempts: int = 10,
    base_backoff_sec: int = 5,
) -> Dict[str, Any]:
    attempts = 0
    initial_key_idx = rotator.idx
    session = requests.Session()

    while True:
        attempts += 1
        api_key = rotator.current()
        headers = {"API-Key": api_key, "Accept": "application/json"}

        params = {'q': query, 'size': str(size)}
        if search_after:
            # 여기서 최종적으로 조립
            params['search_after'] = ",".join(map(str, search_after))

        try:
            resp = session.get(URLSCAN_SEARCH_API, headers=headers, params=params, timeout=30)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait_sec = int(retry_after) if retry_after and retry_after.isdigit() else base_backoff_sec
                print(f"Rate limit hit. Waiting for {wait_sec}s before rotating key...")
                time.sleep(wait_sec)
                rotator.rotate()
            elif resp.status_code == 403:
                print("FATAL ERROR: API Key does not have permission for this query.")
                try:
                    print(resp.json())
                except Exception:
                    print(resp.text)
                resp.raise_for_status()
            elif resp.status_code == 400:
                print("FATAL ERROR: Bad Request. Check your query or parameters.")
                try:
                    print(resp.json())
                except Exception:
                    print(resp.text)
                resp.raise_for_status()
            elif 500 <= resp.status_code < 600:
                print(f"Server error ({resp.status_code}). Retrying after {base_backoff_sec}s...")
                time.sleep(base_backoff_sec)
            else:
                print(f"Unhandled error: {resp.status_code}")
                print(resp.text)
                resp.raise_for_status()

        except requests.exceptions.RequestException as e:
            print(f"Network error: {e}. Retrying after {base_backoff_sec}s...")
            time.sleep(base_backoff_sec)

        if rotator.idx == initial_key_idx and attempts > len(rotator):
            print("All API keys have been tried and failed. Raising exception.")
            raise RuntimeError("All available API keys failed with rate limits or errors.")

# ----------------------------------------------------------------------
# 결과 파싱
# ----------------------------------------------------------------------
def _parse_result_item(item: Dict[str, Any]) -> Dict[str, Any]:
    task = item.get("task", {}) or {}
    page = item.get("page", {}) or {}
    verdicts = item.get("verdicts", {}) or {}
    task_url = task.get("url")
    page_url = page.get("url")
    urlscan_ts = task.get("time")
    urlscan_uuid = task.get("uuid")
    return {
        "source": "urlscan",
        "proxy_type": guess_proxy_type_from_host((urlparse(page_url or task_url or "").netloc).lower()),
        "task_url": task_url,
        "page_url": page_url,
        "final_redirect_url": None,
        "score": (verdicts.get("overall", {}) or {}).get("score"),
        "malicious": (verdicts.get("overall", {}) or {}).get("malicious"),
        "country": page.get("country"),
        "ip": page.get("ip"),
        "http_requests": page.get("requests"),
        "unique_ips": page.get("uniqIPs"),
        "urlscan_timestamp": urlscan_ts,
        "urlscan_uuid": urlscan_uuid,
        "collected_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "status_checked": 0,
    }

# ----------------------------------------------------------------------
# 

# ----------------------------------------------------------------------


def _build_search_after_token(results: List[Dict[str, Any]]) -> Optional[List[str]]:
    if not results:
        return None

    # 빠른 경로: 마지막 아이템
    last = results[-1]
    sort = last.get("sort") or []
    _id = last.get("_id")
    ts = _normalize_ms13(sort[0] if len(sort) >= 1 else None)

    if ts and _looks_like_uuid(_id):
        return [ts, _id]

    # 폴백: 뒤에서 앞으로 훑어 유효 조합 찾기
    for item in reversed(results):
        sort = item.get("sort") or []
        _id = item.get("_id")
        ts = _normalize_ms13(sort[0] if len(sort) >= 1 else None)
        if ts and _looks_like_uuid(_id):
            return [ts, _id]

    # 끝까지 못 찾으면 None 반환 → 페이징 중단
    return None

# ----------------------------------------------------------------------
# 메인 수집 함수
# ----------------------------------------------------------------------
def collect_and_store(
    query: str,
    db_path: str,
    api_keys: List[str],
    proxy_type_hint: Optional[str] = None,
    max_pages: int = 1000,
    page_size: int = 100,
    stop_after_consecutive_empty_pages: int = 5,
) -> int:
    rotator = KeyRotator(api_keys)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    _ensure_schema_updated(cur)
    conn.commit()

    search_after_token = _get_resume_token(cur, query)
    inserted_this_run = 0
    consecutive_empty_pages = 0

    try:
        for _ in range(max_pages):
                data = _call_urlscan_with_rotation(
                    rotator=rotator,
                    query=query,
                    size=page_size,
                    search_after=search_after_token,
                )

                results = data.get("results", []) or []
                page_inserted_count = 0
                for item in results:
                    row = _parse_result_item(item)
                    if proxy_type_hint:
                        row["proxy_type"] = proxy_type_hint
                    if not _row_exists(cur, row["urlscan_uuid"]):
                        _insert_row(cur, row)
                        inserted_this_run += 1
                        page_inserted_count += 1
                
                conn.commit()

                # ✅ 견고한 search_after 토큰 구성
                search_after_token = _build_search_after_token(results)

                if search_after_token:
                    _save_resume_token(cur, query, search_after_token)
                    conn.commit()
                

                if page_inserted_count == 0 and results:
                    consecutive_empty_pages += 1
                else:
                    consecutive_empty_pages = 0
                
                if consecutive_empty_pages >= stop_after_consecutive_empty_pages:
                    print(f"\n[INFO] Found {consecutive_empty_pages} consecutive pages with no new data. Stopping collection.")
                    break

                if not search_after_token:
                    print("\nNo more pages found or could not build a valid search_after token. Collection complete for this query.")
                    break 

                time.sleep(0.2)    
    finally:
        conn.close()
        

    return inserted_this_run

def collect_from_today_to_db_latest(
    db_path: str,
    base_query: str,                 # cfg["query"] 같은 베이스 쿼리
    api_keys: List[str],
    proxy_type_hint: Optional[str] = None,
    max_pages: int = 1000,
    page_size: int = 100,
):
    start_date = today_kst()
    stop_date = get_latest_db_date(db_path, table="urls", date_col="urlscan_timestamp")
    if stop_date is None:
        # DB가 비어있으면 오늘 하루만 수집
        stop_date = start_date

    # 날짜 리스트 생성: today -> stop_date (내림차순)
    dates = []
    cur = start_date
    while cur >= stop_date:
        dates.append(cur)
        cur -= timedelta(days=1)

    # 날짜 단위 진행률 바 (원하면 생략 가능)
    for day in tqdm(dates, desc="Daily collection", unit="day"):
        # urlscan의 날짜 필터: "date:YYYY-MM-DD" (하루 단위)
        daily_query = f"{base_query} date:{day.isoformat()}"
        print(f"\n[INFO] Collecting for {day}  query={daily_query!r}")

        # 날짜가 바뀌면 resume 토큰 초기화(권장)
        conn = sqlite3.connect(db_path)
        try:
            _ensure_schema_updated(conn.cursor())
            _clear_resume_token(conn.cursor(), daily_query)
            conn.commit()
        finally:
            conn.close()

        inserted = collect_and_store(
            query=daily_query,
            db_path=db_path,
            api_keys=api_keys,
            proxy_type_hint=proxy_type_hint,
            max_pages=max_pages,
            page_size=page_size,
        )
        print(f"[INFO] {day} inserted={inserted}")
