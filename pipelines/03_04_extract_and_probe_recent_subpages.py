
"""
03_04_extract_and_probe_recent_subpages.py
파이프라인 03-04: 최근 관측된 하위경로(subpage) 붙여서 probe 후 갱신

🔄 전체 실행 흐름 다이어그램:
┌─────────────────────────────────────────────────────────────────┐
│ 1. 초기화: 관측 큐 생성 (최근 하위경로 저장용)                    │
├─────────────────────────────────────────────────────────────────┤
│ 2. DB 연결 및 데이터 로드 (id 오름차순)                          │
├─────────────────────────────────────────────────────────────────┤
│ 3. 각 행 순회 시작                                               │
│    ┌─────────────────────────────────────────────────────────┐   │
│    │ 3-1. 대상 행인가? (second_page_url 있음 + path 없음)     │   │
│    │     ↓ YES                                               │   │
│    │ 3-2. 직접 접속 시도                                     │   │
│    │     ↓ 성공 → (access) 마커 추가                          │   │
│    │     ↓ 실패 → 3-3으로                                   │   │
│    │ 3-3. 하위경로 후보들 병렬 probe                         │   │
│    │     ↓ 성공 → (sub_o) 마커 추가                          │   │
│    │     ↓ 실패 → (sub_x) 마커 추가                          │   │
│    │ 3-4. DB 업데이트                                        │   │
│    └─────────────────────────────────────────────────────────┘   │
│    ┌─────────────────────────────────────────────────────────┐   │
│    │ 3-5. 관측 큐 갱신 (모든 행에 대해)                      │   │
│    │     - 유효한 하위경로 발견 시 큐에 추가                  │   │
│    └─────────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────────┤
│ 4. 완료 및 요약 출력                                            │
└─────────────────────────────────────────────────────────────────┘

실행방법
---------
- python -m pipelines.03_04_extract_and_probe_recent_subpages

목적(갱신)
---------
- `second_page_url` 컬럼을 기준으로, 하위경로가 없는 값(= path가 비었거나 '/') 을 가진 행만 대상으로 삼는다.
- 관측 큐에 누적된 "최근 하위경로들"을 origin(second_page_url 기준)에 하나씩 붙여 probe.
- 성공 시: `second_page_url = "<cand> (sub_o)"`
  실패 시: `second_page_url = "<원래 second_page_url> (sub_x)"`
- 이미 '(sub_o)/(sub_x)/(access)' 마커가 붙은 행은 재시도하지 않는다.

핵심 규칙
---------
- 대상 행 판정:
    * second_page_url IS NOT NULL
    * second_page_url 에 '(sub_' 문자열이 없음 
    * second_page_url 의 path 가 '' 또는 '/'
- 관측 큐: rowid 오름차순 전체 순회 중, 이전 행들에서 보인 "하위경로"들을 저장(기본 50개).
  - 우선순위: second_page_url 의 path → 없다면 url 의 path도 보조로 사용할 수 있음(옵션).

📋 하위경로 처리 상세 분석
-------------------------
🔄 하위경로 추출 과정:
1. 관측 단계: 모든 행에서 second_page_url의 경로 추출
2. 정규화 단계: 추출된 경로를 정리하고 유효성 검사
3. 큐 저장 단계: 유효한 하위경로를 관측 큐에 저장 (최대 50개)
4. 적용 단계: 대상 행에 하위경로를 붙여서 probe 시도

🎯 하위경로 대상:
- ✅ 허용: /path, /api/users, /admin/dashboard, /user/profile
- ❌ 차단: /google.com, /https:google.com, http://example.com, https:google.com

🔧 하위경로 전처리:
- 공백 제거 및 슬래시 정규화
- 잘못된 URL 패턴 필터링
- 도메인 형태 경로 차단
- 허용 키워드 기반 예외 처리
"""
# -*- coding: utf-8 -*-
import argparse
import sqlite3
from collections import deque
from typing import Deque, Optional, Tuple
from urllib.parse import urlparse, urljoin

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

# ----------------------------
# URL/경로 유틸
# ----------------------------

def extract_path(u: str) -> str:
    """
    🔄 URL에서 경로 추출:
    - 입력: 완전한 URL (예: https://example.com/path/to/page)
    - 출력: 경로 부분만 (예: /path/to/page)
    - 예외 처리: 잘못된 URL은 빈 문자열 반환
    """
    if not u:
        return ''
    try:
        return urlparse(u).path or ''
    except Exception:
        return ''

def origin_of(u: str) -> Optional[str]:
    try:
        p = urlparse(u)
        if p.scheme and p.netloc:
            return f"{p.scheme}://{p.netloc}"
        return None
    except Exception:
        return None

def normalize_subpath(p: str) -> Optional[str]:
    """
    🔄 하위경로 정규화 및 전처리 과정:
    
    📋 입력 예시들:
    - '/path/to/page' → '/path/to/page' (정상)
    - 'path/to/page' → '/path/to/page' (슬래시 추가)
    - '/api/users' → '/api/users' (API 경로 허용)
    - '/google.com/path' → None (도메인 형태 차단)
    - '/https:google.com//' → None (잘못된 URL 차단)
    - 'https:google.com' → None (프로토콜 오류 차단)
    
    🚫 차단되는 패턴들:
    1) 절대 URL: http://, https://로 시작
    2) 잘못된 프로토콜: :// 포함, /https:, /http: 포함
    3) 프로토콜 오류: 콜론 포함하지만 http로 시작하지 않음
    4) 도메인 형태: 점(.) 포함하지만 허용 키워드 없음
    5) 빈 경로: '', '/', 공백만 있는 경우
    
    ✅ 허용되는 패턴들:
    - 일반 경로: /path, /page, /content
    - API 경로: /api, /admin, /user, /www, /app 포함
    - 쿼리/프래그먼트: /path?query=value, /path#fragment
    """
    if not p:
        return None
    
    # 🔄 STEP 1: 기본 정리 - 공백 제거
    path = p.strip()
    if not path or path == '/':
        return None

    # 🔄 STEP 2: 절대 URL 차단
    # 🚫 절대 URL이 path에 섞인 경우 무시
    if path.startswith("http://") or path.startswith("https://"):
        return None
    
    # 🔄 STEP 3: 잘못된 프로토콜 차단
    # 🚫 잘못된 프로토콜이나 도메인 형태가 포함된 경우 무시
    if '://' in path or path.startswith('/https:') or path.startswith('/http:'):
        return None
    
    # 🔄 STEP 4: 프로토콜 오류 차단
    # 🚫 프로토콜이 잘못된 경우 (예: https:google.com)
    if ':' in path and not path.startswith('/') and not path.startswith('http'):
        return None
    
    # 🔄 STEP 5: 도메인 형태 경로 차단
    # 🚫 도메인 형태의 경로 무시 (예: /google.com, /example.com/path)
    # 단, 일반적인 경로는 허용 (예: /api, /admin, /user 등)
    path_segments = path.split('/')
    if len(path_segments) > 1:
        first_segment = path_segments[1]  # 첫 번째 실제 세그먼트 (빈 문자열 제외)
        if first_segment and '.' in first_segment:
            # 도메인 형태인지 확인 (예: google.com, example.com)
            # 허용 키워드가 포함되어 있지 않으면 도메인으로 간주
            allowed_keywords = ['api', 'admin', 'user', 'www', 'app']
            if not any(keyword in first_segment.lower() for keyword in allowed_keywords):
                return None
    
    # 🔄 STEP 6: 슬래시 없는 경로의 도메인 형태 차단
    # 🚫 슬래시로 시작하지 않는 경로에서 도메인 형태 무시 (예: google.com/path)
    if not path.startswith('/'):
        path_segments = path.split('/')
        if path_segments and '.' in path_segments[0]:
            # 첫 번째 세그먼트가 도메인 형태인지 확인
            allowed_keywords = ['api', 'admin', 'user', 'www', 'app']
            if not any(keyword in path_segments[0].lower() for keyword in allowed_keywords):
                return None

    # 🔄 STEP 7: 슬래시 정규화
    if not path.startswith('/'):
        path = '/' + path
    return path


def build_candidate_url(base_origin: str, subpath: str) -> str:
    """
    🔄 하위경로와 기본 URL 결합:
    - base_origin: https://example.com
    - subpath: /path/to/page
    - 결과: https://example.com/path/to/page
    
    📋 처리 과정:
    1) base_origin 끝의 슬래시 제거
    2) 슬래시 추가
    3) subpath 앞의 슬래시 제거
    4) urljoin으로 안전하게 결합
    """
    return urljoin(base_origin.rstrip('/') + '/', subpath.lstrip('/'))

# ----------------------------
# HTTP Prober
# ----------------------------

def is_success_status(code: int) -> bool:
    return 200 <= code < 400

def http_probe(url: str, timeout: int = 4, ua: Optional[str] = None) -> Tuple[bool, int]:
    """
    🔄 HTTP probe 실행 순서:
    1) HEAD 요청으로 빠른 확인 (리소스 절약)
    2) HEAD 실패 시 GET 요청으로 재시도
    3) 성공/실패 여부와 상태코드 반환
    """
    headers = {'User-Agent': ua or 'Mozilla/5.0 (compatible; SubpageProbe/1.0)'}
    try:
        # 🔄 STEP 1: HEAD 요청으로 빠른 확인
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        if is_success_status(r.status_code):
            return True, r.status_code
        
        # 🔄 STEP 2: HEAD 실패 시 GET 요청으로 재시도
        r = requests.get(url, allow_redirects=True, timeout=timeout, headers=headers)
        return (is_success_status(r.status_code), r.status_code)
    except requests.RequestException:
        # 🔄 STEP 3: 예외 발생 시 실패로 처리
        return False, 0

# ----------------------------
# DB access
# ----------------------------

def fetch_all_rows(conn: sqlite3.Connection, table: str):
    conn.row_factory = sqlite3.Row
    # id 기준 오름차순 (rowid 대신 명시 PK 사용)
    sql = f"SELECT id, second_page_url FROM {table} ORDER BY id ASC"
    for row in conn.execute(sql):
        yield row


def is_marker_present(val: Optional[str]) -> bool:
    if not val:
        return False
    v = val.lower()
    return '(sub_o)' in v or '(sub_x)' in v or '(access)' in v

def is_target_row(row) -> bool:
    """
    🔄 하위경로 처리 대상 행 판정:
    
    📋 대상 행 조건:
    1) second_page_url이 존재함
    2) 아직 처리되지 않음 (마커 없음)
    3) 하위경로가 없음 (path가 '' 또는 '/')
    
    📋 대상 행 예시:
    - 'https://example.com' → True (경로 없음)
    - 'https://example.com/' → True (루트 경로)
    - 'https://example.com/path' → False (경로 있음)
    - 'https://example.com (sub_o)' → False (이미 처리됨)
    - None → False (URL 없음)
    
    📋 판정 과정:
    1) second_page_url 존재 여부 확인
    2) 이미 처리된 행(마커 있음)인지 확인
    3) 경로가 비어있거나 루트('/')인지 확인
    4) 모든 조건 만족 시 True 반환
    """
    spu = row['second_page_url']
    if not spu:
        return False
    
    # 🔄 STEP 1: 이미 마커가 있는 행은 대상에서 제외
    if is_marker_present(spu):
        return False
    
    # 🔄 STEP 2: 경로 추출 및 확인
    path = extract_path(spu)
    
    # 🔄 STEP 3: 경로가 비어있거나 루트인지 확인 (하위경로가 없는 상태)
    return (path == '' or path == '/')


def observe_paths_from_row(row) -> Optional[str]:
    """
    🔄 하위경로 관측 및 추출 과정:
    
    📋 관측 대상:
    - second_page_url이 있는 모든 행
    - 이미 처리되지 않은 행 (마커 없음)
    - 유효한 하위경로를 가진 행
    
    📋 관측 과정:
    1) second_page_url 존재 여부 확인
    2) 이미 처리된 행(마커 있음)인지 확인
    3) URL에서 경로 추출
    4) 경로 정규화 및 유효성 검사
    5) 유효한 하위경로 반환
    
    📋 관측 결과 예시:
    - 'https://example.com/path' → '/path'
    - 'https://example.com/api/users' → '/api/users'
    - 'https://example.com/' → None (루트 경로)
    - 'https://example.com/path (sub_o)' → None (이미 처리됨)
    """
    spu = row['second_page_url']
    if not spu:
        return None
    
    # 🔄 STEP 1: 이미 마커가 있는 행은 관측하지 않음 (중복 처리 방지)
    if is_marker_present(spu):
        return None
        
    # 🔄 STEP 2: URL에서 경로 추출
    path = extract_path(spu)
    
    # 🔄 STEP 3: 경로 정규화 및 유효성 검사
    return normalize_subpath(path)

def _probe_one(base_origin: str, subpath: str, timeout: int, ua: Optional[str], idx: int):
    """
    🔄 단일 하위경로 probe 실행 순서:
    1) base_origin + subpath로 완전한 URL 생성
    2) HTTP probe 실행 (HEAD → GET 순서)
    3) 결과 반환 (인덱스, 성공여부, 상태코드, URL, 하위경로)
    """
    # 🔄 STEP 1: 완전한 URL 생성
    cand = build_candidate_url(base_origin, subpath)
    
    # 🔄 STEP 2: HTTP probe 실행
    ok, code = http_probe(cand, timeout=timeout, ua=ua)
    
    # 🔄 STEP 3: 결과 반환 (우선순위 추적을 위해 인덱스 포함)
    return idx, ok, code, cand, subpath

def probe_candidates_concurrently(base_origin: str,
                                  subpaths: list[str],
                                  timeout: int,
                                  ua: Optional[str],
                                  max_workers: int = 8):
    """
    🔄 병렬 probe 실행 순서:
    1) 하위경로 후보들을 병렬로 probe
    2) '가장 우선순위가 높은(리스트에서 가장 앞)' 성공 후보를 반환
    3) 우선순위: 리스트 순서대로, 먼저 성공한 것이 선택됨
    
    반환: (chosen_url | None, chosen_subpath | None)
    """
    if not subpaths:
        return None, None

    # 🔄 STEP 1: 초기화 - 최적 결과 추적 변수들
    # 가장 "우선순위 높은(리스트 앞)" 성공을 고르기 위해
    # 성공 시 보고된 idx 중 '최소 idx'를 최종 선택
    best_idx = None
    best = (None, None)  # (chosen_url, chosen_subpath)
    lock = threading.Lock()  # 스레드 안전성을 위한 락

    # 🔄 STEP 2: 병렬 실행 시작
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        # 🔄 STEP 2-1: 각 하위경로에 대해 probe 작업 제출
        futures = [
            ex.submit(_probe_one, base_origin, sp, timeout, ua, i)
            for i, sp in enumerate(subpaths)
        ]
        
        # 🔄 STEP 2-2: 완료된 작업들 처리
        for fut in as_completed(futures):
            idx, ok, code, cand, sp = fut.result()
            if ok:  # 성공한 경우
                with lock:
                    # 우선순위가 더 높은(인덱스가 더 작은) 성공이면 업데이트
                    if best_idx is None or idx < best_idx:
                        best_idx = idx
                        best = (cand, sp)
            # 🔄 STEP 2-3: 최적화 - 최상위(인덱스 0)가 성공하면 조기 종료
            # 사실상 최적 → 남은 결과는 무시 가능
            if best_idx == 0:
                break

    # 🔄 STEP 3: 최적 결과 반환
    return best

# ----------------------------
# 파이프라인
# ----------------------------

def scan_and_fill(db_path: str,
                  table: str = 'urls',
                  window: int = 50,
                  timeout: int = 4,
                  ua: Optional[str] = None,
                  dry_run: bool = False,
                  verbose: bool = True,
                  limit: Optional[int] = None) -> None:
    """
    📋 실행 순서:
    1) 전체 rows(id ASC)를 순회.
    2) 각 행 이전에 관측된 subpath 큐(q)를 유지(maxlen=window).
    3) '대상 행'(second_page_url이 있고, path 없음, 마커 없음)이면:
       - base_origin = origin(second_page_url)
       - q를 역순으로 probe (최신 우선)
       - 성공: UPDATE second_page_url = "<cand> (sub_o)"
         실패: UPDATE second_page_url = "<원래 second_page_url> (sub_x)"
       → 각 행마다 즉시 DB 반영 (autocommit 모드).
    4) 현재 행에서도 관측 가능한 subpath가 있으면 큐에 push.
    """
    # 🔄 STEP 1: 초기화 - 관측 큐 생성 (최근 하위경로 저장용)
    q: Deque[str] = deque(maxlen=max(1, window))
    
    # 🔄 STEP 2: 데이터베이스 연결 및 설정
    with sqlite3.connect(db_path) as conn:
        conn.isolation_level = None   # autocommit 모드 (각 UPDATE 즉시 반영)
        cur = conn.cursor()

        # 🔄 STEP 3: 전체 데이터 로드 및 필터링
        # 전체 rows 가져오기 (id 오름차순)
        rows = list(fetch_all_rows(conn, table))
        if limit:
            rows = rows[-limit:]   # 최신 limit개만 처리

        # 대상 행들 식별 (second_page_url이 있고, path가 없고, 마커가 없는 행)
        targets = [r for r in rows if is_target_row(r)]
        total = len(targets)
        if verbose:
            print(f"[INFO] targets={total} window={window}")
        progress = tqdm(total=total, desc="processing", unit="row") if tqdm else None

        updated = 0
        
        # 🔄 STEP 4: 메인 처리 루프 - 각 행 순회
        for row in rows:
            rowid = row['id']
            second_url = row['second_page_url']

            try:
                # 🔄 STEP 4-1: 대상 행인지 확인
                if is_target_row(row):
                    # 🔄 STEP 4-2: 직접 접속 시도 (우선순위 1)
                    # 현재 second_page_url로 직접 접속 시도
                    ok_direct, code_direct = http_probe(second_url, timeout=timeout, ua=ua)
                    if ok_direct:
                        # ✅ 직접 접속 성공 → (access) 마커 추가
                        final_val = f"{second_url} (access)"
                        if dry_run:
                            if verbose and not progress:
                                print(f"[DRYRUN] UPDATE {table} SET second_page_url = ? WHERE id = ? -> {final_val}")
                        else:
                            cur.execute(f"UPDATE {table} SET second_page_url = ? WHERE id = ?", (final_val, rowid))
                            updated += 1
                        if progress:
                            progress.update(1)
                        # 다음 행으로 이동
                        continue

                    # 🔄 STEP 4-3: 직접 접속 실패 시 하위경로 후보 시도
                    # base_origin 추출 (예: https://example.com)
                    base_origin = origin_of(second_url)
                    chosen: Optional[str] = None
                    chosen_subpath: Optional[str] = None

                    if base_origin:
                        # 관측 큐를 역순으로 변환 (최신 하위경로 우선)
                        cand_list = list(reversed(q))
                        # 🔄 STEP 4-4: 병렬 probe 실행
                        chosen, chosen_subpath = probe_candidates_concurrently(
                            base_origin=base_origin,
                            subpaths=cand_list,
                            timeout=timeout,
                            ua=ua,
                            max_workers=8,
                        )
                        if verbose and not progress and chosen:
                            print(f"[PROBE-PAR] id={rowid} origin={base_origin} + subpath={chosen_subpath} -> {chosen} OK")

                    # 🔄 STEP 4-5: 결과에 따른 마커 결정
                    if chosen:
                        # ✅ 하위경로 붙여서 성공 → (sub_o) 마커
                        final_val = f"{chosen} (sub_o)"
                        if verbose:
                            print(f"[MATCH] id={rowid} origin={base_origin} + subpath={chosen_subpath} -> {chosen}")
                    else:
                        # ❌ 모든 시도 실패 → (sub_x) 마커
                        final_val = f"{second_url} (sub_x)"

                    # 🔄 STEP 4-6: 데이터베이스 업데이트
                    if dry_run:
                        if verbose and not progress:
                            print(f"[DRYRUN] UPDATE {table} SET second_page_url = ? WHERE id = ? -> {final_val}")
                    else:
                        cur.execute(f"UPDATE {table} SET second_page_url = ? WHERE id = ?", (final_val, rowid))
                        updated += 1

                    if progress:
                        progress.update(1)

                # 🔄 STEP 4-7: 관측 큐 갱신 (모든 행에 대해 실행)
                # 현재 행에서 관측 가능한 하위경로가 있으면 큐에 추가
                obs = observe_paths_from_row(row)
                if obs:
                    q.append(obs)  # 큐가 window 크기를 초과하면 자동으로 오래된 것 제거

            except Exception as e:
                if verbose:
                    print(f"[ERROR] id={rowid} {type(e).__name__}: {e}")

        # 🔄 STEP 5: 완료 처리
        if progress:
            progress.close()
        if verbose:
            print(f"=== SUMMARY ===\nprocessed(all)={len(rows)}\nupdated={updated}\nwindow={window}")


# ----------------------------
# 하위경로 처리 예시 및 테스트
# ----------------------------

def demonstrate_subpath_processing():
    """
    🔄 하위경로 처리 과정 시연:
    실제 데이터 예시를 통해 하위경로가 어떻게 추출, 정규화, 적용되는지 보여줍니다.
    """
    print("=" * 80)
    print("🔄 하위경로 처리 과정 시연")
    print("=" * 80)
    
    # 시뮬레이션 데이터
    sample_rows = [
        {'id': 1, 'second_page_url': 'https://example.com/api/users'},
        {'id': 2, 'second_page_url': 'https://example.com/admin/dashboard'},
        {'id': 3, 'second_page_url': 'https://example.com/user/profile'},
        {'id': 4, 'second_page_url': 'https://example.com'},  # 대상 행
        {'id': 5, 'second_page_url': 'https://example.com/'},  # 대상 행
        {'id': 6, 'second_page_url': 'https://example.com/path/to/page'},
        {'id': 7, 'second_page_url': 'https://example.com/google.com/path'},  # 차단될 것
        {'id': 8, 'second_page_url': 'https://example.com/https:google.com'},  # 차단될 것
    ]
    
    print("\n📋 1단계: 관측 큐 구축 과정")
    print("-" * 50)
    q = []
    
    for row in sample_rows:
        obs = observe_paths_from_row(row)
        is_target = is_target_row(row)
        
        print(f"ID {row['id']}: {row['second_page_url']}")
        print(f"  → 관측 결과: {obs}")
        print(f"  → 대상 행 여부: {'✅ YES' if is_target else '❌ NO'}")
        
        if obs:
            q.append(obs)
            print(f"  → 큐에 추가됨 (현재 큐 크기: {len(q)})")
        print()
    
    print(f"\n📋 최종 관측 큐: {q}")
    
    print("\n📋 2단계: 대상 행에 하위경로 적용")
    print("-" * 50)
    
    for row in sample_rows:
        if is_target_row(row):
            base_origin = origin_of(row['second_page_url'])
            print(f"대상 행 ID {row['id']}: {row['second_page_url']}")
            print(f"  → Base Origin: {base_origin}")
            
            if base_origin and q:
                print("  → 적용 가능한 하위경로들:")
                for i, subpath in enumerate(reversed(q)):  # 최신 우선
                    candidate = build_candidate_url(base_origin, subpath)
                    print(f"    {i+1}. {base_origin} + {subpath} = {candidate}")
            print()

# ----------------------------
# CLI
# ----------------------------

def parse_args():
    ap = argparse.ArgumentParser(description='Attach recent observed subpaths to path-less second_page_url.')
    ap.add_argument('--db', dest='db_path', required=True, help='SQLite DB path (e.g., db/translate_goog_urls.db)')
    ap.add_argument('--table', default='urls', help='Table name (default: urls)')
    ap.add_argument('--window', type=int, default=50, help='Number of recent subpaths to keep (default: 50)')
    ap.add_argument('--timeout', type=int, default=4, help='HTTP timeout seconds (default: 4)')
    ap.add_argument('--user-agent', dest='ua', default=None, help='Custom User-Agent for probing')
    ap.add_argument('--dry-run', type=lambda x: str(x).lower()!='false', default=False, help='Do not write updates (default: False)')
    ap.add_argument('--verbose', type=lambda x: str(x).lower()!='false', default=True, help='Print progress (default: True)')
    return ap.parse_args()

if __name__ == '__main__':
    """
    🔄 메인 실행 순서:
    1) 설정값 초기화
    2) 시연 모드 또는 실제 실행 선택
    3) 전체 파이프라인 실행
    """
    import sys
    
    # 🔄 STEP 1: 시연 모드 확인
    if len(sys.argv) > 1 and sys.argv[1] == '--demo':
        print("🎯 하위경로 처리 시연 모드 실행")
        demonstrate_subpath_processing()
        sys.exit(0)
    
    # 🔄 STEP 2: 하드코딩된 기본값 설정
    DB_PATH = "db/translate_goog_urls.db"   # 사용할 SQLite DB 파일
    TABLE = "urls"                          # 테이블 이름
    WINDOW = 50                             # 최근 하위경로 개수 (관측 큐 크기)
    TIMEOUT = 3                             # HTTP 타임아웃(초)
    USER_AGENT = None                       # 필요하다면 문자열로 지정
    DRY_RUN = False                         # True로 하면 UPDATE 실행 안 함 (테스트용)
    VERBOSE = True                          # 진행 상황 출력 여부
    LIMIT = None   # ← 최신 10개만 테스트. 전체 돌리려면 None

    # 🔄 STEP 3: 메인 파이프라인 실행
    scan_and_fill(
        db_path=DB_PATH,
        table=TABLE,
        window=WINDOW,
        timeout=TIMEOUT,
        ua=USER_AGENT,
        dry_run=DRY_RUN,
        verbose=VERBOSE,
        limit=LIMIT,
        
    )
