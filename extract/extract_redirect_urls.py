# -*- coding: utf-8 -*-
from __future__ import annotations
import re
import subprocess
from typing import List, Tuple, Optional

# 웹 요청 시 사용할 User-Agent. 봇으로 인식되는 것을 피하기 위함
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/124.0.0.0 Safari/537.36")

# -----------------------------------------------------------------------------
# Helper Functions (보조 함수)
# -----------------------------------------------------------------------------

def _dedupe_tuples(tuples: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    """(url, snippet) 튜플 리스트에서 URL 기준 중복을 제거합니다."""
    seen_urls, out = set(), []
    for url, snippet in tuples:
        url = (url or "").strip()
        if url and url not in seen_urls:
            seen_urls.add(url)
            # 스니펫의 줄바꿈과 과도한 공백을 정리하여 한 줄로 만듬
            clean_snippet = re.sub(r'\s+', ' ', snippet.strip())
            out.append((url, clean_snippet))
    return out

def normalize_url(raw: str) -> str:
    """입력된 URL 문자열을 정리합니다."""
    if not raw: return ""
    s = raw.strip()
    suffixes = [" (sub)", " (sub_o)", " (sub_x)", " (access)"]
    for suf in suffixes:
        if s.endswith(suf): s = s[: -len(suf)].strip()
    if (s.startswith('"') and s.endswith('"')) or \
       (s.startswith("'") and s.endswith("'")): s = s[1:-1]
    return s

def _short_err(label: str, rc: int, stderr: str) -> str:
    """curl 실행 시 발생한 오류 메시지를 간결하게 요약합니다."""
    s = (stderr or "").lower()
    if "timed out" in s or rc == 28: reason = "timeout"
    elif "could not resolve host" in s or rc == 6: reason = "resolve"
    elif "failed to connect" in s or rc == 7: reason = "connect"
    else:
        first = (stderr or f"rc={rc}").strip().splitlines()[0][:120]
        reason = first if first else f"rc={rc}"
    return f"ERR:{label}:{reason}"

def _run_curl(args: list, timeout: int = 20) -> Tuple[int, str, str]:
    """subprocess를 이용해 curl 명령어를 실행하고 결과를 반환합니다."""
    try:
        p = subprocess.run(args, capture_output=True, text=True, check=False, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired:
        return 28, "", "timed out"
    except Exception as e:
        return 1, "", f"{type(e).__name__}:{e}"

# -----------------------------------------------------------------------------
# Core Functions (핵심 기능 함수)
# -----------------------------------------------------------------------------

def get_html_body(url: str, timeout: int = 20) -> Tuple[Optional[str], Optional[str]]:
    """주어진 URL의 HTML 본문 전체를 가져옵니다."""
    args = ["curl", "-sSL", "--max-time", str(timeout), "--connect-timeout", "10", "-A", UA, url]
    rc, out, err = _run_curl(args, timeout)
    if rc == 0:
        return out, None
    return None, _short_err("get_body", rc, err)

def extract_redirect_snippets(html: str) -> List[Tuple[str, str]]:
    """
    HTML 소스 코드에서 리디렉션 URL과 해당 URL을 포함한 코드 스니펫(증거)을 함께 추출합니다.
    - 반환값: [(추출된 URL, 증거 코드 스니펫), ...] 형태의 리스트
    """
    if not html: return []
    
    results: List[Tuple[str, str]] = []
    
    # 1. JavaScript: window.location = "..." 패턴 (전체 라인을 스니펫으로 저장)
    for m in re.finditer(r"""((?:window|self)?\.?location(?:\.href)?\s*=\s*['"][^'"]+['"])""", html, flags=re.I):
        snippet = m.group(1)
        url_match = re.search(r"""['"]([^'"]+)['"]""", snippet)
        if url_match:
            results.append((url_match.group(1), snippet))

    # 2. JavaScript: setTimeout 내의 location 변경 패턴 (setTimeout 전체를 스니펫으로 저장)
    for m in re.finditer(r"""(setTimeout\([^)]*?location(?:\.href)?\s*=\s*['"][^'"]+['"][^)]*\))""", html, flags=re.I|re.S):
        snippet = m.group(1)
        url_match = re.search(r"""location(?:\.href)?\s*=\s*['"]([^'"]+)['"]""", snippet, flags=re.I|re.S)
        if url_match:
            results.append((url_match.group(1), snippet))
            
    # 3. HTML Meta Tag: <meta http-equiv="refresh" ...> 패턴 (태그 전체를 스니펫으로 저장)
    for m in re.finditer(r"""(<meta[^>]+http-equiv=["']refresh["'][^>]+>)""", html, flags=re.I):
        snippet = m.group(1)
        url_match = re.search(r"""content=["'][^"']*url=([^"'>]+)""", snippet, flags=re.I)
        if url_match:
            results.append((url_match.group(1).strip(), snippet))

    # 4. 특정 스크립트에서 사용하는 URL 배열 패턴 (var urls = [...] 전체를 스니펫으로 저장)
    arr_m = re.search(r"""(var\s+urls\s*=\s*\[[^\]]+\])""", html, flags=re.I|re.S)
    if arr_m:
        snippet = arr_m.group(1)
        urls_in_arr = re.findall(r"""['"]([^'"]+)['"]""", snippet)
        for url in urls_in_arr:
            results.append((url, snippet))

    return _dedupe_tuples(results)

# -----------------------------------------------------------------------------
# Main Logic (메인 분석 함수) - 새로운 요구사항에 맞춰 재구성
# -----------------------------------------------------------------------------

def analyze_script_redirects(url_raw: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    주어진 URL의 소스 코드를 분석하여 스크립트 리디렉션 URL과 그 증거 스니펫을 찾습니다.

    :param url_raw: 분석할 원본 URL 문자열
    :return: (리디렉션 URL 목록 문자열, 증거 스니펫 목록 문자열, 에러 메시지)
    """
    url = normalize_url(url_raw)
    if not url:
        return None, None, "ERR:input:empty"

    # 1단계: HTML 본문 가져오기
    body, err = get_html_body(url)
    if not body:
        return None, None, err or "ERR:get_body:empty"

    # 2단계: HTML에서 URL과 스니펫 추출
    extracted_data = extract_redirect_snippets(body)
    if not extracted_data:
        return None, None, "ERR:extract:no_redirect_script_found"

    # 3단계: 결과 포맷팅
    # final_urls_str 변수는 final_redirect_url 컬럼을 위한 것이었으므로 그대로 둠
    final_urls_str = "|".join([item[0] for item in extracted_data])
    # redirect_snippet 컬럼: 모든 증거 스니펫을 '|'로 연결
    snippets_str = "|".join([item[1] for item in extracted_data])
    
    return final_urls_str, snippets_str, None


if __name__ == "__main__":
    tests = [
        "https://www.paycom.com (access)",
        "https://commons.wikimedia.org/wiki/Main_Page",
        "https://ejcuddlebunnymx.lnyxzodm.ru.com/EjCuddleBunnyMX",
        "https://5lyskr5mu2hw.pages.dev/support.apple.com",
        "https://www.onenotegem.com"
    ]

    print("--- 스크립트 리디렉션 분석 시작 ---\n")
    for t in tests:
        # 새로운 함수 및 반환값에 맞춰 변수명 변경
        final_urls, snippets, err = analyze_script_redirects(t)

        print(f"▶ 입력 URL: '{t}'")
        if err:
            print(f"  결과: 분석 실패")
            print(f"  오류: {err}")
        else:
            # [수정] final_redirect_url -> script_redirect_url로 명칭 변경
            print(f"  - script_redirect_url:")
            print(f"    {final_urls}")
            
            # redirect_snippet 컬럼에 저장될 내용
            print(f"  - redirect_snippet (증거 스니펫):")
            print(f"    {snippets}")
        print("-" * 20)