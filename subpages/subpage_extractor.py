# utils/subpage_extractor.py
import re
from urllib.parse import urlparse, urljoin

def extract_subpages(base_url, html_text):
    """
    HTML 안에서 하위 페이지(subpages) 후보들을 추출하는 함수
    - base_url: 기준이 되는 최종 URL
    - html_text: 해당 페이지의 HTML 원문
    - return: 하위 페이지 URL 리스트
    """
    # href="..." 링크 추출
    hrefs = re.findall(r'href=[\'"]?([^\'" >]+)', html_text)

    subpages = []
    for href in hrefs:
        if href.startswith("http"):  
            subpages.append(href)
        else:
            # 상대경로는 절대경로로 변환
            subpages.append(urljoin(base_url, href))

    # 중복 제거
    return list(set(subpages))
