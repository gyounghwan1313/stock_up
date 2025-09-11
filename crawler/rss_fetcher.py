import requests
from typing import Optional, Dict, Any
import time


class RSSFetcher:
    """RSS 피드를 가져오는 클래스"""
    
    def __init__(self, timeout: int = 30, user_agent: str = None):
        self.timeout = timeout
        self.user_agent = user_agent or "RSS Fetcher 1.0"
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})
    
    def fetch(self, url: str, retries: int = 3) -> str:
        """RSS 피드를 가져옵니다"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise ValueError(f"RSS 피드를 가져오는데 실패했습니다: {e}")
                time.sleep(1)  # 재시도 전 대기
    
    def fetch_with_info(self, url: str, retries: int = 3) -> Dict[str, Any]:
        """RSS 피드와 추가 정보를 함께 가져옵니다"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                return {
                    'content': response.text,
                    'status_code': response.status_code,
                    'headers': dict(response.headers),
                    'encoding': response.encoding,
                    'url': response.url
                }
            except requests.RequestException as e:
                if attempt == retries - 1:
                    raise ValueError(f"RSS 피드를 가져오는데 실패했습니다: {e}")
                time.sleep(1)
    
    def close(self):
        """세션을 닫습니다"""
        self.session.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()