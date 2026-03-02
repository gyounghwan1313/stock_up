import ipaddress
import socket
from typing import Optional, Dict, Any
from urllib.parse import urlparse
import requests
import time


def _validate_url(url: str) -> None:
    """SSRF 방지를 위해 URL을 검증합니다"""
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"허용되지 않은 URL 스킴입니다: {parsed.scheme}")

    hostname = parsed.hostname
    if not hostname:
        raise ValueError("URL에 호스트명이 없습니다")

    _BLOCKED_HOSTNAMES = {"localhost", "metadata.google.internal"}
    if hostname.lower() in _BLOCKED_HOSTNAMES:
        raise ValueError(f"허용되지 않은 호스트입니다: {hostname}")

    try:
        ip = ipaddress.ip_address(hostname)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise ValueError(f"내부 IP 주소는 허용되지 않습니다: {hostname}")
    except ValueError as exc:
        # hostname이 IP가 아닌 경우 — DNS 조회로 최종 확인
        if "내부 IP" in str(exc):
            raise
        try:
            resolved_ip = ipaddress.ip_address(socket.gethostbyname(hostname))
            if resolved_ip.is_private or resolved_ip.is_loopback or resolved_ip.is_link_local:
                raise ValueError(f"내부 IP로 해석되는 호스트는 허용되지 않습니다: {hostname}")
        except socket.gaierror:
            pass  # DNS 조회 실패는 요청 시점에서 처리


class RSSFetcher:
    """RSS 피드를 가져오는 클래스"""

    def __init__(self, timeout: int = 30, user_agent: str = None):
        self.timeout = timeout
        self.user_agent = user_agent or "RSS Fetcher 1.0"
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})

    def fetch(self, url: str, retries: int = 3) -> str:
        """RSS 피드를 가져옵니다"""
        _validate_url(url)
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
        _validate_url(url)
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