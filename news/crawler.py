import ipaddress
import re
import html
import socket
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests
from defusedxml import ElementTree as ET
from xml.etree.ElementTree import Element


# ── SSRF 방지 ──

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
        if "내부 IP" in str(exc):
            raise
        try:
            resolved_ip = ipaddress.ip_address(socket.gethostbyname(hostname))
            if resolved_ip.is_private or resolved_ip.is_loopback or resolved_ip.is_link_local:
                raise ValueError(f"내부 IP로 해석되는 호스트는 허용되지 않습니다: {hostname}")
        except socket.gaierror:
            pass


# ── RSS Fetcher ──

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
                time.sleep(1)

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


# ── RSS Parser ──

_ATOM_NS = "http://www.w3.org/2005/Atom"


class RSSItem:
    """RSS 아이템을 나타내는 클래스"""

    def __init__(self, title: str = "", link: str = "", pub_date: Optional[datetime] = None):
        self.title = title.strip()
        self.link = link.strip()
        self.pub_date = pub_date

    def to_dict(self) -> Dict[str, str]:
        """딕셔너리로 변환"""
        return {
            'title': self.title,
            'link': self.link,
            'pub_date': self.pub_date.isoformat() if self.pub_date else None
        }

    def __str__(self):
        return f"RSSItem(title='{self.title[:50]}...', link='{self.link}', pub_date={self.pub_date})"


class RSSParser:
    """RSS XML을 파싱하는 클래스 (RSS 2.0 + Atom 지원)"""

    def parse_items(self, xml_content: str) -> List[RSSItem]:
        """RSS XML에서 item들을 파싱합니다 (RSS 2.0 및 Atom 형식 모두 지원)"""
        try:
            xml_content = xml_content.lstrip('\ufeff')
            if xml_content.startswith('ï»¿'):
                xml_content = xml_content[3:]

            xml_content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#)', '&amp;', xml_content)

            root = ET.fromstring(xml_content)
            items = []

            for item in root.findall('.//item'):
                title = self._extract_text(item, 'title')
                link = self._extract_text(item, 'link')
                pub_date = self._parse_pub_date(item)

                rss_item = RSSItem(title=title, link=link, pub_date=pub_date)
                items.append(rss_item)

            if not items:
                items = self._parse_atom_entries(root)

            return items

        except ET.ParseError as e:
            raise ValueError(f"XML 파싱 오류: {e}")
        except Exception as e:
            raise ValueError(f"RSS 파싱 중 오류 발생: {e}")

    def _parse_atom_entries(self, root: Element) -> List[RSSItem]:
        items = []
        entries = root.findall(f'.//{{{_ATOM_NS}}}entry')
        if not entries:
            entries = root.findall('.//entry')

        for entry in entries:
            title = self._extract_text_ns(entry, 'title')
            link = self._extract_atom_link(entry)
            pub_date = self._parse_atom_date(entry)
            items.append(RSSItem(title=title, link=link, pub_date=pub_date))

        return items

    def _extract_text_ns(self, item: Element, tag_name: str) -> str:
        element = item.find(f'{{{_ATOM_NS}}}{tag_name}')
        if element is None:
            element = item.find(tag_name)
        if element is not None and element.text:
            text = element.text.strip()
            text = re.sub(r'\s+', ' ', text)
            return text
        return ""

    def _extract_atom_link(self, entry: Element) -> str:
        link = entry.find(f'{{{_ATOM_NS}}}link')
        if link is None:
            link = entry.find('link')

        if link is not None:
            href = link.get('href')
            if href:
                return href.strip()
            if link.text:
                return link.text.strip()
        return ""

    def _parse_atom_date(self, entry: Element) -> Optional[datetime]:
        for tag in ('updated', 'published'):
            date_text = self._extract_text_ns(entry, tag)
            if date_text:
                parsed = self._parse_iso8601(date_text)
                if parsed:
                    return parsed
        return None

    def _extract_text(self, item: Element, tag_name: str) -> str:
        element = item.find(tag_name)
        if element is not None and element.text:
            text = element.text.strip()
            text = re.sub(r'\s+', ' ', text)
            return text
        return ""

    def _parse_pub_date(self, item: Element) -> Optional[datetime]:
        pub_date_text = self._extract_text(item, 'pubDate')
        if not pub_date_text:
            pub_date_text = self._extract_text(item, 'date')
        if not pub_date_text:
            return None

        try:
            return self._parse_rfc2822_date(pub_date_text)
        except (ValueError, Exception):
            pass

        return self._parse_iso8601(pub_date_text)

    def _parse_rfc2822_date(self, date_str: str) -> datetime:
        date_str = date_str.strip()
        if ',' in date_str:
            date_str = date_str.split(',', 1)[1].strip()
        date_str = re.sub(r'\s+(GMT|UTC)$', '', date_str)

        try:
            return datetime.strptime(date_str, '%d %b %Y %H:%M:%S')
        except ValueError as e:
            raise ValueError(f"날짜 형식을 파싱할 수 없습니다: {date_str} - {e}")

    def _parse_iso8601(self, date_str: str) -> Optional[datetime]:
        date_str = date_str.strip()
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'

        try:
            dt = datetime.fromisoformat(date_str)
            return dt.replace(tzinfo=None)
        except (ValueError, TypeError):
            return None

    def get_latest_items(self, xml_content: str, limit: int = 10) -> List[RSSItem]:
        items = self.parse_items(xml_content)
        sorted_items = sorted(
            items,
            key=lambda x: x.pub_date or datetime.min,
            reverse=True
        )
        return sorted_items[:limit]

    def filter_by_keywords(self, items: List[RSSItem], keywords: List[str]) -> List[RSSItem]:
        filtered_items = []
        keywords_lower = [keyword.lower() for keyword in keywords]

        for item in items:
            title_lower = item.title.lower()
            if any(keyword in title_lower for keyword in keywords_lower):
                filtered_items.append(item)

        return filtered_items
