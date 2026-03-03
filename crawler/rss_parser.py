from defusedxml import ElementTree as ET
from xml.etree.ElementTree import Element
from datetime import datetime
from typing import List, Dict, Optional
import re
import html


# Atom 네임스페이스
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
            # BOM 제거 (Federal Reserve 등 일부 피드에 UTF-8 BOM 포함)
            # \ufeff = 정상 유니코드 BOM, ï»¿ = UTF-8 BOM이 Latin-1로 잘못 디코딩된 경우
            xml_content = xml_content.lstrip('\ufeff')
            if xml_content.startswith('ï»¿'):
                xml_content = xml_content[3:]

            # 이미 올바른 XML 엔티티(&amp; 등)를 보존하면서 bare '&'만 이스케이프
            xml_content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#)', '&amp;', xml_content)

            root = ET.fromstring(xml_content)
            items = []

            # RSS 2.0 형식에서 item 찾기
            for item in root.findall('.//item'):
                title = self._extract_text(item, 'title')
                link = self._extract_text(item, 'link')
                pub_date = self._parse_pub_date(item)

                rss_item = RSSItem(title=title, link=link, pub_date=pub_date)
                items.append(rss_item)

            # Atom 형식에서 entry 찾기 (RSS 2.0에서 아이템이 없을 경우)
            if not items:
                items = self._parse_atom_entries(root)

            return items

        except ET.ParseError as e:
            raise ValueError(f"XML 파싱 오류: {e}")
        except Exception as e:
            raise ValueError(f"RSS 파싱 중 오류 발생: {e}")

    def _parse_atom_entries(self, root: Element) -> List[RSSItem]:
        """Atom 형식의 entry 요소들을 파싱합니다"""
        items = []

        # 네임스페이스 있는 경우와 없는 경우 모두 탐색
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
        """Atom 네임스페이스를 고려하여 텍스트를 추출합니다"""
        # 네임스페이스 있는 경우
        element = item.find(f'{{{_ATOM_NS}}}{tag_name}')
        if element is None:
            # 네임스페이스 없는 경우
            element = item.find(tag_name)
        if element is not None and element.text:
            text = element.text.strip()
            text = re.sub(r'\s+', ' ', text)
            return text
        return ""

    def _extract_atom_link(self, entry: Element) -> str:
        """Atom entry에서 link를 추출합니다 (<link href="..."/> 형식 지원)"""
        # 네임스페이스 있는 경우
        link = entry.find(f'{{{_ATOM_NS}}}link')
        if link is None:
            link = entry.find('link')

        if link is not None:
            # href 속성 방식 (Atom 표준)
            href = link.get('href')
            if href:
                return href.strip()
            # 텍스트 방식
            if link.text:
                return link.text.strip()
        return ""

    def _parse_atom_date(self, entry: Element) -> Optional[datetime]:
        """Atom entry에서 날짜를 파싱합니다 (<updated> 또는 <published>)"""
        for tag in ('updated', 'published'):
            date_text = self._extract_text_ns(entry, tag)
            if date_text:
                parsed = self._parse_iso8601(date_text)
                if parsed:
                    return parsed
        return None

    def _extract_text(self, item: Element, tag_name: str) -> str:
        """XML 요소에서 텍스트를 추출합니다"""
        element = item.find(tag_name)
        if element is not None and element.text:
            # 여러 줄에 걸쳐진 텍스트와 공백 정리
            text = element.text.strip()
            # 연속된 공백을 하나로 합치기
            text = re.sub(r'\s+', ' ', text)
            return text
        return ""

    def _parse_pub_date(self, item: Element) -> Optional[datetime]:
        """pubDate를 파싱합니다 (RFC 2822 및 ISO 8601 지원)"""
        pub_date_text = self._extract_text(item, 'pubDate')
        if not pub_date_text:
            # dc:date 등 다른 날짜 태그도 시도
            pub_date_text = self._extract_text(item, 'date')
        if not pub_date_text:
            return None

        # RFC 2822 먼저 시도
        try:
            return self._parse_rfc2822_date(pub_date_text)
        except (ValueError, Exception):
            pass

        # ISO 8601 시도
        return self._parse_iso8601(pub_date_text)

    def _parse_rfc2822_date(self, date_str: str) -> datetime:
        """RFC 2822 형식의 날짜를 파싱합니다"""
        # "Thu, 11 Sep 2025 11:27:51 GMT" 형식
        date_str = date_str.strip()

        # 요일 제거
        if ',' in date_str:
            date_str = date_str.split(',', 1)[1].strip()

        # GMT/UTC 제거
        date_str = re.sub(r'\s+(GMT|UTC)$', '', date_str)

        try:
            return datetime.strptime(date_str, '%d %b %Y %H:%M:%S')
        except ValueError as e:
            raise ValueError(f"날짜 형식을 파싱할 수 없습니다: {date_str} - {e}")

    def _parse_iso8601(self, date_str: str) -> Optional[datetime]:
        """ISO 8601 형식의 날짜를 파싱합니다 (예: 2025-09-11T11:27:51Z)"""
        date_str = date_str.strip()
        # 끝의 Z를 +00:00으로 변환
        if date_str.endswith('Z'):
            date_str = date_str[:-1] + '+00:00'

        try:
            dt = datetime.fromisoformat(date_str)
            # timezone-aware → naive로 변환 (기존 코드와 호환)
            return dt.replace(tzinfo=None)
        except (ValueError, TypeError):
            return None

    def get_latest_items(self, xml_content: str, limit: int = 10) -> List[RSSItem]:
        """최신 아이템들을 가져옵니다"""
        items = self.parse_items(xml_content)

        # pubDate 기준으로 정렬 (최신순)
        sorted_items = sorted(
            items,
            key=lambda x: x.pub_date or datetime.min,
            reverse=True
        )

        return sorted_items[:limit]

    def filter_by_keywords(self, items: List[RSSItem], keywords: List[str]) -> List[RSSItem]:
        """키워드로 아이템을 필터링합니다"""
        filtered_items = []
        keywords_lower = [keyword.lower() for keyword in keywords]

        for item in items:
            title_lower = item.title.lower()
            if any(keyword in title_lower for keyword in keywords_lower):
                filtered_items.append(item)

        return filtered_items
