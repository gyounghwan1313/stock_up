import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Dict, Optional
import re
import html


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
    """RSS XML을 파싱하는 클래스"""
    
    def parse_items(self, xml_content: str) -> List[RSSItem]:
        """RSS XML에서 item들을 파싱합니다"""
        try:
            # HTML 엔티티를 먼저 디코드
            xml_content = html.unescape(xml_content)
            
            # XML 파싱에 안전하지 않은 문자들을 이스케이프
            xml_content = xml_content.replace('&', '&amp;')
            
            root = ET.fromstring(xml_content)
            items = []
            
            # RSS 2.0 형식에서 item 찾기
            for item in root.findall('.//item'):
                title = self._extract_text(item, 'title')
                link = self._extract_text(item, 'link')
                pub_date = self._parse_pub_date(item)
                
                rss_item = RSSItem(title=title, link=link, pub_date=pub_date)
                items.append(rss_item)
            
            return items
            
        except ET.ParseError as e:
            raise ValueError(f"XML 파싱 오류: {e}")
        except Exception as e:
            raise ValueError(f"RSS 파싱 중 오류 발생: {e}")
    
    def _extract_text(self, item: ET.Element, tag_name: str) -> str:
        """XML 요소에서 텍스트를 추출합니다"""
        element = item.find(tag_name)
        if element is not None and element.text:
            # 여러 줄에 걸쳐진 텍스트와 공백 정리
            text = element.text.strip()
            # 연속된 공백을 하나로 합치기
            text = re.sub(r'\s+', ' ', text)
            return text
        return ""
    
    def _parse_pub_date(self, item: ET.Element) -> Optional[datetime]:
        """pubDate를 파싱합니다"""
        pub_date_text = self._extract_text(item, 'pubDate')
        if not pub_date_text:
            return None
        
        try:
            # "Thu, 11 Sep 2025 11:27:51 GMT" 형식 파싱
            return self._parse_rfc2822_date(pub_date_text)
        except:
            return None
    
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