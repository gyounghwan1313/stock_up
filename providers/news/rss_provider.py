import logging

from crawler.rss_fetcher import RSSFetcher
from crawler.rss_parser import RSSParser

logger = logging.getLogger(__name__)


class RSSNewsProvider:
    def __init__(self, url: str):
        self.url = url
        self.fetcher = RSSFetcher()
        self.parser = RSSParser()

    def fetch_news(self) -> list[dict]:
        try:
            xml_content = self.fetcher.fetch(self.url)
            items = self.parser.get_latest_items(xml_content, limit=20)
            return [item.to_dict() for item in items]
        except Exception as e:
            logger.error("RSS fetch failed: %s", e)
            return []
