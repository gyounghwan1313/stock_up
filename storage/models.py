from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class NewsRecord:
    title_original: str
    title_translated: str
    source: str
    link: str
    published_at: Optional[datetime]
    collected_at: datetime
    sentiment_score: Optional[float] = None
    related_symbols: Optional[list[str]] = None
    id: Optional[int] = None
