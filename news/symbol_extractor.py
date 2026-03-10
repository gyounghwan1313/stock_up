"""뉴스 헤드라인에서 관련 주식 심볼을 추출하는 유틸리티."""

import re

# 자주 등장하는 기업명 → 티커 매핑
_COMPANY_TICKER_MAP = {
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "alphabet": "GOOGL",
    "amazon": "AMZN", "tesla": "TSLA", "nvidia": "NVDA", "meta": "META",
    "facebook": "META", "jpmorgan": "JPM", "goldman sachs": "GS", "goldman": "GS",
    "wells fargo": "WFC", "bank of america": "BAC", "morgan stanley": "MS",
    "disney": "DIS", "intel": "INTC", "amd": "AMD", "netflix": "NFLX",
    "coreweave": "CRWV", "rimini street": "RMNI",
}

# 괄호 안의 티커 패턴 (e.g., "(AAPL)", "(DK)")
_TICKER_PATTERN = re.compile(r"\(([A-Z]{1,5})\)")


def extract_symbols(title: str, watchlist: list[str] | None = None) -> list[str]:
    """헤드라인에서 관련 종목 심볼을 추출.

    1. 괄호 안의 티커 심볼 추출 (e.g., "Delek US Holdings (DK)")
    2. 워치리스트에 있는 심볼이 제목에 언급되면 추출
    3. 알려진 기업명 매핑
    """
    symbols: set[str] = set()
    title_upper = title.upper()

    # 1. 괄호 안 티커
    for match in _TICKER_PATTERN.finditer(title):
        symbols.add(match.group(1))

    # 2. 워치리스트 심볼 매칭
    if watchlist:
        for sym in watchlist:
            if re.search(rf"\b{re.escape(sym)}\b", title_upper):
                symbols.add(sym)

    # 3. 기업명 매핑
    title_lower = title.lower()
    for company, ticker in _COMPANY_TICKER_MAP.items():
        if company in title_lower:
            symbols.add(ticker)

    return sorted(symbols)
