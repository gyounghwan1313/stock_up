import re

COMPANY_ALIASES: dict[str, str] = {
    "apple": "AAPL",
    "microsoft": "MSFT",
    "google": "GOOGL",
    "alphabet": "GOOGL",
    "amazon": "AMZN",
    "tesla": "TSLA",
    "nvidia": "NVDA",
    "meta": "META",
    "netflix": "NFLX",
    "amd": "AMD",
    "intel": "INTC",
    "boeing": "BA",
    "jpmorgan": "JPM",
    "goldman sachs": "GS",
    "goldman": "GS",
    "berkshire": "BRK-B",
}

# 대문자 1~5자 (주식 심볼 패턴)
TICKER_PATTERN = re.compile(r'\b([A-Z]{1,5})\b')

# 너무 흔한 단어 제외
COMMON_WORDS = {
    "THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL",
    "CAN", "HER", "WAS", "ONE", "OUR", "OUT", "HAS", "HIS",
    "HOW", "MAN", "NEW", "NOW", "OLD", "SEE", "WAY", "WHO",
    "BOY", "DID", "GET", "HIM", "LET", "SAY", "SHE", "TOO",
    "USE", "DAD", "MOM", "ITS", "GDP", "CPI", "FED", "USD",
    "EUR", "GBP", "JPY", "OIL", "IMF", "PMI", "API", "ETF",
    "IPO", "CEO", "CFO", "NYSE", "SEC", "DOJ", "FBI", "CIA",
    "NATO", "OPEC", "WHO", "WTO", "ECB", "BOE", "BOJ",
    "RSS", "URL", "USA", "UK",
}


def extract_symbols(text: str, watchlist: list[str] | None = None) -> list[str]:
    symbols: set[str] = set()

    # 1. 회사명 매칭
    text_lower = text.lower()
    for alias, symbol in COMPANY_ALIASES.items():
        if alias in text_lower:
            symbols.add(symbol)

    # 2. 워치리스트 심볼 직접 매칭
    if watchlist:
        for sym in watchlist:
            if sym.upper() in text.upper():
                symbols.add(sym.upper())

    # 3. 대문자 패턴 매칭 (보수적으로)
    for match in TICKER_PATTERN.finditer(text):
        candidate = match.group(1)
        if candidate not in COMMON_WORDS and len(candidate) >= 2:
            if watchlist and candidate in [w.upper() for w in watchlist]:
                symbols.add(candidate)

    return sorted(symbols)
