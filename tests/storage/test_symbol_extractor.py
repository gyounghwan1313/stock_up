from storage.symbol_extractor import extract_symbols


def test_extract_company_name():
    text = "Apple reports record quarterly earnings"
    result = extract_symbols(text)
    assert "AAPL" in result


def test_extract_multiple_companies():
    text = "Microsoft and Google announce AI partnership"
    result = extract_symbols(text)
    assert "MSFT" in result
    assert "GOOGL" in result


def test_extract_from_watchlist():
    text = "TSLA stock drops after NHTSA investigation"
    result = extract_symbols(text, watchlist=["TSLA", "AAPL"])
    assert "TSLA" in result
    assert "AAPL" not in result


def test_no_false_positives():
    text = "The Fed raised interest rates by 25 basis points"
    result = extract_symbols(text)
    assert len(result) == 0


def test_case_insensitive_company_names():
    text = "TESLA announces new factory in Berlin"
    result = extract_symbols(text)
    assert "TSLA" in result


def test_empty_text():
    assert extract_symbols("") == []
    assert extract_symbols("no stock symbols here", watchlist=["AAPL"]) == []
