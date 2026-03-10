import logging

logger = logging.getLogger(__name__)


def init_news_store(config: dict, read_only: bool = False):
    """DB 설정이 있으면 NewsStore를 초기화, 없으면 None 반환"""
    db_cfg = config.get("database", {})
    if not db_cfg.get("enabled", False):
        return None

    try:
        from news.store import NewsStore

        db_path = db_cfg.get("path", "./data/news.duckdb")
        store = NewsStore(db_path=db_path, read_only=read_only)
        if not read_only:
            store.init_schema()
        logger.info("DuckDB news store initialized: %s (read_only=%s)", db_path, read_only)
        return store
    except Exception as e:
        logger.warning("DB initialization failed, continuing without DB: %s", e)
        return None


def init_stock_store(config: dict, read_only: bool = False):
    """DB 설정이 있으면 StockStore를 초기화, 없으면 None 반환"""
    db_cfg = config.get("database", {})
    if not db_cfg.get("enabled", False):
        return None

    try:
        from analysis.store import StockStore

        db_path = db_cfg.get("path", "./data/news.duckdb")
        store = StockStore(db_path=db_path, read_only=read_only)
        if not read_only:
            store.init_schema()
        logger.info("DuckDB stock store initialized: %s (read_only=%s)", db_path, read_only)
        return store
    except Exception as e:
        logger.warning("Stock store initialization failed, continuing without it: %s", e)
        return None
