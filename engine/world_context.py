import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

from openai import OpenAI

logger = logging.getLogger(__name__)

WORLD_CONTEXT_PROMPT = """You are a senior geopolitical and macroeconomic analyst.
Provide a concise briefing (300-400 words) of the CURRENT state of the world as of {date}.
Cover these areas:
1. Ongoing military conflicts and geopolitical tensions
2. Major central bank policies (Fed, ECB, BOJ, PBOC) and interest rate stance
3. Inflation trends across major economies
4. Trade wars, sanctions, or tariff developments
5. Major economic indicators trend (GDP growth, unemployment, PMI)
6. Commodity market dynamics (oil, gold, key metals)
7. Technology sector developments (AI, semiconductors)
8. Any ongoing financial crises or systemic risks

Be factual and specific. Include names, numbers, and dates where possible.
This briefing will be used to help assess the importance of incoming financial news headlines."""


class WorldContextProvider:
    def __init__(
        self,
        model: str = "gpt-4o-mini",
        api_key: Optional[str] = None,
        cache_hours: int = 24,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.client = OpenAI(api_key=self.api_key) if self.api_key else None
        self.model = model
        self.cache_hours = cache_hours
        self._cached_context: Optional[str] = None
        self._cached_at: Optional[datetime] = None

    def get_context(self) -> str:
        """Return current world context, refreshing if stale."""
        now = datetime.now(timezone.utc)
        if (
            self._cached_context is not None
            and self._cached_at is not None
            and (now - self._cached_at) < timedelta(hours=self.cache_hours)
        ):
            return self._cached_context

        self._cached_context = self._fetch_context()
        self._cached_at = datetime.now(timezone.utc)
        return self._cached_context

    def _fetch_context(self) -> str:
        if not self.client:
            logger.warning("No OpenAI client available for world context")
            return ""
        try:
            date_str = datetime.now().strftime("%Y-%m-%d")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a senior macroeconomic and geopolitical analyst.",
                    },
                    {
                        "role": "user",
                        "content": WORLD_CONTEXT_PROMPT.format(date=date_str),
                    },
                ],
                temperature=0.3,
                max_tokens=800,
            )
            context = response.choices[0].message.content.strip()
            logger.info("World context refreshed (%d chars)", len(context))
            return context
        except Exception as e:
            logger.error("World context fetch failed: %s", e)
            return self._cached_context or ""
