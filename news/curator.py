import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Optional

from openai import OpenAI

from shared.categories import CATEGORIES

logger = logging.getLogger(__name__)

categories_str = ", ".join(CATEGORIES)

CURATION_PROMPT = """You are a senior financial news editor and curator.
Your job is to process a batch of raw English news headlines into curated news groups
for professional traders and investors.

## Current World Context
{world_context}

## Your Tasks (do ALL of these in a single response)

### 1. Group similar headlines
- Headlines about the same event, company, or topic should be grouped together.
- Example: multiple insider selling reports for the same company → one group.
- Headlines that are unique stay as their own group (with a single headline).

### 2. Write a Korean summary for each group
- NOT a literal translation — write a clear, concise summary that captures the essence.
- Use explanatory tone (해설체), natural Korean.
- Keep important terms with English in parentheses: 금리 인하(rate cut)
- For groups with multiple headlines, synthesize them into one summary.
  Example: 4 headlines about Delek executives selling stock →
  "Delek US Holdings 임원 다수, 총 630만 달러 규모 주식 매도"
- For single headlines, translate clearly with context from the world briefing above.

### 3. Rate importance (1-10)
Use the world context to assess whether this is NEW, SURPRISING, or MARKET-MOVING.
- 9-10: Market-moving (unexpected rate decisions, geopolitical escalations, major earnings surprises)
- 7-8: Significant for specific sectors/assets. New information investors would act on.
- 5-6: Noteworthy but expected (scheduled data in line with expectations, routine actions)
- 3-4: Low-value noise (routine CFTC positions, minor FX moves, restating known facts)
- 1-2: Irrelevant noise

### 4. Assign ONE category
Available: {categories}
IMPORTANT: Be consistent. Headlines of the same nature MUST get the same category.
- Executive/insider stock sales → 기업실적
- Central bank rate decisions → 금리/통화정책
- Military/war/geopolitical → 지정학/무역

### 5. Extract stock symbols
- Only for companies that are the PRIMARY SUBJECT.
- Use standard US tickers (AAPL, MSFT, etc.)

## Headlines
{headlines}

## Response Format
Respond with ONLY valid JSON:
{{
  "groups": [
    {{
      "headline_indices": [0, 3, 5],
      "summary": "Korean summary here",
      "category": "기업실적",
      "importance": 4,
      "reason": "Brief explanation (max 20 words)",
      "symbols": ["DK"]
    }}
  ]
}}

Rules:
- Every headline index (0 to N-1) must appear in exactly one group.
- headline_indices uses 0-based indexing.
- Do NOT omit any headline.
"""


@dataclass
class CuratedGroup:
    group_id: str
    headline_indices: list[int]
    summary: str
    category: str
    importance: int
    reason: str
    symbols: list[str] = field(default_factory=list)


class NewsCurator:
    def __init__(
        self,
        model: str = "gpt-4o-mini",
        api_key: Optional[str] = None,
        importance_threshold: int = 6,
        batch_size: int = 30,
    ):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.client = OpenAI(api_key=self.api_key) if self.api_key else None
        self.model = model
        self.importance_threshold = importance_threshold
        self.batch_size = batch_size

    def curate(
        self, titles: list[str], world_context: str = ""
    ) -> list[CuratedGroup]:
        if not titles:
            return []

        if not self.client:
            logger.warning("No OpenAI client, returning ungrouped results")
            return self._fallback(titles)

        all_groups: list[CuratedGroup] = []

        for chunk_start in range(0, len(titles), self.batch_size):
            chunk = titles[chunk_start : chunk_start + self.batch_size]
            chunk_groups = self._curate_chunk(chunk, world_context)
            for g in chunk_groups:
                g.headline_indices = [i + chunk_start for i in g.headline_indices]
            all_groups.extend(chunk_groups)

        return all_groups

    def _curate_chunk(
        self, titles: list[str], world_context: str
    ) -> list[CuratedGroup]:
        headlines_text = "\n".join(f"{i}. {t}" for i, t in enumerate(titles))
        prompt = CURATION_PROMPT.format(
            world_context=world_context or "(No context available)",
            categories=categories_str,
            headlines=headlines_text,
        )
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a financial news curator. Respond only with JSON.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                max_tokens=max(200, len(titles) * 80),
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content.strip()
            parsed = json.loads(content)

            raw_groups = parsed.get("groups", [])
            if not raw_groups:
                logger.warning("LLM returned empty groups, using fallback")
                return self._fallback(titles)

            groups = []
            seen_indices: set[int] = set()
            for g in raw_groups:
                indices = g.get("headline_indices", [])
                indices = [i for i in indices if isinstance(i, int) and 0 <= i < len(titles)]
                if not indices:
                    continue

                importance = int(g.get("importance", 5))
                importance = max(1, min(10, importance))

                category = g.get("category", "기타")
                if category not in CATEGORIES:
                    category = "기타"

                symbols = [
                    s.upper().strip()
                    for s in g.get("symbols", [])
                    if isinstance(s, str) and s.strip()
                ]

                group = CuratedGroup(
                    group_id=str(uuid.uuid4())[:8],
                    headline_indices=indices,
                    summary=g.get("summary", titles[indices[0]]),
                    category=category,
                    importance=importance,
                    reason=g.get("reason", ""),
                    symbols=symbols,
                )
                groups.append(group)
                seen_indices.update(indices)

            for i in range(len(titles)):
                if i not in seen_indices:
                    groups.append(
                        CuratedGroup(
                            group_id=str(uuid.uuid4())[:8],
                            headline_indices=[i],
                            summary=titles[i],
                            category="기타",
                            importance=5,
                            reason="not scored by LLM",
                        )
                    )

            return groups

        except Exception as e:
            logger.error("News curation failed: %s", e)
            return self._fallback(titles)

    def _fallback(self, titles: list[str]) -> list[CuratedGroup]:
        return [
            CuratedGroup(
                group_id=str(uuid.uuid4())[:8],
                headline_indices=[i],
                summary=title,
                category="기타",
                importance=5,
                reason="fallback - no curation",
            )
            for i, title in enumerate(titles)
        ]
