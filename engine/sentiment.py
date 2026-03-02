import json
import logging
import os
from typing import Optional

from openai import OpenAI

logger = logging.getLogger(__name__)

SENTIMENT_PROMPT = """Analyze the sentiment of the following financial news headlines.
Rate each headline from -1.0 (very bearish) to +1.0 (very bullish).
Return ONLY a single float number representing the average sentiment score.

Headlines:
{headlines}
"""

SENTIMENT_BATCH_PROMPT = """Analyze the sentiment of each financial news headline below.
Rate each from -1.0 (very bearish) to +1.0 (very bullish).
Return ONLY a JSON array of floats in the same order as the headlines.
Example: [-0.5, 0.8, 0.0]

Headlines:
{headlines}
"""


class SentimentAnalyzer:
    def __init__(self, model: str = "gpt-4o-mini", api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.client = OpenAI(api_key=self.api_key) if self.api_key else None
        self.model = model

    def analyze(self, headlines: list[str]) -> float:
        if not headlines or not self.client:
            return 0.0

        try:
            prompt = SENTIMENT_PROMPT.format(headlines="\n".join(f"- {h}" for h in headlines))
            request_params = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "You are a financial sentiment analyzer. Respond with only a number."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 10,
            }
            logger.debug("OpenAI request: %s", json.dumps(request_params, ensure_ascii=False))
            response = self.client.chat.completions.create(**request_params)
            score_text = response.choices[0].message.content.strip()
            logger.debug("OpenAI response: %s", score_text)
            score = float(score_text)
            return max(-1.0, min(1.0, score))
        except Exception as e:
            logger.error("Sentiment analysis failed: %s", e)
            return 0.0

    def analyze_batch(self, headlines: list[str]) -> list[float]:
        """헤드라인 목록을 한 번의 API 호출로 개별 감성점수 리스트로 반환."""
        if not headlines or not self.client:
            return [0.0] * len(headlines)

        try:
            prompt = SENTIMENT_BATCH_PROMPT.format(
                headlines="\n".join(f"{i+1}. {h}" for i, h in enumerate(headlines))
            )
            request_params = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "You are a financial sentiment analyzer. Respond with only a JSON array of floats."},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": max(20, len(headlines) * 8),
            }
            logger.debug("OpenAI batch request: %s", json.dumps(request_params, ensure_ascii=False))
            response = self.client.chat.completions.create(**request_params)
            response_text = response.choices[0].message.content.strip()
            logger.debug("OpenAI batch response: %s", response_text)

            # Strip markdown code fences if present (e.g. ```json ... ```)
            if response_text.startswith("```"):
                lines = response_text.splitlines()
                response_text = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

            scores = json.loads(response_text)
            if not isinstance(scores, list):
                raise ValueError(f"Expected list, got {type(scores)}")

            result = [max(-1.0, min(1.0, float(s))) for s in scores]
            # 개수 불일치 시 0.0으로 채움
            if len(result) < len(headlines):
                result.extend([0.0] * (len(headlines) - len(result)))
            return result[:len(headlines)]

        except Exception as e:
            logger.error("Batch sentiment analysis failed: %s", e)
            return [0.0] * len(headlines)
