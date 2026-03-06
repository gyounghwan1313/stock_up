import json
import logging
import os
import time
from typing import List, Optional

from openai import OpenAI

logger = logging.getLogger(__name__)


CATEGORIES = [
    "금리/통화정책", "인플레이션/물가", "고용/노동시장", "기업실적",
    "반도체/AI", "에너지/원자재", "지정학/무역", "부동산/건설",
    "암호화폐/디지털자산", "은행/금융", "소비/유통", "IPO/M&A",
    "규제/정책", "기술/소프트웨어", "헬스케어/바이오", "기타",
]


class GPTTranslator:
    """GPT를 사용한 경제 뉴스 번역기"""

    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-3.5-turbo"):
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API 키가 필요합니다. OPENAI_API_KEY 환경변수를 설정하거나 api_key 파라미터를 제공하세요.")

        self.client = OpenAI(api_key=self.api_key)
        self.model = model

        # 경제 뉴스 번역을 위한 시스템 프롬프트
        self.system_prompt = """
You are a professional translator specialized in economics and finance.
When I provide an English news title about the economy, translate it into Korean.

Translation rules:
- Use a clear and reader-friendly explanatory tone (해설체), not a rigid newspaper style.
- Keep important economic/financial terms in Korean with the original English term in parentheses the first time they appear.
  Example: 물가 상승(Inflation), 금리 인하(interest rate cut)
- Break down long sentences into shorter, easy-to-read sentences.
- Ensure the translation is natural and understandable for general Korean readers without losing the economic nuance.
- Only respond with the translated title, nothing else.
"""

        categories_str = ", ".join(CATEGORIES)
        self.categorize_system_prompt = f"""
You are a professional translator and news categorizer specialized in economics and finance.
When I provide an English news title about the economy, do three things:
1. Translate it into Korean.
2. Assign 1-3 categories from the following list that best describe the news topic.
3. Extract stock ticker symbols for companies that are the PRIMARY SUBJECT of the news (not merely mentioned in passing).

Available categories: {categories_str}

Translation rules:
- Use a clear and reader-friendly explanatory tone (해설체), not a rigid newspaper style.
- Keep important economic/financial terms in Korean with the original English term in parentheses the first time they appear.
- Break down long sentences into shorter, easy-to-read sentences.
- Ensure the translation is natural and understandable for general Korean readers without losing the economic nuance.

Symbol extraction rules:
- Only include symbols for companies that the news is ABOUT (the main subject/actor).
- Do NOT include symbols for companies that are only mentioned as context, comparison, or background.
- Examples:
  - "JPMorgan raises interest rate forecast" → ["JPM"] (JPMorgan is the subject)
  - "Fed signals rate cut amid JPMorgan warning" → [] (JPMorgan is just providing context)
  - "Apple and Microsoft earnings beat estimates" → ["AAPL", "MSFT"] (both are subjects)
- Use standard US stock ticker symbols (e.g. AAPL, MSFT, GOOGL, AMZN, TSLA, NVDA, META, JPM, GS, etc.)
- If no company is clearly the primary subject, return an empty array.

Respond ONLY with valid JSON in this format:
{{"translation": "번역된 제목", "categories": ["카테고리1", "카테고리2"], "symbols": ["TICKER1"]}}
"""
    
    def translate_title(self, english_title: str) -> str:
        """단일 제목을 번역합니다"""
        try:
            request_params = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": english_title},
                ],
                "temperature": 0.3,
                "max_tokens": 150,
            }
            logger.debug("OpenAI request: %s", json.dumps(request_params, ensure_ascii=False))
            response = self.client.chat.completions.create(**request_params)
            
            translated = response.choices[0].message.content.strip()
            logger.debug("OpenAI response: %s", translated)
            # 따옴표 제거
            if translated.startswith('"') and translated.endswith('"'):
                translated = translated[1:-1]

            return translated

        except Exception as e:
            logger.error("번역 중 오류 발생: %s", e)
            return english_title  # 번역 실패시 원문 반환
    
    def translate_titles(self, english_titles: List[str], delay: float = 1.0) -> List[str]:
        """여러 제목을 번역합니다 (API 호출 제한을 위한 지연 포함)"""
        translated_titles = []
        
        for i, title in enumerate(english_titles):
            if i > 0:  # 첫 번째 호출이 아닌 경우 지연
                time.sleep(delay)

            translated = self.translate_title(title)
            translated_titles.append(translated)
            logger.info("번역 완료 (%d/%d): %s... → %s...", i + 1, len(english_titles), title[:50], translated[:50])
        
        return translated_titles
    
    def translate_and_categorize(self, english_title: str) -> tuple[str, List[str], List[str]]:
        """단일 제목을 번역하고 카테고리를 태깅하고 관련 심볼을 추출합니다.

        Returns:
            (번역된 제목, 카테고리 이름 리스트, 심볼 리스트) 튜플
        """
        try:
            request_params = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": self.categorize_system_prompt},
                    {"role": "user", "content": english_title},
                ],
                "temperature": 0.3,
                "max_tokens": 400,
                "response_format": {"type": "json_object"},
            }
            logger.debug("OpenAI categorize request: %s", json.dumps(request_params, ensure_ascii=False))
            response = self.client.chat.completions.create(**request_params)

            content = response.choices[0].message.content.strip()
            logger.debug("OpenAI categorize response: %s", content)

            data = json.loads(content)
            translated = data.get("translation", english_title)
            categories = data.get("categories", [])
            symbols = data.get("symbols", [])

            # 따옴표 제거
            if translated.startswith('"') and translated.endswith('"'):
                translated = translated[1:-1]

            # 유효한 카테고리만 필터링
            valid_categories = [c for c in categories if c in CATEGORIES]
            if not valid_categories:
                valid_categories = ["기타"]

            # 심볼은 대문자 문자열 리스트로 정규화
            valid_symbols = [s.upper().strip() for s in symbols if isinstance(s, str) and s.strip()]

            return translated, valid_categories, valid_symbols

        except Exception as e:
            logger.error("번역+카테고리 태깅 중 오류 발생: %s", e)
            return english_title, ["기타"], []

    def translate_and_categorize_titles(
        self, english_titles: List[str], delay: float = 1.0
    ) -> tuple[List[str], List[List[str]], List[List[str]]]:
        """여러 제목을 번역 + 카테고리 태깅 + 심볼 추출합니다.

        Returns:
            (번역 리스트, 카테고리 리스트, 심볼 리스트) 튜플
        """
        translated_titles = []
        categories_list = []
        symbols_list = []

        for i, title in enumerate(english_titles):
            if i > 0:
                time.sleep(delay)

            translated, categories, symbols = self.translate_and_categorize(title)
            translated_titles.append(translated)
            categories_list.append(categories)
            symbols_list.append(symbols)
            logger.info(
                "번역+카테고리+심볼 완료 (%d/%d): %s... → %s... [%s] {%s}",
                i + 1, len(english_titles), title[:50], translated[:50],
                ", ".join(categories), ", ".join(symbols),
            )

        return translated_titles, categories_list, symbols_list

    def translate_batch(self, english_titles: List[str], batch_size: int = 5) -> List[str]:
        """배치로 여러 제목을 번역합니다"""
        if not english_titles:
            return []
        
        # 제목들을 하나의 요청으로 묶어서 처리
        titles_text = "\n".join([f"{i+1}. {title}" for i, title in enumerate(english_titles[:batch_size])])
        
        try:
            request_params = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": f"다음 경제 뉴스 제목들을 한국어로 번역해주세요. 각 번호에 맞춰 번역된 제목만 응답해주세요:\n\n{titles_text}"},
                ],
                "temperature": 0.3,
                "max_tokens": 500,
            }
            logger.debug("OpenAI batch request: %s", json.dumps(request_params, ensure_ascii=False))
            response = self.client.chat.completions.create(**request_params)
            
            translated_text = response.choices[0].message.content.strip()
            logger.debug("OpenAI batch response: %s", translated_text)

            # 번호별로 분리
            translated_titles = []
            lines = translated_text.split('\n')
            
            for line in lines:
                line = line.strip()
                if line and (line[0].isdigit() or line.startswith('1.') or line.startswith('2.') or 
                           line.startswith('3.') or line.startswith('4.') or line.startswith('5.')):
                    # 번호와 점 제거
                    title = line.split('.', 1)[1].strip() if '.' in line else line
                    if title.startswith('"') and title.endswith('"'):
                        title = title[1:-1]
                    translated_titles.append(title)
            
            # 원본 리스트 길이와 맞춰주기
            while len(translated_titles) < len(english_titles[:batch_size]):
                missing_index = len(translated_titles)
                translated_titles.append(english_titles[missing_index])
            
            return translated_titles[:len(english_titles)]
            
        except Exception as e:
            logger.error("배치 번역 중 오류 발생: %s", e)
            return english_titles  # 번역 실패시 원문 반환