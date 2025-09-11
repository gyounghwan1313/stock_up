import os
from typing import List, Optional
from openai import OpenAI
import time


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
    
    def translate_title(self, english_title: str) -> str:
        """단일 제목을 번역합니다"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": english_title}
                ],
                temperature=0.3,
                max_tokens=150
            )
            
            translated = response.choices[0].message.content.strip()
            # 따옴표 제거
            if translated.startswith('"') and translated.endswith('"'):
                translated = translated[1:-1]
            
            return translated
            
        except Exception as e:
            print(f"번역 중 오류 발생: {e}")
            return english_title  # 번역 실패시 원문 반환
    
    def translate_titles(self, english_titles: List[str], delay: float = 1.0) -> List[str]:
        """여러 제목을 번역합니다 (API 호출 제한을 위한 지연 포함)"""
        translated_titles = []
        
        for i, title in enumerate(english_titles):
            if i > 0:  # 첫 번째 호출이 아닌 경우 지연
                time.sleep(delay)
            
            translated = self.translate_title(title)
            translated_titles.append(translated)
            print(f"번역 완료 ({i+1}/{len(english_titles)}): {title[:50]}... → {translated[:50]}...")
        
        return translated_titles
    
    def translate_batch(self, english_titles: List[str], batch_size: int = 5) -> List[str]:
        """배치로 여러 제목을 번역합니다"""
        if not english_titles:
            return []
        
        # 제목들을 하나의 요청으로 묶어서 처리
        titles_text = "\n".join([f"{i+1}. {title}" for i, title in enumerate(english_titles[:batch_size])])
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": f"다음 경제 뉴스 제목들을 한국어로 번역해주세요. 각 번호에 맞춰 번역된 제목만 응답해주세요:\n\n{titles_text}"}
                ],
                temperature=0.3,
                max_tokens=500
            )
            
            translated_text = response.choices[0].message.content.strip()
            
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
            print(f"배치 번역 중 오류 발생: {e}")
            return english_titles  # 번역 실패시 원문 반환