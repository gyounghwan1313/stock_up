
import logging
import math
import os
from typing import List, Dict
from datetime import datetime, timedelta
import hashlib

logger = logging.getLogger(__name__)



def hash_title(title: str) -> str:
    """제목을 SHA-256 해시로 변환"""
    return hashlib.sha256(title.encode('utf-8')).hexdigest()

class DuplicateChecker:

    def __init__(self):
        self.__base_dir = "./data"

    def is_file_exist(self, file_name: str):
        """이미 저장된 파일(제목)이 없으면 파일 저장 경로 리턴"""
        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        today_file_path = os.path.join(self.__base_dir, today, file_name)
        yesterday_file_path = os.path.join(self.__base_dir, yesterday, file_name)

        if os.path.exists(today_file_path) or os.path.exists(yesterday_file_path):
            return None
        else:
            return today_file_path


    def check(self, title_list: List[str]) -> Dict:
        """어제/오늘 동일한 제목이 수집되어 있는지 확인"""
        result_dict = {"new": {}, "duplicate": {}}
        for title in title_list:
            file_name = f"{hash_title(title)}.txt"
            file_path = self.is_file_exist(file_name)

            if file_path:
                result_dict["new"][title] = file_path
            else:
                result_dict["duplicate"][title] = file_path

        return result_dict


    def check_signal_duplicate(self, symbol: str, signal_type: str) -> bool:
        """같은 날 동일 종목+시그널이 이미 발생했는지 확인"""
        key = f"signal_{symbol}_{signal_type}"
        file_name = f"{hash_title(key)}.txt"
        return self.is_file_exist(file_name) is None

    def mark_signal_sent(self, symbol: str, signal_type: str) -> None:
        """시그널 전송 기록"""
        from utils.file_ctrl import save_file

        key = f"signal_{symbol}_{signal_type}"
        file_name = f"{hash_title(key)}.txt"
        file_path = self.is_file_exist(file_name)
        if file_path:
            save_file(file_path=file_path, title=key)


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def deduplicate_similar(items: List[dict], threshold: float = 0.85) -> List[dict]:
    """Embedding 기반 의미적 중복 제거. items는 cleaned_title 키를 가진 dict 리스트.

    OpenAI text-embedding-3-small으로 제목을 벡터화한 뒤 코사인 유사도를 계산.
    threshold 이상인 쌍에서 뒤쪽(나중에 수집된) 아이템을 제거한다.
    살아남은 각 아이템 dict에 "embedding" 키로 벡터를 첨부하여 반환 — DB 저장에 재사용.
    OPENAI_API_KEY가 없으면 중복 제거 없이 원본 반환.
    """
    if not items:
        return items

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        logger.warning("[dedup] OPENAI_API_KEY not set, skipping similarity dedup")
        return items

    titles = [c["cleaned_title"] for c in items]

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
        response = client.embeddings.create(
            model="text-embedding-3-small",
            input=titles,
        )
        vectors = [r.embedding for r in response.data]
    except Exception as e:
        logger.warning("[dedup] Embedding API error, skipping dedup: %s", e)
        return items

    removed: set[int] = set()
    for i in range(len(items)):
        if i in removed:
            continue
        for j in range(i + 1, len(items)):
            if j in removed:
                continue
            sim = _cosine_similarity(vectors[i], vectors[j])
            if sim >= threshold:
                removed.add(j)

    kept = []
    for idx, item in enumerate(items):
        if idx not in removed:
            item["embedding"] = vectors[idx]
            kept.append(item)

    n_removed = len(items) - len(kept)
    if n_removed:
        logger.info("[dedup] removed %d similar items (threshold=%.2f)", n_removed, threshold)
    return kept


if __name__ == '__main__':
    checker = DuplicateChecker()

    title_list = [ 'MXN CFTC 포지션 업데이트 - FJElit',
 '엔화 CFTC 포지션 업데이트 - FJElit',
 '미국 달러 CFTC 포지션 업데이트 - FJElit']
    checker.check(title_list)