
import os
from typing import List, Dict
from datetime import datetime, timedelta
import hashlib



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


if __name__ == '__main__':
    checker = DuplicateChecker()

    title_list = [ 'MXN CFTC 포지션 업데이트 - FJElit',
 '엔화 CFTC 포지션 업데이트 - FJElit',
 '미국 달러 CFTC 포지션 업데이트 - FJElit']
    checker.check(title_list)