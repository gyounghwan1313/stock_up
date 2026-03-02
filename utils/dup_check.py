
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


if __name__ == '__main__':
    checker = DuplicateChecker()

    title_list = [ 'MXN CFTC 포지션 업데이트 - FJElit',
 '엔화 CFTC 포지션 업데이트 - FJElit',
 '미국 달러 CFTC 포지션 업데이트 - FJElit']
    checker.check(title_list)