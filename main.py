
import os
from time import sleep
from dotenv import load_dotenv

from crawler.rss_fetcher import RSSFetcher
from crawler.rss_parser import RSSParser
from sender.translator import GPTTranslator
from utils.dup_check import DuplicateChecker
from utils.file_ctrl import save_file


def main():
    load_dotenv()
    RSS_URL = os.environ.get("RSS_URL")
    SIZE=20

    while True:
        # RSS 불러오기
        fetcher = RSSFetcher()
        xml_content = fetcher.fetch(RSS_URL)

        # 파싱
        parser = RSSParser()
        latest_items = parser.get_latest_items(xml_content, limit=SIZE)
        titles =  [i.title.strip("FinancialJuice:").strip() for i in latest_items]

        # 번역
        translator = GPTTranslator()
        translated_title = translator.translate_titles(titles)

        # 중복체크
        dup_checker = DuplicateChecker()
        dup_check_result = dup_checker.check(translated_title)

        # slack 보내기

        # 신규 제목 저장
        for new_title, new_file_path in dup_check_result['new'].items():
            save_file(file_path=new_file_path, title=new_title)





        sleep(60*5)