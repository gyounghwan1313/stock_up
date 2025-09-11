## 프로젝트 구조

```
stock_up/
├── crawler/                   
│   ├── rss_fetcher.py         
│   └── rss_parser.py          
├── sender/                    
│   ├── translator.py          
│   └── slack_sender.py        
├── tests/                     
│   └── sender/
│       └── test_slack_sender.py
└── pyproject.toml      
```

### 1. 프로젝트 클론 및 설치

```bash
cd stock_up

# uv를 사용한 설치 (권장)
uv sync
```

### 2. 환경변수 설정

프로젝트 루트에 `.env` 파일을 생성하고 다음 내용을 작성하세요:

```env
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
OPENAI_API_KEY=sk-your-openai-api-key-here
```

### 개별 모듈 사용 방법

```python
# RSS 크롤링
from crawler.rss_fetcher import RSSFetcher
from crawler.rss_parser import RSSParser

fetcher = RSSFetcher()
xml_content = fetcher.fetch("url")

parser = RSSParser()
items = parser.parse_items(xml_content)
latest_items = parser.get_latest_items(xml_content, limit=5)

# 번역
from sender.translator import GPTTranslator

translator = GPTTranslator()
translated_title = translator.translate_title("US GDP Growth Rate Actual 2.1%")
```

### 테스트 

```bash
# 전체 테스트
pytest

# 특정 모듈 테스트
pytest tests/crawler/test_rss_parser.py -v
pytest tests/sender/test_slack_sender.py -v
```

