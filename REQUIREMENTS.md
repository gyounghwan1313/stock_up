# Stock Up - 요구사항 문서

## 프로젝트 개요

**Stock Up**은 금융 뉴스 RSS 피드를 자동으로 크롤링하여 번역한 후 Slack으로 전송하는 자동화 애플리케이션입니다.

- **목적**: 금융 뉴스(주식 관련)를 자동으로 수집, 번역, 공유
- **실행 주기**: 5분마다 반복 실행
- **주요 사용처**: Slack 팀 채널에 금융 뉴스 자동 게시

---

## 1. 기능 요구사항

### 1.1 RSS 크롤링 및 파싱
- **요구사항**
  - RSS 피드(FinancialJuice) URL에서 최신 뉴스 크롤링
  - XML 형식의 RSS 데이터 파싱
  - 설정 가능한 개수(기본 20개)의 최신 항목 추출
  - 뉴스 제목에서 "FinancialJuice:" 접두사 제거

- **현황**: ✅ 구현 완료
  - `crawler/rss_fetcher.py`: RSS 데이터 다운로드
  - `crawler/rss_parser.py`: RSS 파싱 및 항목 추출

### 1.2 뉴스 제목 번역
- **요구사항**
  - OpenAI API를 사용하여 영어 제목을 한국어로 번역
  - 배치 번역 지원 (여러 제목을 한 번에 번역)
  - 번역 오류 처리

- **현황**: ✅ 구현 완료
  - `sender/translator.py`: GPT 기반 번역 모듈

### 1.3 중복 체크
- **요구사항**
  - 이전에 수집한 뉴스와 중복 여부 확인
  - 중복된 뉴스는 Slack에 전송하지 않음
  - 중복 확인을 위한 저장된 제목 관리
  - 신규 제목과 중복 제목 분류

- **현황**: ✅ 구현 완료
  - `utils/dup_check.py`: 중복 체크 로직

### 1.4 파일 저장
- **요구사항**
  - 신규 뉴스 제목을 파일로 저장
  - 중복 체크에 사용할 데이터 영속성 보장
  - 파일 경로 관리

- **현황**: ✅ 구현 완료
  - `utils/file_ctrl.py`: 파일 저장 및 관리

### 1.5 Slack 메시지 전송
- **요구사항**
  - 신규 뉴스를 Slack 채널로 전송
  - Slack Webhook 또는 Bot Token 사용
  - 메시지 포맷팅 (제목, 링크 등)
  - 전송 실패 처리

- **현황**: 🚧 부분 구현
  - `sender/slack_sender.py`: 슬랙 전송 모듈 존재
  - `main.py`에서 아직 호출되지 않음 (주석 처리)

### 1.6 자동 실행
- **요구사항**
  - 애플리케이션을 데몬으로 지속 실행
  - 5분 간격으로 RSS 크롤링 및 처리
  - 오류 시 재시도 또는 로깅

- **현황**: ✅ 부분 구현
  - `main.py`에서 기본 루프 구조 구현
  - 오류 처리 및 로깅 개선 필요

---

## 2. 기술 스택

| 항목 | 기술 |
|------|------|
| **언어** | Python 3.12+ |
| **패키지 관리** | uv |
| **웹 크롤링** | requests, feedparser |
| **AI/번역** | OpenAI API |
| **메시징** | Slack API (Webhook/Bot Token) |
| **테스트** | pytest, pytest-mock |
| **코드 품질** | black, ruff, pylint, isort |

---

## 3. 환경 변수

다음 환경 변수가 필요합니다 (`.env` 파일):

```env
# RSS 피드 URL
RSS_URL=https://feeds.financialjuice.com/...

# OpenAI API Key (번역용)
OPENAI_API_KEY=sk-...

# Slack 설정 (둘 중 하나 필요)
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
SLACK_BOT_TOKEN=xoxb-...

# 선택사항
SLACK_CHANNEL=#news  # 기본 채널 (Webhook 사용 시)
```

---

## 4. 데이터 흐름

```
1. RSS 크롤링
   └── FinancialJuice RSS URL에서 최신 뉴스 추출 (최대 20개)

2. 제목 정제
   └── "FinancialJuice:" 접두사 제거

3. 번역
   └── OpenAI GPT를 통해 영어 제목을 한국어로 번역

4. 중복 체크
   └── 저장된 제목과 비교하여 신규/중복 분류

5. 파일 저장
   └── 신규 제목을 파일로 저장 (중복 체크 데이터 유지)

6. Slack 전송
   └── 신규 뉴스를 Slack 채널로 전송

7. 반복
   └── 5분 대기 후 1번으로 돌아감
```

---

## 5. 모듈 구조

```
stock_up/
├── crawler/                 # RSS 크롤링 및 파싱
│   ├── rss_fetcher.py      # RSS URL에서 데이터 다운로드
│   └── rss_parser.py       # RSS XML 파싱 및 항목 추출
│
├── sender/                  # 데이터 처리 및 전송
│   ├── translator.py       # OpenAI를 사용한 번역
│   └── slack_sender.py     # Slack 메시지 전송
│
├── utils/                   # 유틸리티 모듈
│   ├── dup_check.py        # 중복 체크 로직
│   └── file_ctrl.py        # 파일 저장/로드
│
├── tests/                   # 테스트
│   └── sender/
│       └── test_slack_sender.py
│
└── main.py                  # 메인 애플리케이션 루프
```

---

## 6. 개발 요구사항

### 6.1 코드 품질
- Python 3.12+ 준수
- Black으로 코드 포맷팅
- Ruff/Pylint로 린트 검사
- 테스트 커버리지 유지

### 6.2 테스트
- 단위 테스트 (pytest)
- Mock을 사용한 외부 API 테스트
- 테스트 설정: `pyproject.toml` 참고

### 6.3 CI/CD
- GitHub Actions를 통한 자동 검사 (`.github/workflows/` 참고)
- Pre-commit hooks (`.pre-commit-config.yaml`)

---

## 7. 알려진 문제 및 개선 사항

### 현재 상태
- [ ] Slack 메시지 전송 기능 완성 (주석 처리되어 있음)
- [ ] 오류 처리 및 로깅 강화
- [ ] 실패한 항목 재시도 로직
- [ ] 크롤링 성공/실패 로깅

### 예정된 개선
- RSS 피드 여러 개 지원
- 메시지 포맷팅 커스터마이징
- 데이터베이스를 통한 중복 체크 개선
- 상태 모니터링 및 알림

---

## 8. 실행 방법

```bash
# 설치
uv sync

# 환경 변수 설정
cp .env.example .env
# .env 파일에 API 키 등 설정

# 실행
python main.py

# 테스트
pytest

# 코드 포맷팅
black .
isort .

# 린트 검사
ruff check .
pylint --recursive=y .
```

---

## 9. 참고사항

- RSS URL, API 키 등 민감한 정보는 `.env` 파일에 저장
- 모든 외부 API 호출은 오류 처리 필요
- 5분 간격 실행으로 API 비용 및 요청 수 제한
- Slack 전송 전에 중복 체크를 통해 중복 메시지 방지
