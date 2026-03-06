# 검증 규칙

작업 완료 후 아래 절차를 반드시 수행하고, 실패 시 코드를 수정할 것.

1. 컨테이너가 이미 떠있으면 `docker-compose down`으로 내린 후 `docker-compose up --build -d`로 다시 실행
2. 컨테이너 로그 확인 - error가 있으면 문제 해결
3. Slack 채널(C0AHPTBAUKD) 메시지 확인 - 메시지 전송 주기까지 기다려도 메시지가 오지 않으면 fail
