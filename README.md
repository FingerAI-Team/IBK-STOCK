### Stock-Service
|IBK 챗봇 로그를 수집하고 주식 관련 질문을 분류하여 DB에 저장하는 데이터 파이프라인 서비스 
---
### Pipeline 
```Plain text
Chat API -> Data Collector -> TransformPipe -> DataClassifier -> StorePipe -> PostgreSQL 

Model Train 
Stock Data -> 
```

### Features 
- ChatAPI 로그 수집 
- Q/A 메시지 분리
- 주식 질문 분류 
- 중복 로그 방지
- UTC / KST 시간 처리 
- 스케쥴 기반 파이프라인 실행 

### RUN 
```bash
# Single Run
python main.py --mode once --start_date 2026-03-01

# Scheduler 
python main.py --mode schedule 

# Backfill
python main.py --backfill --start_date 2026-01-01
```