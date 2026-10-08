# 목표
- Silver -> Gold 데이터 파이프라인 구성중 (2가지 방향성 데이터 적제)
    - 분석용 Gold 데이터 (기존 적용 동일)
    - 지식용 Gold 데이터 (RAG용, 청킹, 임베딩)

```text
                         S3 Silver
                             |
                      Airflow Sensor
                             |
                      inspect_silver
                             |
             +---------------+---------------+  <- 병렬작업, fan-in >
             |                               |
             v                               v
      Analytics Gold                  Knowledge Gold
             |                               |
      product_metrics                    documents
        daily_kpi                           |
             |                           Chunking
             |                               |
             |                           Embedding
             |                               |
             v                               v
      PostgreSQL Tables               pgvector Tables
             |                               |
             +---------------+---------------+  <- fan-out >
                             |
                          작업 완료
                             |                             
                          RAG/Agent
```

# 핵심 개념
## Analytics Gold
- 계산, Group by, join, kpi등 적용
- PostgreSQL 관계형 테이블에 적용

## Knowledge Gold
- 데이터
    - 리뷰, cs, 환불사유, 정책 -> 공통 문서 모델로 변환 처리
- 청킹(task), 임베딩(task)
- PostgreSQL pgvector에 적용

# Airflow
- 3.x 사용
    - Task, TaskGroup(Task-> Task-> Task), fan-in/fan-out => 병렬 작업
    - task 작업 시퀀스 구성 다변화
    - XCom, retry

# 더미 데이터 업로드
- .env 생성
    - ECOMMERCE_BUCKET=<본인 버킷명>
- 오늘 날짜로 변경
```
/
L sample_data
    L scripts
        L dt=2026-10-08 <- 수정(당일 날짜)
```
- 업로드 
```
python -m scripts.upload_silver --date 2026-10-08
```
- 최종 s3
```text
s3://ECOMMERCE_BUCKET/
└── silver/
    └── dt=2026-10-08/
        ├── products.csv
        ├── orders.csv
        ├── refunds.csv
        ├── reviews.csv
        ├── cs_tickets.csv
        ├── policies.csv
        └── _SUCCESS
```

# 로컬 환경 구성
- docker-compose
    - airflow
        - DockerFile 구성
    - postgresql
        - image 구성
    - 설치 
      ```
        docker compose up -d --build
      ```