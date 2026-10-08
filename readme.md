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
- 대시보드 접속
  - http://localhost:8080
  - 특징 
    - 한글화 잘 적용
    - 메인 : 대시보드 중심 (현황, 모니터링)
    - UI 재편 -> 추적, 스케줄 관리 확장
      - 스케줄링 -> 이벤트 트리거 중심 전환
    - DAG 추가 후에 자세하게 체크

- 계정 조회
  ```
    docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
    ---
    {"admin": "txpWKSKHHVxmTfaG"}
  ```

- 관리자
    - 커넥션들
        - + 커넥션 추가
        ```
            ID : aws_default
            유형 : Amazon Web Services
            KEY ID : 
            Access key : 
            추가 필드 JSON
                {"region_name":"ap-northeast-2"}
        ```

- 디비 접속 확인 (구성이 잘되었는지 점검)
```
# [v] 이커머스용 접속
docker compose exec postgres psql -U ecommerce -d ecommerce
---
PS C:\Users\NT551_11TH\Desktop\workspace\airflow-medallion-rag> docker compose exec postgres psql -U ecommerce -d ecommerce                              
psql (16.15 (Debian 16.15-1.pgdg12+2))
Type "help" for help.

# 테이블 확인
ecommerce=> \dt
                List of relations
 Schema |        Name         | Type  |   Owner   
--------+---------------------+-------+-----------
 public | daily_kpi           | table | ecommerce
 public | document_chunks     | table | ecommerce
 public | knowledge_documents | table | ecommerce
 public | product_metrics     | table | ecommerce
(4 rows)

# 사용자 확인
ecommerce=> \l
                                                       List of databases
   Name    |   Owner   | Encoding | Locale Provider |  Collate   |   Ctype    | ICU Locale | ICU Rules |   Access privileges   
-----------+-----------+----------+-----------------+------------+------------+------------+-----------+-----------------------
 airflow   | airflow   | UTF8     | libc            | en_US.utf8 | en_US.utf8 |            |           | 
 ecommerce | ecommerce | UTF8     | libc            | en_US.utf8 | en_US.utf8 |            |           | 
 postgres  | postgres  | UTF8     | libc            | en_US.utf8 | en_US.utf8 |            |           | 
 template0 | postgres  | UTF8     | libc            | en_US.utf8 | en_US.utf8 |            |           | =c/postgres          +
           |           |          |                 |            |            |            |           | postgres=CTc/postgres
 template1 | postgres  | UTF8     | libc            | en_US.utf8 | en_US.utf8 |            |           | =c/postgres          +
           |           |          |                 |            |            |            |           | postgres=CTc/postgres
(5 rows)

# 확장팩 설치 확인 -> 백터
ecommerce=> \dx
                             List of installed extensions
  Name   | Version |   Schema   |                     Description                      
---------+---------+------------+------------------------------------------------------
 plpgsql | 1.0     | pg_catalog | PL/pgSQL procedural language
 vector  | 0.8.6   | public     | vector data type and ivfflat and hnsw access methods
(2 rows)

ecommerce=> exit;

# airflow용 접속 
docker compose exec postgres psql -U airflow -d airflow    
```

# DAG 구성
```
/
L dags
  L ecommerce_silver_to_gold.py : 실버 데이터 -> 가공 -> 골드 데이터 구성 (분석, 지식 병렬 처리)
```