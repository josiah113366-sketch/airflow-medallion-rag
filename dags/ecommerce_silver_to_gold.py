# 모듈 가져오기
from __future__ import annotations
import hashlib
import io
import json
import math
import os
from datetime import timedelta
from typing import Any
import boto3
import pandas as pd
import pendulum
import logging
from airflow.sdk import Param, dag, task, task_group, get_current_context # 필수 요소
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# 전역 변수(환경 변수) -> .env는 적용된 상태임 (airflow 내부 컨테이너)
AWS_REGION          = os.getenv("AWS_REGION", "ap-northeast-2")
BUCKET              = os.getenv("ECOMMERCE_BUCKET", "de-ai-16-infra-s3-bk-827913617635")
BEDROCK_EMBED_MODEL = os.getenv("BEDROCK_EMBED_MODEL", "amazon.titan-embed-text-v2:0")  
BEDROCK_REGION      = os.getenv("BEDROCK_REGION","us-east-1")
log                 = logging.getLogger(__name__)

# 공용/공통 등 함수 

# DAG (@dag), 특정 함수에 @dag 데커레이터 추가하면 DAG 구성됨
@dag(
  # airflow ui(대시보드)에 표시되는 DAG ID 
  dag_id      ="ecommerce_s_to_g", 
  # dag 설명
  description ="s3 silver -> 분석/지식 gold -> rds + vector"  ,
  start_date  = pendulum.datetime(2026,10,1, tz="Asia/Seoul"),
  # 트리거 방식 진행 (버튼 클릭)
  schedule    = None, 
  catchup     = False, 
  default_args = {
    "owner": "ai-16",
    # 실패 시 재시도 2회
    "retries": 2, 
    # 간격 30초 대기
    "retry_delay": timedelta(seconds=30)
  }, 
  # 실습 환경 통제하기 위해 임의로 파라미터 전달 
  params = {
    "process_date":Param(
      "2026-10-08", 
      type="string",
      description="실버 데이터의 파티션 정보, ti의 실행 날짜 정보",
    )
  },
  # 태그 
  tags = ["medallion", "gold", "rag", "vector"]
) 
def ecommerce_silver_to_gold(): 

  # TASK (@task 구성, taskgroup(n개 task 그룹화))

  # 의존성(3.X 방향성 지시, TASK의 결과를 새로 넣으면서 진행, 병렬 진행, fan-in/fan-out 구성)
  pass

ecommerce_silver_to_gold()