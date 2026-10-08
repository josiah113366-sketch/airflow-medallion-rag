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

# 전역 변수(환경 변수)

# 공용/공통 등 함수 

# DAG (@dag)

  # TASK (@task 구성, taskgroup(n개 task 그룹화))

# 의존성(3.X 방향성 지시, TASK의 결과를 새로 넣으면서 진행, 병렬 진행, fan-in/fan-out 구성)