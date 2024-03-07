#config values 사용
import os
#aws S3 연동
import boto3
#timezone 설정
from pytz import timezone
#날짜 생성 및 계산
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time

#접속 정보
session = boto3.Session( 
    aws_access_key_id = os.environ['HDRIVE_S3_ACCESS_KEY'],
    aws_secret_access_key = os.environ['HDRIVE_S3_SECRET_KEY'])
s3 = session.resource('s3')
my_bucket = s3.Bucket(os.environ['HDRIVE_S3_BUCKET'])

#현재 시간 사용하여 해당 시간 기준으로 삭제하는 방법으로 진행
#hdrive UTC+00으로 시간 설정되어 있어서 timezone 설정 필요
now = datetime.now(timezone('Asia/Seoul')).replace(hour=0, minute=0, second=0, microsecond=0)
#삭제할 데이터의 기준일 설정 - 오늘 기준 n달
ref_months = int(os.environ['WORKER_REF_MONTHS'])
#현재 날짜에서 ref_months 만큼의 달을 빼고, YYYY-MM-DD 형태로 변환
ref_date = str(now - relativedelta(months=ref_months))[:10]
ref_date = datetime.strptime(ref_date, '%Y-%m-%d')

for bo in my_bucket.objects.filter(Prefix=''):
    #폴더명 추출을 위해 split하여 tmp 변수에 담음
    tmp = bo.key.split('/')
    #가장 상위 디렉토리에 존재하고, 폴더며, CDP폴더가 아닌 파일 검색
    if bo.key[-1:] == "/" and len(tmp) == 2 and bo.key[:3] != "CDP":
        try:
            folderDate = datetime.strptime(bo.key, '%Y-%m-%d/')
            #기준일보다 과거 날짜의 폴더일 경우 삭제
            if folderDate < ref_date:
                print(bo.key + " - 폴더 삭제")
                my_bucket.objects.filter(Prefix=bo.key).delete()
        #예외일 경우 스킵 (폴더명이 날짜 형태 아닐 경우)
        except:
            continue
