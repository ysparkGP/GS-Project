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

#prefix에 해당되는 디렉토리의 리스트 검색
prefix = 'CDP'
#현재 시간 사용하여 해당 시간 기준으로 삭제하는 방법으로 진행
#hdrive UTC+00으로 시간 설정되어 있어서 timezone 설정 필요
now = str(datetime.now(timezone('Asia/Seoul')).replace(hour=0, minute=0, second=0, microsecond=0))[:10]
#날짜 이름대로 폴더 생성
my_bucket.put_object(Key=(now+'/'))

#폴더 복사
#기존폴더
src_info = "CDP/"
#복사폴더
dst_info = now + "/"
for obj in my_bucket.objects.filter(Prefix=src_info):
	old_source = {'Bucket': os.environ['HDRIVE_S3_BUCKET'], 'Key': obj.key}
	new_key = obj.key.replace(src_info, dst_info, 1)
	print(f"Copy {obj.key} -> {new_key}")
	new_obj = my_bucket.Object(new_key)
	new_obj.copy(old_source)
	
#이후 폴더 삭제 후 재생성
my_bucket.objects.filter(Prefix="CDP/").delete()
my_bucket.put_object(Key=('CDP/'))