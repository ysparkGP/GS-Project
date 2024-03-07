import pandas as pd
import boto3
import io
from sqlalchemy import create_engine
import time
import datetime
from glob import glob
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import smtplib
from email.mime.text import MIMEText
import os 

def send_email(sender, receiver, subject, content, password):
    # 이메일 생성
    msg = MIMEText(content)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver

    # 이메일 서버에 연결
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(sender, password)

    # 이메일 보내기
    server.sendmail(sender, receiver, msg.as_string())
    server.quit()

def main():

    start = time.time()

    try:
        post_db = os.environ['db_name']
        post_user = os.environ['db_user']
        post_pass = os.environ['db_password']
        post_host = os.environ['db_host']
        post_port = os.environ['db_port']
        post_engine = create_engine(f'postgresql://{post_user}:{post_pass}@{post_host}:{post_port}/{post_db}', pool_size=40, max_overflow=55)

        Session = sessionmaker(bind=post_engine)
        session = Session()
        query = text(f'select query from pg_stat_activity where 1=1 and application_name = \'PostgreSQL JDBC Driver\' and query like \'%BI%\' and now() - interval \'1 hours\' >= query_start order by query_start desc');
        
        print(query)
        result = session.execute(query).all()
        # session.commit()
        print(result)
        if len(result) > 0:
            email_pass = os.environ['EMAIL_PASS']
            send_email('qkrdbtjd016@gmail.com','yspark@goldenplanet.co.kr','Integrate.IO BI 마트 쿼리 건',f'{result} 확인 요망',email_pass)


    except Exception as e:
        print(e)

    finally:
        end = time.time()
        sec = (end - start)
        result = datetime.timedelta(seconds=sec)
        print(result)

main()