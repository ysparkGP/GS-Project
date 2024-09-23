import pandas as pd
import boto3
import io
from sqlalchemy import create_engine
import time
import datetime
from glob import glob
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import os

start = time.time()

try:

    s3_client = boto3.client(service_name="s3",
                            aws_access_key_id=os.environ['HDRIVE_S3_ACCESS_KEY'],
                            aws_secret_access_key=os.environ['HDRIVE_S3_SECRET_KEY'])

    post_db = os.environ['db_name']
    post_user = os.environ['db_user']
    post_pass = os.environ['db_password']
    post_host = os.environ['db_host']
    post_port = os.environ['db_port']
    post_engine = create_engine(f'postgresql://{post_user}:{post_pass}@{post_host}:{post_port}/{post_db}', pool_size=40, max_overflow=55)

    Session = sessionmaker(bind=post_engine)
    session = Session()
    query = text(f'TRUNCATE TABLE public.\"UnifiedssotIndividualRule__dlm\"');
    
    print(query)
    session.execute(query)
    session.commit()

    obj_list = s3_client.list_objects(Bucket='gs-cp-s3', Prefix='CDP')
    unified_csv_location = ''
    for content in obj_list['Contents']:
            if content['Key'].find('part') > 0: 
                # print(content['Key'], content['Key'].find('part'))
                unified_csv_location = content['Key']
    d_type = {
        'DS_MBR_SEQ': str,
        'VI_MBR_SEQ': str,
        'MBR_HP_NO': str  
    }

    obj = s3_client.get_object(Bucket="gs-cp-s3", Key=unified_csv_location)
    # obj = s3_client.get_object(Bucket="gs-cp-s3", Key=""CDP/Salesforce-c360-Segments/2024/02/06/21/S3 EXPORT_S3 CSV EXPORT_2024020621000585/part-00000-8ca82fd6-e052-42fb-8ccc-a2804bdb157a-c000.csv"")
    df = pd.read_csv(io.BytesIO(obj["Body"].read()), quotechar = '"', escapechar= '\\', dtype=d_type)
    df.to_sql(name='UnifiedssotIndividualRule__dlm', con=post_engine, schema='public', if_exists='append', chunksize=1000, index=False, method='multi')



except Exception as e:
    print(e)

finally:
    end = time.time()
    sec = (end - start)
    result = datetime.timedelta(seconds=sec)
    print(result)   

