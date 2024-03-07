import jaydebeapi
from sqlalchemy import create_engine, MetaData
import os
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
import time

db_name = os.environ['db_name']
db_user = os.environ['db_user']
db_password = os.environ['db_password']
db_host = os.environ['db_host']
db_port = os.environ['db_port']
target_schema = "public"

cdp_list = [ 
    "UnifiedLinkssotIndividualRule__dlm","MC_Subscriber_Key__dll"
]

def queryCDP(table):
    curs = cdpConnection.cursor()
    query = get_query(table)
    max_retries = 10  # 최대 재시도 횟수 설정
    retries = 0

    while retries < max_retries:
        try:
            curs.execute(query)
            data = curs.fetchall()
            return data
        except Exception as e:
            retries += 1
            print(f'Error executing query: {str(e)}')
            print(f'Retrying... ({retries}/{max_retries})')
    
    print(f'Exceeded maximum number of retries ({max_retries}). Aborting...')
    return None


def get_query(table):
    query = None
    if table == 'MC_Subscriber_Key__dll':
        query = f"SELECT subscriberkey__c, mbr_hp_no__c, formattede164phonenumber__c, emailaddress__c FROM {table}"
    elif table == 'UnifiedLinkssotIndividualRule__dlm':
        query = f"SELECT CreatedDate__c, SourceRecordId__c, ssot__DataSourceId__c, ssot__DataSourceObjectId__c, ssot__InternalOrganizationId__c, UnifiedRecordId__c FROM {table}"
    return query

def insert(table, data):
    newData = []
    for row in data:
        newRow = ()
        for value in row:
            if value == None:
                value = ''
            newRow += (value,)
        newData.append(newRow)

    col = get_col(table)
    if len(col):
        df = pd.DataFrame(newData, columns=col)
        df.to_sql(table, db, schema=target_schema, index=False, if_exists='append', method='multi', chunksize=100)
        print(f'{table} insert 완료')
    else:
        print(f"col is empty")

def get_col(table):
    col = []
    if table == 'MC_Subscriber_Key__dll':
        col = ['subscriberkey__c', 'mbr_hp_no__c', 'formattede164phonenumber__c', 'emailaddress__c']
    elif table == 'UnifiedLinkssotIndividualRule__dlm':
        col = ['CreatedDate__c', 'SourceRecordId__c', 'ssot__DataSourceId__c', 'ssot__DataSourceObjectId__c', 'ssot__InternalOrganizationId__c', 'UnifiedRecordId__c']

    return col

def connectPostgre():
    db = create_engine(
            sqlalchemy.engine.url.URL.create(
                drivername='postgresql+psycopg2',
                username=db_user,
                password=db_password,
                host=db_host,
                port=db_port,
                database=db_name,
            )
        )
    return db 


def start():
    print("start")
    global cdpConnection
    properties = {
        'user': os.environ['cdp_master_userid'],
        'password': os.environ['cdp_master_userpw'],
        'clientId': os.environ['cdp_master_clientid'],
        'clientSecret': os.environ['cdp_master_clientsecret']
    }
    #jar파일 다운로드는 여기서 : https://github.com/forcedotcom/Salesforce-CDP-jdbc/releases
    jar_path = os.path.join("Salesforce-CDP-jdbc-1.19.0.jar")
    cdpConnection = jaydebeapi.connect("com.salesforce.cdp.queryservice.QueryServiceDriver", "jdbc:queryService-jdbc:https://login.salesforce.com", properties, jar_path)
    
    try:
        global db
        db = connectPostgre()
        with db.connect() as conn:
            print(f'DB is connected!!!')
            meta = MetaData(schema='{schema_name}'.format(schema_name=target_schema))
            global Base
            Base = declarative_base(metadata=meta)
            Base.metadata.create_all(db)
            
            global session
            Session = sessionmaker(bind=db)
            session = Session()
            # Truncate 먼저
            for table in cdp_list:
                try:
                    print(table)
                    query = text(f"TRUNCATE TABLE \"{target_schema}\".\"{table}\"")
                    print(query)
                    session.execute(query)
                    session.commit()
                except:
                    print("테이블 없음")
            
            time.sleep(5)
            
            for table in cdp_list:
                data = queryCDP(table)
                insert(table, data)
    except Exception as e:
        print(f'Error: {str(e)}')

if __name__ == '__main__':
    start_time = datetime.now()
    start()
    end_time = datetime.now()
    sec = (end_time - start_time)
    elapsed_time = sec
    print(elapsed_time)