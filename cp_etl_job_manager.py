import requests
import psycopg2
import os

class Databases():
    def __init__(self):
        self.db = psycopg2.connect(host=os.environ['db_host'], dbname=os.environ['db_name'],user=os.environ['db_user'],password=os.environ['db_password'],port=os.environ['db_port'])
        self.cursor = self.db.cursor()

    def __del__(self):
        self.db.close()
        self.cursor.close()

    def execute(self,query,args={}):
        self.cursor.execute(query,args)
        row = self.cursor.fetchall()
        return row

    def commit(self):
        self.cursor.commit()

class DML(Databases):
    def query(self,query):
        try:
            self.cursor.execute(query)
            self.db.commit()
        except Exception as e :
            print(e) 

headers = {'Accept': 'application/vnd.xplenty+json; version=2'}
auth = (os.environ['XPLENTY_API_KEY'], '') 
account_id = os.environ['XPLENTY_ACCOUNT_ID']
sql = "SELECT pg_terminate_backend(pid) FROM pg_locks l,pg_stat_all_tables t WHERE l.relation = t.relid AND relname NOT LIKE 'pg_%' ORDER BY relation ASC;"
if __name__ == "__main__":
    url = f'https://api.xplenty.com/{account_id}/api/jobs'
    params = {'status': 'running'}

    response = requests.get(url, headers=headers, auth=auth, params=params)
    db_dml = DML()
    # 응답 처리
    if response.status_code == 200:
        # 요청이 성공적으로 완료됨
        data = response.json()
        for i in data:
            if i['runtime_in_seconds'] > 18000:
                print(i)
                url = f'https://api.xplenty.com/{account_id}/api/jobs/{i["id"]}'
                response = requests.delete(url, headers=headers, auth=auth)
                db_dml.query(sql)
        # JSON 데이터로 변환된 응답
        # 필요에 따라 데이터를 처리하거나 출력합니다.
    else:
        # 요청이 실패했거나 오류가 발생함
        print('요청 실패:', response.status_code)
        # 실패한 경우 오류 코드를 출력합니다.