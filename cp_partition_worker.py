import psycopg2
from pytz import timezone
from dateutil.relativedelta import relativedelta
from datetime import datetime, date, timedelta
import os

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

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

class DDL(Databases):
    #테이블 생성
    def createTB(self,schema,table,startDate,endDate):
        table_date = startDate.replace(",","")
        sql = " CREATE TABLE IF NOT EXISTS \"{schema}\".\"{table}{table_date}\" PARTITION OF \"{schema}\".\"{table}\" FOR VALUES FROM ({startDate}) TO ({endDate});".format(schema=schema,table=table,startDate=startDate,endDate=endDate,table_date=table_date)
        try:
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            self.db.rollback()
            print(" create TB err ",e) 

    #테이블 드랍
    def dropTB(self,schema,table,date):
        date = date.replace(",","")
        sql = " DROP TABLE IF EXISTS \"{schema}\".\"{table}{date}\";".format(schema=schema,table=table,date=date)
        try :
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print( "drop TB err", e)

class DML(Databases):
    #insert
    def insertDB(self,schema,table,colum,data):
        sql = " INSERT INTO \"{schema}\".\"{table}\" ({colum}) VALUES ({data}) ;".format(schema=schema,table=table,colum=colum,data=data)
        try:
            print(sql)
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(" insert DB err ",e) 
    
    #select
    def readDB(self,schema,table,colum,condition):
        sql = " SELECT {colum} from \"{schema}\".\"{table}\" {condition}".format(colum=colum,schema=schema,table=table,condition=condition)
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
        except Exception as e :
            result = (" read DB err",e)
        return result

    #update
    def updateDB(self,schema,table,colum,value,condition):
        sql = " UPDATE \"{schema}\".\"{table}\" SET \"{colum}\"='{value}' where {condition} ".format(schema=schema
        , table=table , colum=colum ,value=value,condition=condition )
        try :
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(" update DB err",e)

    #delete
    def deleteDB(self,schema,table,condition):
        sql = " delete from {schema}.{table} where {condition} ; ".format(schema=schema,table=table,
        condition=condition)
        try :
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e:
            print( "delete DB err", e)
            
if __name__ == "__main__":
    #클래스 호출
    db_ddl = DDL()
    db_dml = DML()
    
    #오늘 날짜
    today = datetime.now(timezone('Asia/Seoul'))

    #과거 1주 (PO 끝나고 수정해야함)
    minus_one_week = today - relativedelta(weeks=1)

    #미래 1년
    plus_one_years = today + relativedelta(days=7)

    #존재하는 테이블 조회
    #작업 전, 테이블 존재 여부 확인을 위해서 Y값인 테이블을 TMP로 임시 변경
    db_dml.updateDB(schema="L1", table="TB_L1_PART_TBL_MNG_B", colum="USE_YN", value="TMP", condition="\"USE_YN\"='Y'")
    for i in db_dml.readDB(schema='pg_catalog',table='pg_tables',colum='schemaname,tablename',condition="where (schemaname in ('L0') and tablename in (SELECT c.relname FROM pg_class AS c WHERE NOT EXISTS (SELECT 1 FROM pg_inherits AS i WHERE i.inhrelid = c.oid) AND c.relkind IN ('r', 'p')) or tablename IN ('TB_L2_UNIFIED_PROFILE','TB_L2_UNIFIED_LINK_PROFILE','marketing_customer_log'))"):
        chk = db_dml.readDB(schema="L1",table="TB_L1_PART_TBL_MNG_B",colum="count(\"TBL_NM\")",condition="where \"SCHEMA_NM\" = '" + i[0] + "' and \"TBL_NM\"='" + i[1] + "'")[0][0]
        if chk == 0:
            #DB에 없는 테이블 생겼을 경우 자동으로 컬럼 생성
            db_dml.insertDB('L1', 'TB_L1_PART_TBL_MNG_B', '"SCHEMA_NM", "TBL_NM", "PART_CYCLE_VAL", "USE_YN", "REG_DATE"', "'"+i[0]+"','"+i[1]+"','D','Y',now() AT TIME ZONE 'Asia/Seoul'")
        else:
            #이미 있는 컬럼에 대해서는 Y로 값 다시 변경
            db_dml.updateDB(schema="L1", table="TB_L1_PART_TBL_MNG_B", colum="USE_YN", value="Y", condition="\"USE_YN\"='TMP' AND \"SCHEMA_NM\" = '"+ i[0] +"' AND \"TBL_NM\" = '"+ i[1] +"'")
    #위 반복문에서 검사 안된 컬럼은 N으로 변경 (없는 컬럼이란 뜻)
    db_dml.updateDB(schema="L1", table="TB_L1_PART_TBL_MNG_B", colum="USE_YN", value="N", condition="\"USE_YN\"='TMP'")
    for i in db_dml.readDB(schema='L1',table='TB_L1_PART_TBL_MNG_B',colum='"SCHEMA_NM","TBL_NM"',condition="where \"USE_YN\" = 'Y'"):
        #1주 지난 데이터 삭제
        #스크립트 오류로 안돌아가는 날 발생을 대비하여 1~7일 지난 데이터 삭제 반복
        for k in range(1,30):
            delete_date = minus_one_week - relativedelta(days=k)
            delete_date_str = str(delete_date)[:10].replace("-",",")
            db_ddl.dropTB(i[0], i[1], delete_date_str)
        #과거1주 ~ 미래1년 반복 
        for single_date in daterange(minus_one_week,plus_one_years):
            #일 간격으로 날짜 잘라서 추출 (추후 일,주,월,년 단위로 조건걸어야함)
            tomorrow = single_date + relativedelta(days=1)
            today_str = str(single_date)[:10].replace("-",",")
            tomorrow_str = str(tomorrow)[:10].replace("-",",")
            #테이블 생성
            db_ddl.createTB(i[0], i[1], today_str, tomorrow_str)