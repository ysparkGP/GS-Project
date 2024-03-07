import requests
import json
import psycopg2
import os

#DB 정보
host = os.environ['db_host']
dbname = os.environ['db_name']
dbuser = os.environ['db_user']
dbpw = os.environ['db_password']

#API 정보 : 주소기반산업지원서비스
def getRes(keyword):
    try:
        URL = 'https://business.juso.go.kr/addrlink/addrLinkApi.do'
        #검색한 첫번째 결과만 JSON 형태로 가져오도록 설정
        data = {'confmKey': os.environ['juso_api_key'], 'currentPage': '1', 'countPerPage':'1','keyword':keyword,'resultType':'json'}
        res = requests.post(URL, data=data)
        return res
    except:
        return None

class Databases():
    def __init__(self):
        #deploy 전에 db 접속 정보 heroku 변수로 바꿔야함
        #추후 변경 예정
        self.db = psycopg2.connect(host=host, dbname=dbname, user=dbuser, password=dbpw, port=5432)
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
    #insert
    def insertDB(self,schema,table,colum,data,ext):
        sql = " INSERT INTO \"{schema}\".\"{table}\" ({colum}) VALUES ({data}) {ext} ;".format(schema=schema,table=table,colum=colum,data=data,ext=ext)
        try:
            self.cursor.execute(sql)
            self.db.commit()
        except Exception as e :
            print(" insert DB err ",e) 
    
    #select
    def readDB(self,schema,table,colum,condition):
        sql = " SELECT DISTINCT {colum} from \"{schema}\".\"{table}\" {condition}".format(colum=colum,schema=schema,table=table,condition=condition)
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


if __name__ == "__main__":
    db = DML()

    #주소 정제를 진행할 L1 테이블, 컬럼 입력 (현재는 자이, 분양관리, MGM, 카카오챗봇 채널에서 가져와서 검색)
    srcTBList = ['TB_L1_XW_MEM_M', 'TB_L1_DI_CUST_INFO_M','TB_L1_MG_MGM_I','TB_L1_XW_CHAT_MEM_M']
    srcCLlist = ['trim(replace(replace(CONCAT("DO_ADDR",\' \',"GU_ADDR",\' \',"DONG_ADDR",\' \',"SCEN_DTL_ADDR"),\'  \',\' \'),\'  \',\' \')) as "ADDR_VAL"','"L2"."REFINE_ADDR"(CONCAT("FRST_REG_ADDR",\' \', "SCEN_REG_ADDR"))','"ADDR"','"MBR_ADDR"']
    for m, n in zip(srcTBList, srcCLlist):
        #테이블의 원본주소 저장된 컬럼명의 데이터 추출
        for i in db.readDB(schema='L1',table=m,colum=n,condition=''):
            #네덩이 -> 세덩이 순으로 순차 검증
            chk = False
            for cnt in [4, 3]:
                #원본주소를 뛰어쓰기 기준으로 잘라서 사용 (풀주소 검색은 검색이 안되는 경우가 많음)
                orgaddr = i[0].split(' ')[:cnt]
                #나눠진 덩어리를 다시 붙여서 검색에 사용
                orgaddr = " ".join(orgaddr)
                if len(orgaddr) > 1 and chk == False:
                    #검색 전 API 과부하 방지 및 밴 가능성을 염두하여 이미 검색해본 데이터인지 검색 (검색 내역을 public.addresshistory에 저장)
                    for j in db.readDB(schema='public',table='addresshistory',colum='count(orgadd)',condition="WHERE orgadd = '"+ i[0].replace("'","''") +"' and (city is not null or town is not null or district is not null or zipcode is not null)"):
                        #검색 결과가 없을 경우
                        if j[0] == 0:
                            res = getRes(orgaddr)
                            try:
                                #REQUEST 결과 헤더가 200이고, results가 1개 이상일 경우
                                if res.status_code == 200 and int(json.loads(res.text)['results']['common']['totalCount']) > 0:
                                    #데이터를 추출하여 우편번호, 시, 도, 군을 추출
                                    data = json.loads(res.text)['results']['juso'][0]
                                    print("입력받은주소 : " + orgaddr)
                                    print("우편번호 : " + data['zipNo'])
                                    print("시 : " + data['siNm'])
                                    print("도 : " + data['sggNm'])
                                    print("군 : " + data['emdNm'])
                                    #DB에 저장
                                    db.insertDB(schema='public',table='addresshistory',colum='orgadd,city,district,town,zipcode,regdate',data='\''+i[0].replace("'","''")+'\',\''+data['siNm']+'\',\''+data['sggNm']+'\',\''+data['emdNm']+'\',\''+data['zipNo']+'\',NOW() AT TIME ZONE \'Asia/Seoul\'',ext='ON CONFLICT (orgadd) DO UPDATE SET city=\''+data['siNm']+'\', town=\''+data['sggNm']+'\', district=\''+data['emdNm']+'\', zipcode=\''+data['zipNo']+'\', regdate=NOW() AT TIME ZONE \'Asia/Seoul\'')
                                    chk = True
                            except:
                                print("REQUEST 오류 발생")
                        #이미 존재하는 주소일 경우
                        else:
                            print("이미 존재하는 주소 : " + i[0])
                            chk = True