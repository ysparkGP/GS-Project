import psycopg2

class database():
    def __init__(self):
        self.db = psycopg2.connect(host='ec2-3-115-109-119.ap-northeast-1.compute.amazonaws.com', dbname='d3jmtl5deevfhi',user='u44ckbv4qnpmqa',password='pd166dc4e2b855d35fcee577e79c92a6070ed018220a5877442755bba62a82249',port=5432)
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
    