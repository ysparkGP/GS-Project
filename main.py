from fastapi import FastAPI
from database import *

app = FastAPI()
db = database()

@app.get("/subscriberKey/phoneNumber/{phoneNumber}")
async def subscrPN(phoneNumber: str):
    sql = "SELECT \"SUBSCR_KEY\" from \"L2\".\"TB_L2_MC_SUBSCR\" where length(\"MBR_HP_NO\") > 0 AND (\"MBR_HP_NO\" = '" + phoneNumber + "' OR \"MBR_HP_NO_164\" = '" + phoneNumber + "')"
    db.cursor.execute(sql)
    result = db.cursor.fetchall()
    if len(result) > 0:
        item = {"phoneNumber": phoneNumber, "SubscriberKey": str(result[0][0])}
        return item
    else:
        item = {"phoneNumber": phoneNumber, "SubscriberKey": "false"}
        return item

@app.get("/subscriberKey/email/{email}")
async def subscrEM(email: str):
    sql = "SELECT \"SUBSCR_KEY\" from \"L2\".\"TB_L2_MC_SUBSCR\" where length(\"MBR_EMAIL\") > 0 AND \"MBR_EMAIL\" = '" + email + "'"
    db.cursor.execute(sql)
    result = db.cursor.fetchall()
    if len(result) > 0:
        item = {"email": email, "SubscriberKey": str(result[0][0])}
        return item
    else:
        item = {"email": email, "SubscriberKey": "false"}
        return item

