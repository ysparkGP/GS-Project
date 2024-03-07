# list connection에서 ID ,TYPE 추출해서 배열로 test connection 돌리는 스크립트 (1시간 마다 실행)
import requests
import smtplib
from email.mime.text import MIMEText
import os

def send_email(subject, body):
    sender_email = os.environ['sender_email']
    sender_password = os.environ['sender_password']
    receiver_email = os.environ['receiver_email']

    message = MIMEText(body)
    message['Subject'] = subject
    message['From'] = sender_email
    message['To'] = receiver_email

    try:
        # gmail에선 587번 포트 허용하는 서버를 SMTP 서버 열어줌
        # 보안 수준이 낮은 앱 허용하고 보안 2단계의 앱 비밀번호 사용해야함
        server = smtplib.SMTP('smtp.gmail.com', 587)
        # server.set_debuglevel(True)
        server.ehlo()
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_email, message.as_string())
        server.quit()
        print('이메일 전송 완료')

    except Exception as e:
        print('이메일 전송 오류')
        print(str(e))

def connection_check():
    headers = {'Accept': 'application/vnd.xplenty+json; version=2'}
    auth = (os.environ['XPLENTY_API_KEY'], '') # xPlenty에서 관리하는 api key 동환 매니저님이 로컬에서 텍스트파일로 관리 중
    account_id = os.environ['XPLENTY_ACCOUNT_ID']

    url = f'https://api.xplenty.com/{account_id}/api/connections'

    response = requests.get(url, headers=headers,auth=auth)

    # 응답 처리
    if response.status_code == 200:
        # 요청이 성공적으로 완료됨
        data = response.json()
        for i in data:
            if i['name'] == 'GSENC': continue

            url = f'https://api.xplenty.com/{account_id}/api/connections/{i["type"]}/{i["id"]}/test'
            response = requests.post(url, headers=headers, auth=auth)
            
            data = response.json()
            # connection 성공 시 True 반환
            # connection 실패 시 {'error_message': 'The Connection attempt timed out.'} 반환
            if response.json() != True:
                # 메시지 전송
                send_email('Heroku Connection 실패 건', f'{i["name"]} Connection이 끊어졌습니다.')

    else:
        print(f'{response.status_code} 로 인한 에러')
        # 요청이 실패했거나 오류가 발생함

if __name__ == '__main__':
    connection_check()