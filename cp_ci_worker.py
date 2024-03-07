import requests
import time
import os

def refresh_token():
    # Set up the request data
    data = {
        'grant_type': 'password',                                                   
        'client_id': os.environ['cdp_master_clientid'],
        'client_secret': os.environ['cdp_master_clientsecret'],
        'username': os.environ['cdp_master_userid'],
        'password': os.environ['cdp_master_userpw']
    }

    # Make the request
    response = requests.post('https://login.salesforce.com/services/oauth2/token', data=data)

    # Extract the access token from the response
    access_token = response.json()['access_token']

    # Set up the request headers
    headers = {
        'Authorization': 'Bearer ' + access_token,
        'Accept-Type': 'application/json'
    }

    return headers

headers = refresh_token()
# Make the request
response = requests.get(os.environ['cdp_login_url']+ '/services/data/v57.0/ssot/identity-resolutions', headers=headers)
# Extract the data from the response
data = response.json()

IRstatus = data['identityResolutions'][0]['lastJobStatus']


while 1:
    if IRstatus == 'SUCCESS':
        headers = refresh_token()
        # Make the request
        response = requests.get(os.environ['cdp_login_url']+ '/services/data/v57.0/ssot/calculated-insights?batchSize=200', headers=headers)
        # Extract the data from the response
        data = response.json()['collection']['items']

        for i in data:      
            headers = refresh_token()                                                      
            response = requests.post(os.environ['cdp_login_url']+ '/services/data/v57.0/ssot/calculated-insights/'+i['apiName']+'/actions/run', headers=headers)
            # Extract the data from the response
            data = response.json()
            print(i['apiName'],data)
        break
    else:
        print(IRstatus)
        time.sleep(30)
        headers = refresh_token()
        # Make the request
        response = requests.get(os.environ['cdp_login_url']+ '/services/data/v57.0/ssot/identity-resolutions', headers=headers)
        # Extract the data from the response
        data = response.json()

        IRstatus = data['identityResolutions'][0]['lastJobStatus']