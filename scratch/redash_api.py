# Get data from redash API
# I copied heavily from this example: https://gist.github.com/arikfr/e3e434d8cfd7f331d499ccf351abbff9
import os
import requests
import time

api_key = 'KFj4lLJnrFDMxdNU0b0yTC2BnuMXNT1U1apLO7y9'
redash_url = 'https://sql.telemetry.mozilla.org'
query_id = '61387'
params = {'p_param': 1234}

s = requests.Session()

s.headers.update({'Authorization': 'Key {}'.format(api_key)})

response = s.post('{}/api/queries/{}/refresh'.format(redash_url, query_id), params=params)

job = response.json()['job']

while job['status'] not in (3,4):
    response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
    job = response.json()['job']
    time.sleep(1)

result_id = job['query_result_id']
response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))

print(response.json()['query_result']['data']['rows'])
