import os
import requests
import time

def load_amo(api_key, redash_api, query_id, sqlContext):

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

    data_dict = response.json()['query_result']['data']['rows']
    df = sqlContext.createDataFrame(data=data_dict)

    return df
