import os
import requests
import pandas as pd

API_URL = 'http://localhost:8000/index'
data_dir = os.path.join('data')


def index_docs(docs):
    r = requests.post(API_URL, json={"docs": docs})
    return r


for filename in os.listdir(data_dir):
    print(f'indexing {filename} data..')
    file_path = os.path.join(data_dir, filename)
    df = pd.read_json(file_path, orient='records')
    df = df.fillna('')
    records = df.to_dict('records')

    titles = [record for record in records]
    resp = index_docs(titles)
    if resp.status_code != 200:
        print(f'indexing {filename} fails ❌')
        print(resp.json())
    else:
        print(f'finish indexing {filename} ✅')
