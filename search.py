import sys
import requests

API_URL = 'http://localhost:8000/search'


def search_docs(q):
    r = requests.post(API_URL, json={"query": q})
    return r


query = sys.argv[1]
results = search_docs(query)

print(results.json())
