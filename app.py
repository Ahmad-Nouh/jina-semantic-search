import os
import sys
from flask import Flask, request
from jina import Flow
from jinahub.indexers.searcher.AnnoySearcher.annoy_searcher import AnnoySearcher
from jinahub.indexers.storage.MongoDBStorage.mongo_storage import MongoDBStorage

sys.path.append(os.path.abspath('./'))
from executors import DocumentEncoder
from handlers import index_documents, search_documents
from config import AppConfig, PathConfig

app = Flask(__name__)

# index flow
app.index_flow = Flow(name='index_flow', port_expose=AppConfig.INDEX_PORT, cors=True) \
    .add(name='storage_encoder',
         uses=DocumentEncoder) \
    .add(name='storage_indexer',
         uses=MongoDBStorage,
         uses_with={
             'port': AppConfig.DB_PORT,
             'database': AppConfig.DB_NAME,
             'collection': AppConfig.COLLECTION_NAME
         })

# search flow
app.search_flow = Flow(name='search_flow', port_expose=AppConfig.SEARCH_PORT, cors=True) \
    .add(name='query_encoder', uses=DocumentEncoder) \
    .add(name='query_searcher',
         uses=AnnoySearcher,
         replicas=2)

# global variable to count the number of dump files that we've been creating
app.dump_count = 0


@app.route('/index', methods=['POST'])
def index():
    """
    index handler
    :return: dict
    """
    if request.method == 'POST':
        return index_documents()
    else:
        return {'message': 'method not allowed'}, 405


@app.route('/search', methods=['POST'])
def search():
    """
    search handler
    :return: dict
    """
    if request.method == 'POST':
        return search_documents()
    else:
        return {'message': 'method not allowed'}, 405


if __name__ == '__main__':
    os.makedirs(PathConfig.TEMP_DIR, exist_ok=True)
    app.index_flow.start()
    app.search_flow.start()
    app.run(debug=True, port=8000)
