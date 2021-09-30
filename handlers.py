import os
from flask import current_app as app
from flask import request
from jina.logging.profile import TimeContext
from jina import Document, DocumentArray
from config import PathConfig


def rolling_update(nr_docs, dump_path):
    with TimeContext(f'### rolling update {nr_docs} docs'):
        app.search_flow.rolling_update(pod_name='query_searcher', dump_path=dump_path)


def dump_docs(nr_docs, dump_path):
    with TimeContext(f'### dumping {nr_docs} docs'):
        dump_results = app.index_flow.post(
            on='/dump',
            target_peapod='storage_indexer',
            parameters={
                'dump_path': dump_path,
                'shards': 1,
                'timeout': -1,
            },
            return_results=True
        )
        print(f'dump_results = {dump_results}')


def index_docs(docs, nr_docs):
    with TimeContext(f'### indexing {nr_docs} docs'):
        index_batch = DocumentArray([
            Document(id=doc['id'], text=doc['title_t_bi'], tags=doc)
            for doc in docs
        ])

        index_results = app.index_flow.post(
            on='/index',
            inputs=index_batch,
            return_results=True
        )
        print(f'index_results = {index_results}')


def index_documents():
    req_body = request.get_json(force=True)
    docs = req_body.get('docs')
    nr_docs = len(docs)

    dump_path = os.path.join(PathConfig.TEMP_DIR, 'dump_' + str(app.dump_count))
    os.makedirs(dump_path, exist_ok=True)
    try:
        index_docs(docs, nr_docs)
        print('finish indexing')
        dump_docs(nr_docs, dump_path)
        print('finish dumping')
        rolling_update(nr_docs, dump_path)
    except Exception as e:
        return {
            'error': str(e)
        }
    else:
        app.dump_count += 1
        return {
            'message': "indexed successfully"
        }


def search_documents():
    req_body = request.get_json(force=True)
    search_query = req_body.get('query')

    results = app.search_flow.search(
        inputs=Document(text=search_query),
        return_results=True
    )
    print(f'matches = {[match for match in results[0].docs[0].matches]}')

    res = {
        'matches': [match.id for match in results[0].docs[0].matches]
    }
    return res
