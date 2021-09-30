import os
from flask import current_app as app
from flask import request
from jina.logging.profile import TimeContext
from jina import Document, DocumentArray
from config import PathConfig


def rolling_update(dump_path):
    """
    :param dump_path: the path you want to roll-update from
    :return: None
    """
    app.search_flow.rolling_update(pod_name='query_searcher', dump_path=dump_path)


def dump_docs(dump_path: str):
    """
    :param dump_path: the path you want to dump the data in
    :return: None
    """
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


def index_docs(docs: list):
    """
    :param docs: list of documents you want to index
    :return: None
    """
    nr_docs = len(docs)
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


def index_documents():
    """
    indexing documents handler
    :return: dict
    """
    req_body = request.get_json(force=True)
    docs = req_body.get('docs')

    dump_path = os.path.join(PathConfig.TEMP_DIR, 'dump_' + str(app.dump_count))
    os.makedirs(dump_path, exist_ok=True)
    try:
        index_docs(docs)
        print('finish indexing')
        dump_docs(dump_path)
        print('finish dumping')
        rolling_update(dump_path)
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
    """
    searching documents handler.
    it returns dictionary that has "matches" key which contains list of documents id
    :return: dict
    """
    req_body = request.get_json(force=True)
    search_query = req_body.get('query')

    results = app.search_flow.search(
        inputs=Document(text=search_query),
        return_results=True
    )

    res = {
        'matches': [match.id for match in results[0].docs[0].matches]
    }
    return res
