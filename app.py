import os
import time
import traceback
import click
import pandas as pd
from daemon.clients import JinaDClient
from jina.logging.logger import JinaLogger
from jina import __default_host__, Client, Document, DocumentArray

os.environ['JINA_LOG_LEVEL'] = 'DEBUG'
HOST = __default_host__
JINAD_PORT = 8000  # change this if you start jinad on a different port
DUMP_PATH = '/jinad_workspace/dump'  # the path where to dump
SHARDS = 1  # change this if you change pods/query_indexer.yml
DUMP_RELOAD_INTERVAL = 5  # time between dump - rolling update calls
DOCS_PER_ROUND = 5  # nr of documents to index in each round
STORAGE_FLOW_YAML_FILE = 'storage.yml'  # indexing Flow yaml name
QUERY_FLOW_YAML_FILE = 'query.yml'  # querying Flow yaml name
STORAGE_REST_PORT = 9000  # REST port of storage Flow, defined in flows/storage.yml
QUERY_REST_PORT = 9001  # REST port of Query Flow, defined in flows/query.yml

data_dir = os.path.join('data')

logger = JinaLogger('jina')
cur_dir = os.path.dirname(os.path.abspath(__file__))
jinad_client = JinaDClient(host=HOST, port=JINAD_PORT, timeout=-1)


def batches_from_dir(dir_name):
    for filename in os.listdir(dir_name):
        file_path = os.path.join(dir_name, filename)
        ext = os.path.splitext(file_path)[-1].lower()

        if os.path.isfile(file_path) and ext == '.json':
            df = pd.read_json(file_path, orient='records')
            df = df.fillna('')
            records = df.to_dict('records')
            yield DocumentArray([
                Document(id=str(record.get('id')), text=record.get('title_t_bi'), tags=record)
                for record in records
            ])


def index_batch(docs: DocumentArray):
    logger.info(f'Indexing {len(docs)} document(s)...')
    Client(host=HOST, port=STORAGE_REST_PORT, protocol='http').index(inputs=docs)


def query_docs(docs: Document):
    logger.info(f'Searching document {docs}...')
    return Client(host=HOST, port=QUERY_REST_PORT, protocol='http').search(inputs=docs, return_results=True)


def create_flows():
    workspace_id = jinad_client.workspaces.create(paths=[os.path.join(cur_dir, 'flows')])
    jinad_workspace = jinad_client.workspaces.get(workspace_id)['metadata']['workdir']
    logger.info(f'jinad_workspace = {jinad_workspace}')
    logger.info('Creating storage Flow...')
    storage_flow_id = jinad_client.flows.create(
        workspace_id=workspace_id, filename=STORAGE_FLOW_YAML_FILE, envs={'JINAD_WORKSPACE': jinad_workspace}
    )
    logger.info(f'Created successfully. Flow ID: {storage_flow_id}')
    logger.info('Creating Query Flow...')
    query_flow_id = jinad_client.flows.create(
        workspace_id=workspace_id, filename=QUERY_FLOW_YAML_FILE, envs={'JINAD_WORKSPACE': jinad_workspace}
    )
    logger.info(f'Created successfully. Flow ID: {query_flow_id}')
    return storage_flow_id, query_flow_id, workspace_id


def dump_and_roll_update(data_generator, query_flow_id: str):
    for i, batch in enumerate(data_generator):
        logger.info(f'batch {i}:')
        index_batch(batch)
        current_dump_path = os.path.join(DUMP_PATH, str(i))
        logger.info(f'dumping...')
        Client(host=HOST, port=STORAGE_REST_PORT, protocol='http').post(
            on='/dump',
            parameters={'shards': SHARDS, 'dump_path': current_dump_path},
            target_peapod='storage_indexer',
        )

        # JinaD is used for ctrl requests on Flows
        logger.info(f'performing rolling update across replicas...')
        jinad_client.flows.update(
            id=query_flow_id,
            kind='rolling_update',
            pod_name='query_indexer',
            dump_path=current_dump_path,
        )
        logger.info(f'rolling update done. sleeping for {DUMP_RELOAD_INTERVAL}secs...')
        time.sleep(DUMP_RELOAD_INTERVAL)


def query_restful():
    while True:
        text = input('please type a sentence: ')
        if not text:
            break

        query_doc = Document()
        query_doc.text = text
        response = query_docs(query_doc)
        matches = response[0].data.docs[0].matches
        len_matches = len(matches)
        logger.info(f'Ta-Dah🔮, {len_matches} matches we found for: "{text}" :')

        for idx, match in enumerate(matches):
            score = match.scores['euclidean'].value
            if score < 0.0:
                continue
            logger.info(f'> {idx:>2d}({score:.2f}). {match.text}')


def cleanup(storage_flow_id, query_flow_id, workspace_id):
    jinad_client.flows.delete(storage_flow_id)
    jinad_client.flows.delete(query_flow_id)
    jinad_client.workspaces.delete(workspace_id)


@click.command()
@click.option(
    '--task',
    '-t',
    type=click.Choice(['flows', 'client'], case_sensitive=False),
)
def main(task: str):
    """main entrypoint for this system"""
    if task == 'flows':
        # start a Index Flow, dump the data from the Index Flow, and load it into the Query Flow.
        try:
            storage_flow_id, query_flow_id, workspace_id = create_flows()
            logger.info(f'storage_flow_id = {storage_flow_id}')
            logger.info(f'query_flow_id = {query_flow_id}')
            logger.info(f'workspace_id = {workspace_id}')

            # starting a loop that
            # - indexes some data in batches
            # - sends request to storage Flow in JinaD to dump its data to a location
            # - send request to Query Flow in JinaD to perform rolling update across its replicas,
            # which reads the new data in the dump
            data_gen = batches_from_dir(data_dir)
            dump_and_roll_update(data_gen, query_flow_id)
        except (Exception, KeyboardInterrupt) as e:
            if e:
                logger.warning(f'Caught: {e}. Original stacktrace following:')
                logger.error(traceback.format_exc())
            logger.info('Shutting down and cleaning Flows in JinaD...')
            cleanup(storage_flow_id, query_flow_id, workspace_id)

    elif task == 'client':
        query_restful()


main()
