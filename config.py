import os


class AppConfig:
    INDEX_PORT = 5000
    SEARCH_PORT = 5001
    DB_PORT = 27017
    DB_NAME = 'semantic_search'
    COLLECTION_NAME = 'documents'


class PathConfig:
    DATA_DIR = os.path.join('data')
    TEMP_DIR = os.path.join('tmp')
