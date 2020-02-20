from utilities.configuration import ConfigurationMongoDB
from utilities.url import URL
from utilities.exceptions import ExitError
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import ServerSelectionTimeoutError
from bson.objectid import ObjectId
from urllib.parse import quote_plus
from typing import Sequence


class CollectionDoesNotExist(Exception):
    pass


class MongoDB:
    COLLECTION_NAME_CONFIGURATION = 'configuration'

    def __init__(self, configuration: ConfigurationMongoDB):
        self._configuration = configuration
        self._database = None
        self._connected = False

        connection_url = 'mongodb://'

        if '' != configuration.username and '' != configuration.password:
            connection_url += '${0}:${1}@'

        connection_url += configuration.host + ':' + str(configuration.port)

        self._client = MongoClient(
            connection_url.format(quote_plus(configuration.username), quote_plus(configuration.password)),
            serverSelectionTimeoutMS=100
        )

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        try:
            self._client.server_info()
            self._database = self._client.get_database(self._configuration.dbname)
        except ServerSelectionTimeoutError as error:
            raise ExitError('MongoDB connection error: "' + str(error) + '"')

        self._connected = True

    def close(self):
        self._client.close()
        self._connected = False

    def is_connected(self):
        return self._connected

    @staticmethod
    def _init_url(document: dict) -> dict:
        if type(document) is dict and 'url' in document:
            document['url'] = URL(document['url'])

        return document

    def get_database(self) -> Database:
        return self._database

    def get_collection(self, collection_name: str, auto_create: bool = True) -> Collection:
        if not auto_create and not self.has_collection(collection_name):
            raise CollectionDoesNotExist

        return self._database.get_collection(collection_name)

    def has_collection(self, collection_name: str) -> bool:
        return collection_name in self._database.collection_names()

    def insert_documents(self, collection_name: str, data: Sequence[dict], auto_create: bool = True):
        self.get_collection(collection_name, auto_create).insert_many(data)

    def insert_document(self, collection_name: str, data: dict, auto_create: bool = True):
        self.get_collection(collection_name, auto_create).insert_one(data)

    def update_one(self, collection_name: str, document_id: ObjectId, update_data: dict):
        self.get_collection(collection_name, False).find_one_and_update({'_id': document_id}, {'$set': update_data})

    def find(self, collection_name: str, filter_parameter: dict, raw: bool = False):
        result = self.get_collection(collection_name, False).find(filter_parameter)

        if raw is True:
            return result

        return [self._init_url(document) for document in result]

    def find_one(self, collection_name: str, filter_parameter: dict, raw: bool = False):
        result = self.get_collection(collection_name, False).find_one(filter_parameter)

        if raw is True:
            return result

        return self._init_url(result)

    def find_last_sorted(self, collection_name: str, filter_parameter: dict, sort: list):
        result = self.get_collection(collection_name, False).find(filter=filter_parameter, limit=1, sort=sort)

        return [self._init_url(document) for document in result]
