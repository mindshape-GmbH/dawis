from database.orm.tables import Tables
from utilities.configuration import Configuration
from utilities.configuration import ConfigurationBigQuery
from utilities.configuration import ConfigurationBigQueryDataset
from utilities.exceptions import Error
from utilities.exceptions import ConfigurationMissingError
from utilities.exceptions import TableDoesNotExistError
from google.cloud import bigquery
from google.cloud.bigquery import Client
from google.cloud.bigquery.dataset import Dataset
from google.cloud.bigquery.dataset import DatasetReference
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table, TableReference
from google.cloud.bigquery.job import QueryJob
from google.cloud.exceptions import BadRequest
from google.oauth2 import service_account
from datetime import datetime
from typing import Sequence


class QueryError(Exception):
    def __init__(self, errors: list, query):
        self.errors = errors
        self.query = query


class DatasetDoesNotExistError(Error):
    def __init__(self, message: str):
        self.message = message


class BigQuery:
    def __init__(self, configuration: Configuration):
        if type(configuration.databases.bigquery) is not ConfigurationBigQuery:
            raise ConfigurationMissingError('No bigquery connection configured')

        self._configuration = configuration
        self._client = None
        self._dataset = None
        self._additional_datasets = {}
        self._connected = False
        self._insert_batch = {}

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        if self._connected is True:
            return

        credentials = None

        if self._configuration.databases.bigquery.credentials is not None:
            credentials = service_account.Credentials.from_service_account_file(
                self._configuration.databases.bigquery.credentials
            )

        self._client = bigquery.Client(self._configuration.databases.bigquery.project, credentials)

        if not self._has_dataset(self._configuration.databases.bigquery.dataset):
            self._dataset = self._create_dataset(self._configuration.databases.bigquery.dataset)
        else:
            self._dataset = self._get_dataset(self._configuration.databases.bigquery.dataset)

        for key, additional_dataset in self._configuration.databases.bigquery.additional_datasets.items():
            if not self._has_dataset(additional_dataset):
                self._additional_datasets[additional_dataset.name] = self._create_dataset(additional_dataset)
            else:
                self._additional_datasets[additional_dataset.name] = self._get_dataset(additional_dataset)

        for urlset in self._configuration.urlsets.urlsets:
            self.init_check_table(urlset.name)

        self._connected = True

    def close(self):
        self._client.close()
        self._connected = False

    def is_connected(self):
        return self._connected

    @property
    def client(self) -> Client:
        return self._client

    def table_reference(self, table_name: str, dataset_name: str = None) -> TableReference:
        if dataset_name is not None:
            if dataset_name not in self._configuration.databases.bigquery.additional_datasets:
                raise DatasetDoesNotExistError('The dataset "' + dataset_name + '" does not exist')

            dataset = self._configuration.databases.bigquery.additional_datasets[dataset_name]
        else:
            dataset = self._dataset

        return DatasetReference(
            self._configuration.databases.bigquery.project,
            dataset.name
        ).table(table_name)

    def has_table(self, table_name, dataset_name=None) -> bool:
        has_table = False

        if dataset_name is not None:
            if dataset_name not in self._additional_datasets:
                raise DatasetDoesNotExistError('The dataset "' + dataset_name + '" does not exist')

            dataset = self._additional_datasets[dataset_name]
        else:
            dataset = self._dataset

        for table_listitem in self._client.list_tables(dataset):
            if table_listitem.table_id == table_name:
                has_table = True
                break

        return has_table

    def _has_dataset(self, dataset_configuration: ConfigurationBigQueryDataset) -> bool:
        has_dataset = False

        for dataset_listitem in self._client.list_datasets(dataset_configuration.project):
            if dataset_listitem.dataset_id == dataset_configuration.name:
                has_dataset = True
                break

        return has_dataset

    def _get_dataset(self, dataset_configuration: ConfigurationBigQueryDataset) -> Dataset:
        if not self._has_dataset(dataset_configuration):
            raise TableDoesNotExistError('The dataset "' + dataset_configuration.name + '" does not exist')

        return self._client.get_dataset(dataset_configuration.name)

    def _create_dataset(self, dataset_configuration: ConfigurationBigQueryDataset) -> Dataset:
        dataset = bigquery.Dataset(dataset_configuration.project + '.' + dataset_configuration.name)
        dataset.location = dataset_configuration.location
        dataset.description = dataset_configuration.description
        dataset.labels = dataset_configuration.labels

        return self._client.create_dataset(dataset)

    def _has_table(self, table_name) -> bool:
        has_table = False

        for table_listitem in self._client.list_tables(self._dataset):
            if table_listitem.table_id == table_name:
                has_table = True
                break

        return has_table

    def _get_table(self, table_name, dataset_name: str = None) -> Table:
        if dataset_name is not None:
            if dataset_name not in self._additional_datasets:
                raise DatasetDoesNotExistError('The dataset "' + dataset_name + '" does not exist')

            dataset = self._additional_datasets[dataset_name]
        else:
            dataset = self._dataset

        if not self._has_table(table_name):
            raise TableDoesNotExistError('The table "' + table_name + '" does not exist')

        table_id = dataset.project + '.' + dataset.dataset_id + '.' + table_name

        return self._client.get_table(table_id)

    def _create_table(self, name: str, schema: Sequence[SchemaField]) -> Table:
        return self._client.create_table(bigquery.Table(name, schema))

    def init_check_table(self, urlset: str) -> Table:
        table_name = Tables.checks_tablename(urlset)
        table_id = self._dataset.project + '.' + self._dataset.dataset_id + '.' + table_name

        if self._has_table(table_name):
            return self._get_table(table_name)

        return self._create_table(
            table_id,
            [
                bigquery.SchemaField('created', 'DATETIME', 'REQUIRED'),
                bigquery.SchemaField('check', 'STRING', 'REQUIRED'),
                bigquery.SchemaField('diff', 'STRING'),
                bigquery.SchemaField('error', 'STRING'),
                bigquery.SchemaField('value', 'STRING'),
                bigquery.SchemaField('valid', 'BOOL', 'REQUIRED'),
                bigquery.SchemaField('url', 'RECORD', 'REQUIRED', fields=[
                    bigquery.SchemaField('protocol', 'STRING', 'REQUIRED'),
                    bigquery.SchemaField('domain', 'STRING', 'REQUIRED'),
                    bigquery.SchemaField('path', 'STRING', 'REQUIRED'),
                    bigquery.SchemaField('query', 'STRING'),
                ]),
            ]
        )

    def _insert_data_batch(self, table_name: str, data: dict):
        if table_name not in self._insert_batch:
            self._insert_batch[table_name] = []

        self._insert_batch[table_name].append(data)

    def commit(self):
        for table_name, data in self._insert_batch.items():
            self._client.insert_rows(self._get_table(table_name), data)

    def add_check(
            self,
            urlset: str,
            check: str,
            value: str,
            valid: bool,
            diff: str,
            error: str,
            url_protocol: str,
            url_domain: str,
            url_path: str,
            url_query: str
    ):
        self._insert_data_batch(
            Tables.checks_tablename(urlset),
            {
                'created': datetime.utcnow(),
                'check': check,
                'diff': diff,
                'error': error,
                'value': value,
                'valid': valid,
                'url': {
                    'protocol': url_protocol,
                    'domain': url_domain,
                    'path': url_path,
                    'query': url_query,
                },
            }
        )

    def query(self, query: str) -> QueryJob:
        query_job = self._client.query(query)

        if type(query_job.errors) is list and 0 < len(query_job.errors):
            raise QueryError(query_job.errors, query)

        try:
            rows = query_job.result()
        except BadRequest as error:
            raise QueryError(error.errors, query)

        return query_job
