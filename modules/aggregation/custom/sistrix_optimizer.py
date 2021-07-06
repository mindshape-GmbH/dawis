from database.bigquery import BigQuery
from database.connection import Connection
from service.api.sistrix import Client as SistrixApiClient, ApiError as SistrixApiError
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError, ConfigurationInvalidError
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from datetime import datetime, timedelta
from pytz import timezone
from time import time
from typing import Sequence
import re


class _DataNotAvailableYet(Exception):
    pass


class SistrixOptimizer:
    COLLECTION_NAME = 'sistrix_optimizer'
    API_FORMAT = 'json'

    DEFAULT_API_RANKING_LIMIT = 1000000

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration = configuration
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)
        self.connection = connection
        self.mongodb = None
        self.bigquery = None

    def run(self):
        print('Running Sistrix Optimizer Module:')
        timer_run = time()

        if 'configurations' in self.module_configuration.settings and \
            type(self.module_configuration.settings['configurations']) is list:
            for configuration in self.module_configuration.settings['configurations']:
                self._process_configuration(configuration, self.module_configuration.database)

        print('\ncompleted: {}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def _process_configuration(self, configuration: dict, database: str):
        parameters = {}
        dataset = None
        table_reference = None

        if 'apiKey' in configuration and type(configuration['apiKey']) is str:
            api_key = configuration['apiKey']
        else:
            raise ConfigurationMissingError('Missing API Key for configuration')

        if 'projects' in configuration and type(configuration['projects']) is list:
            projects = configuration['projects']
        else:
            raise ConfigurationMissingError('Missing project for configuration')

        if 'method' in configuration and type(configuration['method']) is str:
            method = configuration['method']

            if not method.startswith('optimizer.'):
                method = 'optimizer.' + configuration['method']

            if SistrixApiClient.ENDPOINT_OPTIMIZER_VISIBILITY == method:
                method = SistrixApiClient.ENDPOINT_OPTIMIZER_VISIBILITY
                schema = (
                    SchemaField('request_date', SqlTypeNames.DATETIME, 'REQUIRED'),
                    SchemaField('date', SqlTypeNames.DATETIME, 'REQUIRED'),
                    SchemaField('source', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('type', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('value', SqlTypeNames.FLOAT, 'REQUIRED'),
                )
            elif SistrixApiClient.ENDPOINT_OPTIMIZER_RANKING == method:
                method = SistrixApiClient.ENDPOINT_OPTIMIZER_RANKING
                schema = (
                    SchemaField('request_date', SqlTypeNames.DATETIME, 'REQUIRED'),
                    SchemaField('keyword', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('position', SqlTypeNames.INTEGER, 'REQUIRED'),
                    SchemaField('positionOverflow', SqlTypeNames.BOOL, 'REQUIRED'),
                    SchemaField('url', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('tags', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('device', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('country', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('traffic', SqlTypeNames.INTEGER, 'REQUIRED'),
                    SchemaField('searchengine', SqlTypeNames.STRING, 'REQUIRED'),
                )
            else:
                raise ConfigurationInvalidError('Invalid method "{}" in configuration'.format(configuration['method']))
        else:
            raise ConfigurationMissingError('Missing method for configuration')

        if 'parameters' in configuration and type(configuration['parameters']) is dict:
            parameters = configuration['parameters']

        if 'dataset' in configuration and type(configuration['dataset']) is str:
            dataset = configuration['dataset']

        if 'bigquery' == database:
            if type(self.bigquery) is not BigQuery:
                self.bigquery = self.connection.bigquery

            if 'table' in configuration and type(configuration['table']) is str:
                table_reference = self.bigquery.table_reference(configuration['table'], dataset)
            else:
                raise ConfigurationMissingError('You have to set at least a table if you want to use bigquery')

        api_client = SistrixApiClient(api_key)

        responses = []
        request_date = datetime.utcnow().replace(tzinfo=timezone('UTC'))

        request = {
            'date': request_date.astimezone(timezone('Europe/Berlin')),
            **parameters
        }

        for project in projects:
            request['project'] = project

            try:
                if SistrixApiClient.ENDPOINT_OPTIMIZER_VISIBILITY == method:
                    responses.extend(
                        self._process_visibility_response(
                            api_client.request(method, request),
                            request_date
                        )
                    )
                elif SistrixApiClient.ENDPOINT_OPTIMIZER_RANKING == method:
                    if 'limit' not in request:
                        request['limit'] = self.DEFAULT_API_RANKING_LIMIT

                    responses.extend(
                        self._process_ranking_response(
                            api_client.request(method, request),
                            request_date
                        )
                    )
            except SistrixApiError as error:
                print('API Error: ' + error.message)

        if 'bigquery' == self.module_configuration.database:
            self._process_responses_for_bigquery(responses, schema, table_reference)
        else:
            self._process_responses_for_mongodb(responses)

    @staticmethod
    def _process_visibility_response(response: dict, request_date: datetime) -> list:
        data = []

        for response_data in response['answer'][0]['optimizer.visibility']:
            source = None
            source_type = None

            if 'domain' in response_data:
                source = response_data['domain']
                source_type = 'domain'
            if 'path' in response_data:
                source = response_data['path']
                source_type = 'path'
            if 'host' in response_data:
                source = response_data['host']
                source_type = 'host'
            if 'url' in response_data:
                source = response_data['url']
                source_type = 'url'

            if type(source) is not str:
                raise SistrixApiError('Missing source for response data')

            data.append({
                'request_date': request_date,
                'date': datetime.fromisoformat(response_data['date']).astimezone(timezone('UTC')),
                'source': source,
                'type': source_type,
                'value': float(response_data['value']),
            })

        return data

    def _process_ranking_response(self, response: dict, request_date: datetime) -> list:
        data = []

        for response_data in response['answer'][0]['optimizer.rankings']:
            for ranking in response_data['optimizer.ranking']:
                if ranking['position'].isnumeric():
                    position = int(ranking['position'])
                    position_overflow = False
                else:
                    match = re.search(r'(\d+)$', ranking['position'])

                    if type(match) is re.Match:
                        position = int(match.group(1))
                        position_overflow = True
                    else:
                        raise SistrixApiError('Invalid position data from api: "{}"'.format(ranking['position']))

                data.append({
                    'request_date': request_date,
                    'keyword': ranking['keyword'],
                    'position': position,
                    'positionOverflow': position_overflow,
                    'url': ranking['url'],
                    'tags': ranking['tags'],
                    'device': ranking['device'],
                    'country': ranking['country'],
                    'traffic': int(ranking['traffic']),
                    'searchengine': ranking['searchengine'],
                })

        return data

    def _process_responses_for_bigquery(
        self,
        responses: Sequence[dict],
        schema: Sequence[SchemaField],
        table_reference: TableReference
    ):
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='request_date')
        job_config.schema = schema

        for response in responses:
            if 'request_date' in response:
                response['request_date'] = response['request_date'].strftime('%Y-%m-%dT%H:%M:%S.%f')
            if 'date' in response:
                response['date'] = response['date'].strftime('%Y-%m-%dT%H:%M:%S.%f')

        load_job = self.bigquery.client.load_table_from_json(responses, table_reference, job_config=job_config)
        load_job.result()

    def _process_responses_for_mongodb(self, responses: Sequence[dict]):
        self.mongodb.insert_documents(SistrixOptimizer.COLLECTION_NAME, responses)
