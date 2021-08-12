from database.connection import Connection
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError
from service.api.rankalyst import Client as RankalysApiClient
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from datetime import datetime, timedelta
from time import time
from typing import Sequence


class _DataAlreadyExistError(Exception):
    pass


class RankalystKeywordRanking:
    COLLECTION_NAME = 'rankalyst_keyword_ranking'

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration = configuration
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)
        self.connection = connection
        self.mongodb = None
        self.bigquery = None

    def run(self):
        print('Running Rankalyst Keyword Ranking Module:')
        timer_run = time()

        if 'bigquery' == self.module_configuration.database:
            self.bigquery = self.connection.bigquery
        else:
            self.mongodb = self.connection.mongodb

        if 'configurations' in self.module_configuration.settings and \
            type(self.module_configuration.settings['configurations']) is list:
            for configuration in self.module_configuration.settings['configurations']:
                try:
                    self._process_configuration(configuration, self.module_configuration.database)
                    print(' - OK')
                except _DataAlreadyExistError:
                    print(' - EXISTS')

        print('\ncompleted: {:s}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def _process_configuration(self, configuration: dict, database: str):
        if 'apiKey' in configuration and type(configuration['apiKey']) is str:
            api_key = configuration['apiKey']
        else:
            raise ConfigurationMissingError('Missing api key for configuration')

        if 'username' in configuration and type(configuration['username']) is str:
            username = configuration['username']
        else:
            raise ConfigurationMissingError('Missing username for configuration')

        if 'projectId' in configuration and type(configuration['projectId']) is str:
            project_id = int(configuration['projectId'])
        else:
            raise ConfigurationMissingError('Missing project id for configuration')

        table_reference = None

        if 'bigquery' == database:
            if 'tablename' in configuration and type(configuration['tablename']) is str:
                table_name = configuration['tablename']
            else:
                raise ConfigurationMissingError('Missing tablename for pagespeed to bigquery')

            dataset_name = None

            if 'dataset' in configuration and type(configuration['dataset']) is str:
                dataset_name = configuration['dataset']

            table_reference = self.connection.bigquery.table_reference(table_name, dataset_name)

        print('Project: {:d}'.format(project_id), end='')

        if 'bigquery' == database and self._bigquery_check_has_existing_data(project_id, table_reference):
            raise _DataAlreadyExistError()
        elif 'mongodb' == database and self._mongodb_check_has_existing_data(project_id):
            raise _DataAlreadyExistError()

        client = RankalysApiClient(api_key, username)

        rankings = self._process_keyword_ranking(client, project_id)

        if 'bigquery' == database:
            self._process_response_for_bigquery(table_reference, rankings)
        else:
            self._process_responses_for_mongodb(rankings)

    @staticmethod
    def _process_keyword_ranking(client: RankalysApiClient, project_id):
        rankings = []

        for scalar_type_id, scalar_type_label in RankalysApiClient.PARAMETER_LABEL_SCALAR_TYPE.items():
            response = client.request(RankalysApiClient.ACTION_PROJECT_KEYWORD_RANKING, {
                'project_id': project_id,
                'scrape_type_id': scalar_type_id
            })

            for item in response['data']['items']:
                ranking = None
                prev_rank = None

                if type(item['ranking']) is int or (
                    type(item['prev_rank']) is str and item['prev_rank'].isnumeric()
                ):
                    prev_rank = int(item['prev_rank'])

                if type(item['ranking']) is int or (
                    type(item['ranking']) is str and item['ranking'].isnumeric()
                ):
                    ranking = int(item['ranking'])

                try:
                    item_date = datetime.strptime(item['date'], '%Y-%m-%d %H:%M:%S')
                except TypeError:
                    item_date = datetime.utcnow()

                rankings.append({
                    'project_id': int(project_id),
                    'prev_rank': prev_rank,
                    'keyword': str(item['keyword']),
                    'keyword_group': item['keyword_group'] if item['keyword_group'] is not None else None,
                    'ranking': ranking,
                    'ranking_change': int(item['ranking_change']),
                    'url': str(item['url']),
                    'date': item_date,
                    'rank': str(item['rank']),
                    'scalar_type': scalar_type_label,
                })

        return rankings

    def _process_responses_for_mongodb(self, responses: Sequence[dict]):
        self.mongodb.insert_documents(RankalystKeywordRanking.COLLECTION_NAME, responses)

    def _process_response_for_bigquery(self, table_reference: TableReference, rankings: Sequence[dict]):
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')

        job_config.schema = (
            SchemaField('date', SqlTypeNames.DATETIME, 'REQUIRED'),
            SchemaField('url', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('project_id', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('prev_rank', SqlTypeNames.INTEGER),
            SchemaField('keyword', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('keyword_group', SqlTypeNames.STRING, 'REPEATED'),
            SchemaField('ranking', SqlTypeNames.INTEGER),
            SchemaField('ranking_change', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('rank', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('scalar_type', SqlTypeNames.STRING, 'REQUIRED'),
        )

        for ranking in rankings:
            ranking['date'] = ranking['date'].strftime('%Y-%m-%dT%H:%M:%S.%f')

        load_job = self.bigquery.client.load_table_from_json(rankings, table_reference, job_config=job_config)
        load_job.result()

    def _bigquery_check_has_existing_data(self, project_id: int, table_reference: TableReference) -> bool:
        if not self.bigquery.has_table(table_reference.table_id, table_reference.dataset_id):
            return False

        query_job = self.bigquery.query(
            'SELECT COUNT(*) FROM `{dataset}.{table}` ' \
            'WHERE CAST(`date` as DATE) = "{date:%Y-%m-%d}" ' \
            'AND project_id = {project_id:d}'.format(
                dataset=table_reference.dataset_id,
                table=table_reference.table_id,
                date=datetime.now(),
                project_id=project_id
            )
        )

        count = 0

        for row in query_job.result():
            count = row[0]

        return 0 < count

    def _mongodb_check_has_existing_data(self, project_id: int) -> bool:
        if not self.mongodb.has_collection(RankalystKeywordRanking.COLLECTION_NAME):
            return False

        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)

        return 0 < self.mongodb.find(
            RankalystKeywordRanking.COLLECTION_NAME,
            {
                'project_id': project_id,
                'date': {
                    '$gte': today,
                    '$lt': tomorrow,
                }
            },
            cursor=True
        ).count()
