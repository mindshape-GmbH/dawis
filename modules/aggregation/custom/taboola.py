from database.connection import Connection, BigQuery, MongoDB
from service.api import TaboolaApiClient
from utilities import datetime
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from datetime import date, timedelta
from datetime import datetime
from time import time
from typing import Sequence


class _DataAlreadyExistError(Exception):
    pass


class Taboola:
    COLLECTION_NAME = 'taboola'

    _mongodb: MongoDB
    _bigquery: BigQuery

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration: Configuration = configuration
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)

        if 'bigquery' == self.module_configuration.database:
            self._bigquery = connection.bigquery
        else:
            self._mongodb = connection.mongodb

    def run(self):
        print('Running Taboola Module:')
        timer_run = time()

        if 'configurations' in self.module_configuration.settings and \
            type(self.module_configuration.settings['configurations']) is list:
            for configuration in self.module_configuration.settings['configurations']:
                self._process_configuration(configuration)

        print('\ncompleted: {:s}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def _process_configuration(self, configuration: dict):
        request_days_ago = 1
        timer_base = time()
        dataset = None
        table_reference = None

        if 'clientId' in configuration and type(configuration['clientId']) is str:
            client_id = configuration['clientId']
        else:
            raise ConfigurationMissingError('Missing Client ID for configuration')

        if 'clientSecret' in configuration and type(configuration['clientSecret']) is str:
            client_secret = configuration['clientSecret']
        else:
            raise ConfigurationMissingError('Missing Client Secret for configuration')

        if 'accountId' in configuration and type(configuration['accountId']) is str:
            account_id = configuration['accountId']
        else:
            raise ConfigurationMissingError('Missing Account ID for configuration')

        if 'dateDaysAgo' in configuration and type(configuration['dateDaysAgo']) is int:
            request_days_ago = configuration['dateDaysAgo']

        request_date = datetime.now() - timedelta(days=request_days_ago)

        print(' - Account ID: "{:s}"'.format(account_id))
        print('   + {:%Y-%m-%d}'.format(request_date), end='')

        if 'bigquery' == self.module_configuration.database:
            if 'dataset' in configuration and type(configuration['dataset']) is str:
                dataset = configuration['dataset']

            if 'tablename' in configuration and type(configuration['tablename']) is str:
                table_reference = self._bigquery.table_reference(configuration['tablename'], dataset)
            else:
                raise ConfigurationMissingError('You have to set at least a table if you want to use bigquery')

            if self._bigquery_check_has_existing_data(table_reference, request_date, account_id):
                print(' - EXISTS')
                return

        api_client = TaboolaApiClient(client_id, client_secret)
        response = api_client.request(
            'GET',
            TaboolaApiClient.ENDPOINT_REPORTING_SUMMARY,
            {'account_id': account_id, 'dimension': 'campaign_day_breakdown'},
            {'start_date': request_date.date(), 'end_date': request_date.date()}
        )

        results = self._process_response(response, account_id)

        if 'bigquery' == self.module_configuration.database:
            self._process_results_for_bigquery(table_reference, results)
        else:
            self._process_results_for_mongodb(results)

        print(' - OK - {:s}'.format(str(timedelta(seconds=int(time() - timer_base)))))

    @staticmethod
    def _process_response(response: dict, account_id: str) -> list:
        results = []

        if 'results' in response and type(response['results']) is list:
            for result in response['results']:
                result['date'] = datetime.strptime(result['date'], '%Y-%m-%d %H:%M:%S.%f').date()
                result['accountId'] = account_id
                results.append(result)

        return results

    def _process_results_for_mongodb(self, results: Sequence[dict]):
        for result in results:
            result['date'] = result['date'].strftime('%Y-%m-%d')

        self._mongodb.insert_documents(self.COLLECTION_NAME, results)

    def _process_results_for_bigquery(
        self,
        table_reference: TableReference,
        results: list
    ):
        schema = (
            SchemaField('accountId', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('date', SqlTypeNames.DATE, 'REQUIRED'),
            SchemaField('campaign_name', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('campaign', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('clicks', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('impressions', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('visible_impressions', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('spent', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('conversions_value', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('roas', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('ctr', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('vctr', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('cpm', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('vcpm', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('cpc', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('campaigns_num', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('cpa', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('cpa_clicks', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('cpa_views', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('cpa_actions_num', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('cpa_actions_num_from_clicks', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('cpa_actions_num_from_views', SqlTypeNames.INTEGER, 'REQUIRED'),
            SchemaField('cpa_conversion_rate', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('cpa_conversion_rate_clicks', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('cpa_conversion_rate_views', SqlTypeNames.FLOAT, 'REQUIRED'),
            SchemaField('currency', SqlTypeNames.STRING, 'REQUIRED'),
        )

        for result in results:
            result['date'] = result['date'].strftime('%Y-%m-%d')

        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')
        job_config.schema = schema

        load_job = self._bigquery.client.load_table_from_json(results, table_reference, job_config=job_config)
        load_job.result()

    def _bigquery_check_has_existing_data(
        self,
        table_reference: TableReference,
        request_date: date,
        account_id: str
    ) -> bool:
        if not self._bigquery.has_table(table_reference.table_id, table_reference.dataset_id):
            return False

        query_job = self._bigquery.query(
            'SELECT COUNT(*) FROM `{dataset}.{table}` '
            'WHERE date = "{date:%Y-%m-%d}" '
            'AND accountId = "{accountId}"'.format(
                dataset=table_reference.dataset_id,
                table=table_reference.table_id,
                date=request_date,
                accountId=account_id
            )
        )

        count = 0

        for row in query_job.result():
            count = row[0]

        return 0 < count

    def _mongodb_check_has_existing_data(
        self,
        request_date: date,
        account_id: str
    ) -> bool:
        if not self._mongodb.has_collection(self.COLLECTION_NAME):
            return False

        return 0 < self._mongodb.find(
            self.COLLECTION_NAME,
            {
                'accountId': account_id,
                'date': datetime.combine(request_date, datetime.min.time())
            },
            cursor=True
        ).count()
