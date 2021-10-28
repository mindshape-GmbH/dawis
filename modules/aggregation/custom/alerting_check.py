from database.connection import Connection, BigQuery, MongoDB
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError, ConfigurationInvalidError
from utilities.parsing import parse_comparison
from service.alerting import Alert, AlertQueue
from service.bigquery import QueryHelper
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import LoadJobConfig, TimePartitioning, TimePartitioningType, TableReference, SchemaField
from google.cloud.bigquery.client import Client as BigQueryClient
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.job import WriteDisposition
from google.cloud.bigquery.schema import SchemaField
from google.oauth2 import service_account
from datetime import datetime, timedelta
from os.path import abspath
from time import time
from typing import Sequence


class AlertingCheck:
    ROW_LIMIT = 25000

    _mongodb: MongoDB
    _bigquery: BigQuery
    _alert_queue: AlertQueue

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration: Configuration = configuration
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)
        self.connection: Connection = connection
        self._mongodb = self.connection.mongodb
        self._alert_queue = AlertQueue(self._mongodb)

    def run(self):
        print('Running Alerting Check Module:')
        timer_run = time()

        if 'bigquery' == self.module_configuration.database:
            self._bigquery = self.connection.bigquery
        else:
            raise ConfigurationInvalidError('This module only works with BigQuery')

        if 'configurations' in self.module_configuration.settings and \
            type(self.module_configuration.settings['configurations']) is list:
            for configuration in self.module_configuration.settings['configurations']:
                self._process_configuration(configuration)
                print(' - OK')

        print('\ncompleted: {:s}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def _process_configuration(self, configuration: dict):
        if 'type' in configuration and type(configuration['type']) is str:
            check_type = configuration['type']
        else:
            raise ConfigurationMissingError('Missing type for alert check configuration')

        if 'query' == check_type:
            self._process_query_configuration(configuration)
        else:
            raise ConfigurationMissingError('The alert check type "{}" is not valid'.format(check_type))

    def _process_query_configuration(self, configuration: dict):
        parameters = {}
        negate = False
        checks_per_line = []
        log_configuration = None

        if 'groups' in configuration and type(configuration['groups']) is list:
            groups = configuration['groups']
        else:
            raise ConfigurationMissingError('Missing groups for alert check configuration')

        if 'query' in configuration and type(configuration['query']) is str:
            query = configuration['query']
        else:
            raise ConfigurationMissingError('Missing query for alert check configuration')

        if 'message' in configuration and type(configuration['message']) is str:
            message = configuration['message']
        else:
            raise ConfigurationMissingError('Missing message for alert check configuration')

        if 'negate' in configuration and type(configuration['negate']) is bool:
            negate = configuration['negate']

        if 'parameters' in configuration and type(configuration['parameters']) is dict:
            parameters = configuration['parameters']

        if 'checksPerLine' in configuration and type(configuration['checksPerLine']) is list:
            checks_per_line = configuration['checksPerLine']

        if 'log' in configuration and type(configuration['log']) is dict:
            log_configuration = configuration['log']

            if 'name' not in log_configuration:
                raise ConfigurationMissingError('Missing "name" in log configuration')

            if 'message' not in log_configuration:
                log_configuration['message'] = message

            if 'table' not in log_configuration:
                raise ConfigurationMissingError('Missing "table" in log configuration')

            if 'dataset' not in log_configuration:
                log_configuration['dataset'] = None

        bigquery_client = self._client_from_configuration(configuration)

        query_helper = QueryHelper(self._bigquery, bigquery_client)
        query_helper.run_query(
            query,
            parameters,
            process_result_function=self._process_query_result,
            additional_parameters={
                'groups': groups,
                'negate': negate,
                'message': message,
                'checks_per_line': checks_per_line,
                'log_configuration': log_configuration
            }
        )

    def _client_from_configuration(self, configuration: dict) -> BigQueryClient:
        if 'project' in configuration and type(configuration['project']) is str:
            credentials = None

            if 'credentials' in configuration:
                credentials = service_account.Credentials.from_service_account_file(
                    abspath(configuration['credentials'])
                )

            return BigQueryClient(configuration['project'], credentials)

        return self._bigquery.client

    def _log_to_bigquery(self, log_configuration: dict, log_items: list):
        table_reference = self._bigquery.table_reference(log_configuration['table'], log_configuration['dataset'])

        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')
        job_config.schema = (
            SchemaField('date', SqlTypeNames.DATETIME, 'REQUIRED'),
            SchemaField('name', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('message', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('success', SqlTypeNames.BOOLEAN, 'REQUIRED'),
        )

        for log_item in log_items:
            log_item['date'] = log_item['date'].strftime('%Y-%m-%dT%H:%M:%S.%f')

        try:
            load_job = self._bigquery.client.load_table_from_json(log_items, table_reference, job_config=job_config)
            load_job.result()
        except BadRequest as error:
            print(error.errors)

    def _process_query_result(
        self,
        result_data: Sequence[dict],
        result_schema: Sequence[SchemaField],
        groups: Sequence[str],
        negate: bool,
        message: str,
        checks_per_line: list,
        log_configuration: dict
    ):
        alerts = []
        log_items = []

        if type(checks_per_line) is list and 0 < len(checks_per_line):
            if 0 == len(result_data):
                log_items.append({
                    'date': datetime.utcnow(),
                    'name': log_configuration['name'],
                    'message': '',
                    'success': True,
                })

            for result_item in result_data:
                result_check = True

                for check_per_line in checks_per_line:
                    if parse_comparison(check_per_line, result_item):
                        result_check = False
                        break

                if type(log_configuration) is dict:
                    log_items.append({
                        'date': datetime.utcnow(),
                        'name': log_configuration['name'],
                        'message': log_configuration['message'].format(**result_item),
                        'success': result_check,
                    })

                alert_message = message.format(**result_item)

                if result_check is False and negate is False or result_check is True and negate is False:
                    [
                        alerts.append(Alert(datetime.utcnow(), group, alert_message, result_item))
                        for group in groups
                    ]
        else:
            alert_data = {
                'results': result_data,
                'resultsCount': len(result_data)
            }

            alert_message = message.format(**alert_data)

            if 0 < len(result_data) and negate is False:
                [alerts.append(Alert(datetime.utcnow(), group, alert_message, alert_data)) for group in groups]
                log_items.append({
                    'date': datetime.utcnow(),
                    'name': log_configuration['name'],
                    'message': log_configuration['message'].format(**alert_data),
                    'success': False,
                })
            elif 0 == len(result_data) and negate is True:
                [alerts.append(Alert(datetime.utcnow(), group, message)) for group in groups]
                log_items.append({
                    'date': datetime.utcnow(),
                    'name': log_configuration['name'],
                    'message': log_configuration['message'],
                    'success': False,
                })
            else:
                log_items.append({
                    'date': datetime.utcnow(),
                    'name': log_configuration['name'],
                    'message': log_configuration['message'],
                    'success': True,
                })

        self._log_to_bigquery(log_configuration, log_items)
        self._alert_queue.add_alerts(alerts)
