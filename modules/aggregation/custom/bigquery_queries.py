from database.connection import Connection
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError, ConfigurationInvalidError
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.query import ScalarQueryParameter
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from google.oauth2 import service_account
from datetime import date, datetime, timedelta
from os.path import abspath
from time import time
from typing import Sequence


class BigqueryQueries:
    ROW_LIMIT = 25000
    ALLOWED_WRITE_DISPOSITION = [WriteDisposition.WRITE_APPEND, WriteDisposition.WRITE_TRUNCATE, 'append', 'truncate']
    ALLOWED_TIME_PARTITION_TYPE = [
        TimePartitioningType.HOUR,
        TimePartitioningType.DAY,
        TimePartitioningType.MONTH,
        TimePartitioningType.YEAR
    ]

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration = configuration
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)

        if not connection.has_bigquery():
            raise ConfigurationMissingError('Missing bigquery configuration which is necessary for this module')

        self.bigquery = connection.bigquery
        self._load_jobs = []

    def run(self):
        print('Running BigQuery query module:')
        timer_run = time()

        if 'configurations' in self.module_configuration.settings and \
                type(self.module_configuration.settings['configurations']) is list:
            for configuration in self.module_configuration.settings['configurations']:
                self._process_configuration(configuration)

        print('\ncompleted: {:s}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def _process_configuration(self, configuration: dict):
        column_mapping = None
        result_table = None
        time_partitioning = None
        write_disposition = WriteDisposition.WRITE_APPEND
        parameters = []

        if 'project' in configuration and type(configuration['project']) is str:
            credentials = None

            if 'credentials' in configuration:
                credentials = service_account.Credentials.from_service_account_file(
                    abspath(configuration['credentials'])
                )

            bigquery_client = Client(configuration['project'], credentials)
        else:
            bigquery_client = self.bigquery.client

        if 'query' in configuration and type(configuration['query']) is str:
            query = configuration['query']
        else:
            raise ConfigurationMissingError('Missing query for configuration')

        if 'parameters' in configuration and type(configuration['parameters']) is dict:
            parameters = self._process_parameters(configuration['parameters'])

        if 'result' in configuration and type(configuration['result']) is dict:
            result_configuration = configuration['result']
            dataset_name = None

            if 'columnMapping' in result_configuration and type(result_configuration['columnMapping']) is dict:
                column_mapping = result_configuration['columnMapping']

            if 'tablename' in result_configuration and type(result_configuration['tablename']) is str:
                table_name = result_configuration['tablename']
            else:
                raise ConfigurationMissingError('Missing result tablename for query configuration')

            if 'dataset' in result_configuration and type(result_configuration['dataset']) is str:
                dataset_name = result_configuration['dataset']

            if 'writeDisposition' in result_configuration and type(result_configuration['writeDisposition']) is str:
                write_disposition = result_configuration['writeDisposition']

                if write_disposition not in BigqueryQueries.ALLOWED_WRITE_DISPOSITION:
                    raise ConfigurationInvalidError('Invalid write disposition type "' + write_disposition + '"')
                elif 'append' == write_disposition.lower():
                    write_disposition = WriteDisposition.WRITE_APPEND
                elif 'truncate' == write_disposition.lower():
                    write_disposition = WriteDisposition.WRITE_TRUNCATE

            if 'timePartitioning' in result_configuration and type(result_configuration['timePartitioning']) is dict:
                time_partitioning_configuration = result_configuration['timePartitioning']
                time_partitioning_type = None

                if 'field' in time_partitioning_configuration and type(time_partitioning_configuration['field']) is str:
                    time_partitioning_field = time_partitioning_configuration['field']
                else:
                    raise ConfigurationMissingError('missing field for time partitioning')

                if 'type' in time_partitioning_configuration and type(time_partitioning_configuration['type']) is str:
                    time_partitioning_type = time_partitioning_configuration['type'].upper()

                    if time_partitioning_type not in BigqueryQueries.ALLOWED_TIME_PARTITION_TYPE:
                        raise ConfigurationInvalidError(
                            'Invalid time partitioning type "' + time_partitioning_type + '"'
                        )

                time_partitioning = TimePartitioning(field=time_partitioning_field, type_=time_partitioning_type)

            result_table = self.bigquery.table_reference(table_name, dataset_name)

        self._process_query(
            bigquery_client,
            query,
            parameters,
            column_mapping,
            result_table,
            write_disposition,
            time_partitioning
        )

    def _process_query(
            self,
            bigquery_client,
            query: str,
            parameters: Sequence[ScalarQueryParameter],
            column_mapping: dict,
            result_table: TableReference,
            write_disposition: WriteDisposition.WRITE_APPEND,
            time_partitioning: TimePartitioning = None
    ):
        query_job = self.bigquery.query(query, parameters, bigquery_client)
        row_iterator = query_job.result(page_size=BigqueryQueries.ROW_LIMIT)
        result_schema = self._process_result_schema(row_iterator.schema, column_mapping)
        result_data = []

        for page in row_iterator.pages:
            for row in page:
                result_item = {}

                for column, value in row.items():
                    result_item[column] = value

                result_data.append(
                    self._process_result_row(row, column_mapping)
                )

            self._process_results_for_bigquery(
                result_data,
                result_schema,
                result_table,
                write_disposition,
                time_partitioning
            )

        for load_job in self._load_jobs:
            load_job.result()

    @staticmethod
    def _process_parameters(parameters: dict) -> Sequence[ScalarQueryParameter]:
        processed_parameters = []

        for key, parameter in parameters.items():
            if type(parameter) is str:
                processed_parameters.append(ScalarQueryParameter(key, SqlTypeNames.STRING, parameter))
            elif type(parameter) is int:
                processed_parameters.append(ScalarQueryParameter(key, SqlTypeNames.INTEGER, parameter))
            elif type(parameter) is float:
                processed_parameters.append(ScalarQueryParameter(key, SqlTypeNames.FLOAT, parameter))
            elif type(parameter) is dict:
                processed_parameter = None
                parameter_function, parameter_options = list(parameter.keys())[0], list(parameter.values())[0]

                if 'dateDaysAgo' == parameter_function and type(parameter_options) is int:
                    processed_parameter = ScalarQueryParameter(
                        key,
                        SqlTypeNames.DATE,
                        date.today() - timedelta(days=parameter_options)
                    )

                if type(processed_parameter) is not ScalarQueryParameter:
                    raise ConfigurationInvalidError('Invalid dynamic parameter, could not be properly processed')

                processed_parameters.append(processed_parameter)

        return processed_parameters

    def _process_result_row(self, row: dict, column_mapping: dict) -> dict:
        processed_row = {}

        for column, value in row.items():
            if type(value) is dict:
                processed_row[column] = self._process_result_row(
                    row[column],
                    {
                        old_column.replace(column + '.', ''): new_column
                        for old_column, new_column in column_mapping.items()
                        if old_column.startswith(column + '.')
                    }
                )
            elif type(value) is list:
                processed_row[column] = [
                    self._process_result_row(
                        sub_row,
                        {
                            old_column.replace(column + '.', ''): new_column
                            for old_column, new_column in column_mapping.items()
                            if old_column.startswith(column + '.')
                        }
                    )
                    for sub_row in value
                ]
            elif type(value) is date:
                processed_row[column] = row[column].strftime('%Y-%m-%d')
            elif type(value) is datetime:
                processed_row[column] = row[column].strftime('%Y-%m-%dT%H:%M:%S.%f')
            else:
                processed_row[column] = row[column]

            if column in column_mapping:
                processed_row[column_mapping[column]] = processed_row.pop(column)

        return processed_row

    def _process_result_schema(
            self,
            schema_fields: Sequence[SchemaField],
            column_mapping: dict
    ) -> Sequence[SchemaField]:
        result_schema = []

        for schema_field in schema_fields:
            record_fields = []

            if SqlTypeNames.RECORD == schema_field.field_type:
                record_fields = self._process_result_schema(
                    schema_field.fields,
                    {
                        column.replace(schema_field.name + '.', ''): value
                        for column, value in column_mapping.items()
                        if column.startswith(schema_field.name + '.')
                    }
                )

            if schema_field.name in column_mapping:
                schema_field = SchemaField(
                    column_mapping[schema_field.name],
                    schema_field.field_type,
                    schema_field.mode,
                    schema_field.description,
                    record_fields,
                    schema_field.policy_tags
                )

            result_schema.append(schema_field)

        return result_schema

    def _process_results_for_bigquery(
            self,
            result_data: Sequence[dict],
            schema: Sequence[SchemaField],
            result_table: TableReference,
            write_disposition: str = WriteDisposition.WRITE_APPEND,
            time_partitioning: TimePartitioning = None
    ):
        job_config = LoadJobConfig()
        job_config.destination = result_table
        job_config.write_disposition = write_disposition
        job_config.schema = schema

        if type(time_partitioning) is TimePartitioning:
            job_config.time_partitioning = time_partitioning

        self._load_jobs.append(
            self.bigquery.client.load_table_from_json(result_data, result_table, job_config=job_config)
        )
