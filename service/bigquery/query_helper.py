from database.connection import BigQuery
from google.cloud.bigquery.client import Client
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.query import ScalarQueryParameter
from datetime import date, datetime, timedelta
from typing import Sequence, Callable


class _InvalidDynamicParameter(Exception):
    pass


class QueryHelper:
    ROW_LIMIT = 25000

    def __init__(self, bigquery_connection: BigQuery, bigquery_client: Client = None):
        self._bigquery_connection = bigquery_connection
        self._bigquery_client = bigquery_client if type(bigquery_client) is Client else bigquery_connection.client

    def run_query(
        self,
        query: str,
        parameters: dict = None,
        column_mapping: dict = None,
        process_result_function: Callable = None,
        additional_parameters: dict = None
    ):
        if parameters is None:
            parameters = {}

        if column_mapping is None:
            column_mapping = {}

        if additional_parameters is None:
            additional_parameters = {}

        query_job = self._bigquery_connection.query(query, self._process_parameters(parameters), self._bigquery_client)
        row_iterator = query_job.result(page_size=self.ROW_LIMIT)
        result_schema = self._process_result_schema(row_iterator.schema, column_mapping)

        for page in row_iterator.pages:
            result_data = []

            for row in page:
                result_item = {}

                for column, value in row.items():
                    result_item[column] = value

                result_data.append(
                    self._process_result_row(result_item, column_mapping)
                )

            if callable(process_result_function):
                process_result_function(result_data, result_schema, **additional_parameters)

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
                    raise _InvalidDynamicParameter('Invalid dynamic parameter, could not be properly processed')

                processed_parameters.append(processed_parameter)

        return processed_parameters

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
