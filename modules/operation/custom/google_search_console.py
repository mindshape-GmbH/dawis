from database.connection import Connection
from database.bigquery import BigQuery
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError
from modules.aggregation.custom.google_search_console import GoogleSearchConsole as AggregationGoogleSearchConsole
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import LoadJobConfig, TimePartitioning, TimePartitioningType
from google.cloud.bigquery.job import WriteDisposition
from google.cloud.bigquery.table import TableReference
from datetime import date, datetime, timedelta
from copy import deepcopy
from os.path import realpath
from os.path import isfile
from pandas import DataFrame, Series, read_csv
import re


class _DataNotAvailableYet(Exception):
    pass


class GoogleSearchConsole:
    ROW_LIMIT = 25000

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        if not connection.has_bigquery() and not connection.has_mongodb():
            raise ConfigurationMissingError('Missing a database configuration for this operation')

        self.configuration = configuration
        self.module_configuration = configuration.operations.get_custom_configuration_operation(configuration_key)
        self.connection = connection
        self.mongodb = connection.mongodb
        self.bigquery = None
        self.matching_group_regex = re.compile(r'\$(\d+)')

    def run(self):
        print('Running operation GSC Matching:')

        processing_configurations = []

        if self.mongodb.has_collection(AggregationGoogleSearchConsole.COLLECTION_NAME_RETRY):
            for retry in self.mongodb.find(
                    AggregationGoogleSearchConsole.COLLECTION_NAME_RETRY,
                    {'module': 'operation'},
                    True
            ):
                del retry['module']
                retry['requestDate'] = retry['requestDate'].date()
                processing_configurations.append(retry)

        if 'properties' in self.module_configuration.settings and \
                type(self.module_configuration.settings['properties']) is list:
            for property_configuration in self.module_configuration.settings['properties']:
                input_dataset = None
                output_dataset = None
                exclude_input_fields = []
                matches = []
                request_days_ago = 3

                if 'property' in property_configuration and type(property_configuration['property']) is str:
                    gsc_property = property_configuration['property']
                else:
                    raise ConfigurationMissingError('property is missing')

                if 'inputTable' in property_configuration and type(property_configuration['inputTable']) is str:
                    input_table = property_configuration['inputTable']
                else:
                    raise ConfigurationMissingError('input table is missing')

                if 'outputTable' in property_configuration and type(property_configuration['outputTable']) is str:
                    output_table = property_configuration['outputTable']
                else:
                    raise ConfigurationMissingError('output table is missing')

                if 'inputDataset' in property_configuration and type(property_configuration['inputDataset']) is str:
                    input_dataset = property_configuration['inputDataset']
                elif 'bigquery' == self.module_configuration.database:
                    raise ConfigurationMissingError('input dataset is missing')

                if 'outputDataset' in property_configuration and type(
                        property_configuration['outputDataset']
                ) is str:
                    output_dataset = property_configuration['outputDataset']
                elif 'bigquery' == self.module_configuration.database:
                    raise ConfigurationMissingError('output dataset is missing')

                if 'excludeInputFields' in property_configuration and type(
                        property_configuration['excludeInputFields']
                ) is list:
                    exclude_input_fields = property_configuration['excludeInputFields']

                if 'matches' in property_configuration and type(property_configuration['matches']) is list:
                    matches = property_configuration['matches']

                if 'dateDaysAgo' in property_configuration and type(property_configuration['dateDaysAgo']) is int:
                    request_days_ago = property_configuration['dateDaysAgo']

                request_date = date.today() - timedelta(days=request_days_ago)

                processing_configuration = {
                    'database': self.module_configuration.database,
                    'property': gsc_property,
                    'requestDate': request_date,
                    'inputTable': input_table,
                    'inputDataset': input_dataset,
                    'outputTable': output_table,
                    'outputDataset': output_dataset,
                    'excludeInputFields': exclude_input_fields,
                    'matches': matches,
                }

                if 0 == len(list(filter(lambda x: x == processing_configuration, processing_configurations))):
                    processing_configurations.append(processing_configuration)

        for processing_configuration in processing_configurations:
            if 'bigquery' == processing_configuration['database'] and type(self.bigquery) is not BigQuery:
                self.bigquery = self.connection.bigquery

            try:
                self._process_property(
                    processing_configuration['database'],
                    processing_configuration['property'],
                    processing_configuration['requestDate'],
                    processing_configuration['inputTable'],
                    processing_configuration['outputTable'],
                    processing_configuration['inputDataset'],
                    processing_configuration['outputDataset'],
                    processing_configuration['excludeInputFields'],
                    self._process_matches_configuration(processing_configuration['matches'])
                )

                if '_id' in processing_configuration:
                    self.mongodb.delete_one(
                        AggregationGoogleSearchConsole.COLLECTION_NAME_RETRY,
                        processing_configuration['_id']
                    )
            except _DataNotAvailableYet:
                existing_retry = None

                if self.mongodb.has_collection(AggregationGoogleSearchConsole.COLLECTION_NAME_RETRY):
                    existing_retry = self.mongodb.find_one(AggregationGoogleSearchConsole.COLLECTION_NAME_RETRY, {
                        'module': 'operation',
                        'property': processing_configuration['property'],
                        'requestDate': datetime.combine(processing_configuration['requestDate'], datetime.min.time()),
                        'inputTable': processing_configuration['inputTable'],
                        'outputTable': processing_configuration['outputTable'],
                    })

                if existing_retry is None:
                    self.mongodb.insert_document(AggregationGoogleSearchConsole.COLLECTION_NAME_RETRY, {
                        'module': 'operation',
                        'database': processing_configuration['database'],
                        'property': processing_configuration['property'],
                        'requestDate': datetime.combine(processing_configuration['requestDate'], datetime.min.time()),
                        'inputTable': processing_configuration['inputTable'],
                        'inputDataset': processing_configuration['inputDataset'],
                        'outputTable': processing_configuration['outputTable'],
                        'outputDataset': processing_configuration['outputDataset'],
                        'excludeInputFields': processing_configuration['excludeInputFields'],
                        'matches': processing_configuration['matches']
                    })

    @staticmethod
    def _process_matches_configuration(configuration_matches: list) -> list:
        matches = []

        for configuration_match in configuration_matches:
            expressions = []
            match = deepcopy(configuration_match)

            if 'fallback' not in match:
                match['fallback'] = ''

            if 'inputField' not in match:
                raise ConfigurationMissingError('missing inputField for match configuration')

            if 'outputField' not in match:
                raise ConfigurationMissingError('missing outputField for match configuration')

            if 'expressions' in match and type(match['expressions']) is list:
                for expression in match['expressions']:
                    case_sensitive = True

                    if 'caseSensitive' in expression:
                        case_sensitive = bool(expression['caseSensitive'])

                    expression['caseSensitive'] = case_sensitive

                    if 'regex' not in expression and 'csv' not in expression:
                        raise ConfigurationMissingError('Missing expression or csv')
                    elif 'csv' in expression:
                        csv_file_path = realpath(expression['csv'])

                        if not isfile(csv_file_path):
                            raise ConfigurationMissingError(
                                'CSV path "{:s}" does not exist'.format(expression['csv'])
                            )

                        expression['csv'] = read_csv(csv_file_path)
                        expression['useRegex'] = bool(expression['useRegex']) \
                            if 'useRegex' in expression else False
                    elif 'regex' in expression:
                        expression['regex'] = re.compile(expression['regex']) \
                            if case_sensitive else \
                            re.compile(expression['regex'], re.IGNORECASE)

                    expressions.append(expression)
            else:
                raise ConfigurationMissingError('missing expressions for match configuration')

            match['expressions'] = expressions

            matches.append(match)

        return matches

    def _process_property(
            self,
            database: str,
            gsc_property: str,
            request_date: date,
            input_table: str,
            output_table: str,
            input_dataset: str,
            output_dataset: str,
            exclude_input_fields: list,
            matches: list
    ):
        output_tablereference = self.bigquery.table_reference(output_table, output_dataset)

        iteration_count = 0

        while True:
            if 'bigquery' == database:
                data = self._get_raw_data_from_bigquery(
                    gsc_property,
                    request_date,
                    input_table,
                    input_dataset,
                    GoogleSearchConsole.ROW_LIMIT,
                    iteration_count * GoogleSearchConsole.ROW_LIMIT
                )
            else:
                data = self._get_raw_data_from_mongodb(
                    gsc_property,
                    request_date,
                    AggregationGoogleSearchConsole.COLLECTION_NAME,
                    GoogleSearchConsole.ROW_LIMIT,
                    iteration_count * GoogleSearchConsole.ROW_LIMIT
                )

            if data.empty:
                if 0 == iteration_count:
                    raise _DataNotAvailableYet()

                break

            data = self._process_data(data, matches, exclude_input_fields)

            if 'bigquery' == database:
                self._process_data_for_bigquery(data, output_tablereference)
            else:
                self._process_data_for_mongodb(data, output_table)

            if GoogleSearchConsole.ROW_LIMIT > len(data.index):
                break

            iteration_count += 1

    def _get_raw_data_from_bigquery(
            self,
            gsc_property: str,
            request_date: date,
            input_table: str,
            input_dataset: str,
            limit: int,
            offset: int
    ) -> DataFrame:
        table_string = '`' + \
                       self.bigquery.client.project + '.' + \
                       input_dataset + '.' + \
                       input_table + \
                       '`'

        query_result = self.bigquery.query(
            'SELECT * FROM {:s} '
            'WHERE property = "{:s}" '
            'AND date = "{:%Y-%m-%d}" '
            'LIMIT {:d} '
            'OFFSET {:d}'.format(
                table_string,
                gsc_property,
                request_date,
                limit,
                offset
            )
        ).result()

        if 0 < query_result.total_rows:
            data = query_result.to_dataframe()
        else:
            data = DataFrame(data=[], columns=[schema_field.name for schema_field in query_result.schema])

        return data

    def _get_raw_data_from_mongodb(
            self,
            gsc_property: str,
            request_date: date,
            input_table: str,
            limit: int,
            offset: int
    ) -> DataFrame:
        rows = self.mongodb.find(
            input_table,
            {
                'property': gsc_property,
                'date': datetime.combine(request_date, datetime.min.time())
            },
            True,
            limit,
            offset
        )

        return DataFrame(rows)

    def _process_data(self, data: DataFrame, matches: list, exclude_input_fields: list) -> DataFrame:
        if 'date' in exclude_input_fields:
            exclude_input_fields.remove('date')

        for exclude_input_field in exclude_input_fields:
            if exclude_input_field in list(data.columns.values):
                data = data.drop([exclude_input_field], axis=1)

        for match in matches:
            input_field = match['inputField']
            output_field = match['outputField']
            fallback = match['fallback']

            data[output_field] = ''

            for expression in match['expressions']:
                if 'regex' in expression:
                    output = expression['output']

                    data = data.apply(
                        self._process_expression_regex,
                        args=(expression['regex'], input_field, output_field, output),
                        axis=1
                    )
                elif 'csv' in expression:
                    case_sensitive = expression['caseSensitive']
                    use_regex = expression['useRegex']
                    csv = expression['csv']

                    for output_column in csv:
                        for column in csv[output_column]:
                            data.loc[
                                data[input_field].str.contains(
                                    column,
                                    regex=use_regex,
                                    flags=0 if case_sensitive else re.IGNORECASE,
                                    case=case_sensitive
                                ),
                                output_field
                            ] = output_column

            if 0 < len(fallback):
                data[output_field] = data[output_field].replace('', fallback)

        return data

    def _process_expression_regex(self, row: Series, regex: re.Pattern, input_field: str, output_field: str, output: str):
        value = str(row[input_field])
        match = regex.search(value)

        if type(match) is re.Match:
            for group in self.matching_group_regex.search(output).groups():
                try:
                    output = output.replace('$' + group, match.group(int(group)))
                except IndexError:
                    pass

            row[output_field] = output

        return row

    def _process_data_for_bigquery(self, data: DataFrame, output_tablereference: TableReference):
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')

        try:
            load_job = self.bigquery.client.load_table_from_dataframe(
                data,
                output_tablereference,
                job_config=job_config
            )

            load_job.result()
        except BadRequest as error:
            print(error.errors)

    def _process_data_for_mongodb(self, data: DataFrame, output_table: str):
        self.mongodb.insert_documents(output_table, data.to_dict())
