from database.bigquery import BigQuery
from database.connection import Connection
from service.api.sistrix import Client as SistrixApiClient
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationInvalidError, ConfigurationMissingError
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from pandas import DataFrame
from datetime import datetime, date, timedelta
from typing import Sequence
from time import time

import utilities.datetime as datetime_utility


class _DataNotAvailableYet(Exception):
    pass


class SistrixDomain:
    COLLECTION_NAME = 'sistrix_domain'
    API_FORMAT = 'json'

    DAILY_PARAMETER_ALLOWED = [
        SistrixApiClient.ENDPOINT_DOMAIN_VISIBILITYINDEX,
        SistrixApiClient.ENDPOINT_DOMAIN_VISIBILITYINDEX_OVERVIEW
    ]

    METHODS_PARAMETERS_ALLOWED = {
        SistrixApiClient.ENDPOINT_DOMAIN_VISIBILITYINDEX: ['daily', 'mobile', 'country'],
        SistrixApiClient.ENDPOINT_DOMAIN_PAGES: [],
        SistrixApiClient.ENDPOINT_DOMAIN_KEYWORDCOUNT_SEO: [],
        SistrixApiClient.ENDPOINT_DOMAIN_KEYWORDCOUNT_SEO_TOP10: [],
    }

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration = configuration
        self.timezone = configuration.databases.timezone
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)
        self.connection = connection
        self.mongodb = None
        self.bigquery = None

    def run(self):
        print('Running Sistrix Domain Module:')
        timer_run = time()

        if 'configurations' in self.module_configuration.settings and \
            type(self.module_configuration.settings['configurations']) is list:
            for configuration in self.module_configuration.settings['configurations']:
                self._process_request_configuration(configuration, self.module_configuration.database)

        print('\ncompleted: {}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def _process_request_configuration(self, configuration: dict, database: str):
        api_key = ''
        domain = None
        host = None
        paths = None
        urls = None
        daily = False
        on_weekday = None
        add_parameters_to_result = False
        methods = []
        dataset = None
        table_reference = None
        requests = []
        request_date = datetime_utility.now(self.timezone)

        if 'Europe/Berlin' != self.timezone:
            request_date = request_date.astimezone(datetime_utility.get_timezone('Europe/Berlin')).date()

        if 'apiKey' in configuration and type(configuration['apiKey']) is str:
            api_key = configuration['apiKey']

        if 'domain' in configuration and type(configuration['domain']) is str:
            domain = configuration['domain']

        if 'host' in configuration and type(configuration['host']) is str:
            host = configuration['host']

        if 'paths' in configuration and type(configuration['paths']) is list:
            paths = configuration['paths']

        if 'urls' in configuration and type(configuration['urls']) is list:
            urls = configuration['urls']

        if 'onlyOnWeekday' in configuration and (
            type(configuration['onlyOnWeekday']) is str or
            type(configuration['onlyOnWeekday']) is int
        ):
            on_weekday = configuration['onlyOnWeekday']
        else:
            daily = True

        if 'addParametersToResult' in configuration and type(configuration['addParametersToResult']) is bool:
            add_parameters_to_result = configuration['addParametersToResult']

        if 'methods' in configuration and type(configuration['methods']) is list:
            for method in configuration['methods']:
                if 'method' not in method and type(method['method']) is not str:
                    raise ConfigurationMissingError('Missing api method')
                elif not method['method'].startswith('domain.'):
                    method['method'] = 'domain.' + method['method']
                if method['method'] not in SistrixDomain.METHODS_PARAMETERS_ALLOWED.keys():
                    raise ConfigurationInvalidError('The method "{}" is not allowed'.format(method['method']))
                if 'fieldName' not in method and type(method['fieldName']) is not str:
                    raise ConfigurationMissingError('Missing a field name in api method')
                if 'parameters' not in method:
                    method['parameters'] = {}
                elif type(method['parameters']) is not dict:
                    raise ConfigurationInvalidError('Method parameters must be type of dictionary')
                for parameter in method['parameters']:
                    if parameter not in SistrixDomain.METHODS_PARAMETERS_ALLOWED[method['method']]:
                        raise ConfigurationInvalidError(
                            'The parameter "{}" for "{}" is not allowed in this module'.format(
                                parameter,
                                method['method']
                            )
                        )

                if method['method'] in SistrixDomain.DAILY_PARAMETER_ALLOWED:
                    method['parameters']['daily'] = daily

                methods.append(method)

        if 0 == len(methods):
            raise ConfigurationMissingError('Missing methods to request')

        if 'dataset' in configuration and type(configuration['dataset']) is str:
            dataset = configuration['dataset']

        if 'bigquery' == database:
            if type(self.bigquery) is not BigQuery:
                self.bigquery = self.connection.bigquery

            if 'table' in configuration and type(configuration['table']) is str:
                table_reference = self.bigquery.table_reference(configuration['table'], dataset)
            else:
                raise ConfigurationMissingError('You have to set at least a table if you want to use bigquery')

        if (domain is not None and (host is not None or paths is not None or urls is not None)) or \
            (host is not None and (domain is not None or paths is not None or urls is not None)) or \
            (paths is not None and (host is not None or domain is not None or urls is not None)) or \
            (urls is not None and (host is not None or paths is not None or domain is not None)):
            raise ConfigurationInvalidError('You can\'t use domain, host, path or url parallel to each other')

        if domain is None and host is None and paths is None and urls is None:
            raise ConfigurationInvalidError('You need one of these parameters: "domain, host, path, url"')

        if on_weekday is not None and (
            # weekday format may get influnced by locale
            on_weekday != '{:%a}'.format(datetime.now()) and
            on_weekday != '{:%A}'.format(datetime.now()) and
            on_weekday != datetime.now().isoweekday()
        ):
            return

        if domain is not None:
            requests.append({'domain': domain})

        if host is not None:
            requests.append({'host': host})

        if paths is not None:
            for path in paths:
                requests.append({'path': path})

        if urls is not None:
            for url in urls:
                requests.append({'url': url})

        responses = []

        sistrix_api_client = SistrixApiClient(api_key)

        for request in requests:
            for key, value in request.items():
                response_row = {}

                for method in methods:
                    if 'bigquery' == database:
                        if self._bigquery_check_has_existing_data(
                            table_reference,
                            request_date,
                            add_parameters_to_result,
                            method['parameters']
                        ):
                            continue
                    else:
                        if self._mongodb_check_has_existing_data(request_date, method['parameters']):
                            continue

                    response_row = self._sistrix_api_requests(
                        sistrix_api_client,
                        method,
                        response_row,
                        add_parameters_to_result,
                        **{key: value}
                    )

                    if add_parameters_to_result:
                        responses.append(
                            {
                                key: value,
                                'date': request_date,
                                **response_row
                            }
                        )

                if not add_parameters_to_result:
                    responses.append(
                        {
                            key: value,
                            'date': request_date,
                            **response_row
                        }
                    )

        if 0 < len(responses):
            if table_reference is None and 'mongodb' == database:
                self.mongodb.insert_documents(SistrixDomain.COLLECTION_NAME, responses)
            elif 'bigquery' == database:
                self._process_response_rows_for_bigquery(responses, methods, table_reference)
            else:
                ConfigurationInvalidError('Invalid database configuration for this module')

    def _sistrix_api_requests(
        self,
        sistrix_api_client: SistrixApiClient,
        method: dict,
        response_row: dict,
        add_parameters_to_result: bool,
        **request_parameters
    ) -> dict:
        response = sistrix_api_client.request(method['method'], {**request_parameters, **method['parameters']})

        if SistrixApiClient.ENDPOINT_DOMAIN_VISIBILITYINDEX == method['method']:
            response_row[method['fieldName']] = self._process_response_visibilityindex(response)
        elif SistrixApiClient.ENDPOINT_DOMAIN_PAGES == method['method']:
            response_row[method['fieldName']] = self._process_response_pages(response)
        elif SistrixApiClient.ENDPOINT_DOMAIN_KEYWORDCOUNT_SEO == method['method']:
            response_row[method['fieldName']] = self._process_response_keywordcount_seo(response)
        elif SistrixApiClient.ENDPOINT_DOMAIN_KEYWORDCOUNT_SEO_TOP10 == method['method']:
            response_row[method['fieldName']] = self._process_response_keywordcount_seo_top10(response)
        else:
            raise ConfigurationInvalidError(
                'Method "{}" not mappable for response processing'.format(method['method'])
            )

        if add_parameters_to_result:
            response_row = {**response_row, **method['parameters']}

        return response_row

    @staticmethod
    def _process_response_visibilityindex(response: dict) -> float:
        try:
            return float(response['answer'][0]['sichtbarkeitsindex'][0]['value'])
        except KeyError:
            raise _DataNotAvailableYet()

    @staticmethod
    def _process_response_pages(response: dict) -> int:
        try:
            return int(response['answer'][0]['pages'][0]['value'])
        except KeyError:
            raise _DataNotAvailableYet()

    @staticmethod
    def _process_response_keywordcount_seo(response: dict) -> int:
        try:
            return int(response['answer'][0]['kwcount.seo'][0]['value'])
        except KeyError:
            raise _DataNotAvailableYet()

    @staticmethod
    def _process_response_keywordcount_seo_top10(response: dict) -> int:
        try:
            return int(response['answer'][0]['kwcount.seo.top10'][0]['value'])
        except KeyError:
            raise _DataNotAvailableYet()

    def _process_response_rows_for_bigquery(
        self,
        rows: Sequence[dict],
        methods: Sequence[dict],
        table_reference: TableReference
    ):
        rows_dataframe = DataFrame.from_records(rows)
        rows_dataframe['date'] = rows_dataframe['date'].apply(lambda x: x.date())

        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')
        job_config.schema = [
            self._get_schema_for_field(column, methods) for column in list(rows_dataframe.columns.values)
        ]

        try:
            load_job = self.bigquery.client.load_table_from_dataframe(
                rows_dataframe,
                table_reference,
                job_config=job_config
            )

            load_job.result()
        except BadRequest as error:
            print(error.errors)

    @staticmethod
    def _get_schema_for_field(column: str, methods: Sequence[dict]):
        field_type = 'STRING'
        field_mode = 'REQUIRED'

        method = next((method['method'] for method in methods if method['fieldName'] == column), None)

        if SistrixApiClient.ENDPOINT_DOMAIN_VISIBILITYINDEX == method:
            field_type = SqlTypeNames.FLOAT
            field_mode = 'NULLABLE'
        if SistrixApiClient.ENDPOINT_DOMAIN_PAGES == method or \
            SistrixApiClient.ENDPOINT_DOMAIN_KEYWORDCOUNT_SEO == method or \
            SistrixApiClient.ENDPOINT_DOMAIN_KEYWORDCOUNT_SEO_TOP10 == method:
            field_type = SqlTypeNames.INTEGER
            field_mode = 'NULLABLE'

        if 'date' == column:
            field_type = SqlTypeNames.DATE

        if 'daily' == column or 'mobile' == column:
            field_type = SqlTypeNames.BOOLEAN

        return SchemaField(column, field_type, field_mode)

    def _bigquery_check_has_existing_data(
        self,
        table_reference: TableReference,
        request_date: date,
        add_parameters_to_result: bool,
        parameters: dict
    ) -> bool:
        if not self.bigquery.has_table(table_reference.table_id, table_reference.dataset_id):
            return False

        # Using an alias is necessary due to bigquery issues when column name equals the table name
        query = 'SELECT COUNT(*) FROM `{dataset}`.`{table}` AS count_table WHERE `date` = "{date:%Y-%m-%d}" '.format(
            dataset=table_reference.dataset_id,
            table=table_reference.table_id,
            date=request_date
        )

        if add_parameters_to_result:
            for key, value in parameters.items():
                if type(value) is str:
                    value = '"{value}"'.format(value=value)
                if type(value) is bool:
                    value = '{value}'.format(value='true' if value else 'false')

                query += 'AND `{key}` = {value} '.format(
                    key=key,
                    value=value
                )

        query_job = self.bigquery.query(query)

        count = 0

        for row in query_job.result():
            count = row[0]

        return 0 < count

    def _mongodb_check_has_existing_data(self, request_date: date, parameters: dict) -> bool:
        if not self.mongodb.has_collection(SistrixDomain.COLLECTION_NAME):
            return False

        return 0 < self.mongodb.find(
            SistrixDomain.COLLECTION_NAME,
            {
                'date': datetime.combine(request_date, datetime.min.time()),
                **parameters
            },
            cursor=True
        ).count()
