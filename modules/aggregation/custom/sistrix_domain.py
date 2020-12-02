from database.bigquery import BigQuery
from database.connection import Connection
from service.api.sistrix import Client as SistrixApiClient
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationInvalidError, ConfigurationMissingError
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from pandas import DataFrame, Series
from datetime import datetime, date, timedelta
from typing import Sequence
from time import time


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
        methods = []
        dataset = None
        table_reference = None
        request_date = datetime.now().date()
        requests = []

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

                if daily and 'domain.' + method['method'] in SistrixDomain.DAILY_PARAMETER_ALLOWED:
                    method['parameters']['daily'] = True

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

        for request in requests:
            for key, value in request.items():
                if 'bigquery' == database and self._bigquery_check_has_existing_data(
                        key,
                        value,
                        table_reference,
                        request_date
                ):
                    continue
                elif 'mongodb' == database and self._mongodb_check_has_existing_data(key, value, request_date):
                    continue

                responses.append(
                    {
                        key: value,
                        'date': request_date,
                        **self._sistrix_api_requests(api_key, methods, **{key: value})
                    }
                )

        if table_reference is None and 'mongodb' == database:
            self.mongodb.insert_documents(SistrixDomain.COLLECTION_NAME, responses)
        elif 'bigquery' == database:
            self._process_response_rows_for_bigquery(responses, methods, table_reference)
        else:
            ConfigurationInvalidError('Invalid database configuration for this module')

    def _sistrix_api_requests(
            self,
            api_key: str,
            methods: list,
            **request_parameters
    ) -> dict:
        sistrix_api_client = SistrixApiClient(api_key)
        response_row = {}

        for method in methods:
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

        job_config = LoadJobConfig()
        job_config.destination = table_reference
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

        if 'date' == column:
            field_type = 'DATE'

        method = next((method['method'] for method in methods if method['fieldName'] == column), None)

        if SistrixApiClient.ENDPOINT_DOMAIN_VISIBILITYINDEX == method:
            field_type = 'FLOAT64'
        if SistrixApiClient.ENDPOINT_DOMAIN_PAGES == method or \
                SistrixApiClient.ENDPOINT_DOMAIN_KEYWORDCOUNT_SEO == method or \
                SistrixApiClient.ENDPOINT_DOMAIN_KEYWORDCOUNT_SEO_TOP10 == method:
            field_type = 'INT64'

        return SchemaField(column, field_type, field_mode)

    def _bigquery_check_has_existing_data(
            self,
            key: str,
            value: str,
            table_reference: TableReference,
            request_date: date
    ) -> bool:
        if not self.bigquery.has_table(table_reference.table_id, table_reference.dataset_id):
            return False

        query_job = self.bigquery.query(
            # Using an alias is necessary due to bigquery issues when column name equals the table name
            'SELECT COUNT(*) FROM `{dataset}`.`{table}` AS {key}_table '
            'WHERE `date` = "{date:%Y-%m-%d}" '
            'AND `{key}` = "{value}"'.format(
                dataset=table_reference.dataset_id,
                table=table_reference.table_id,
                date=request_date,
                key=key,
                value=value
            )
        )

        count = 0

        for row in query_job.result():
            count = row[0]

        return 0 < count

    def _mongodb_check_has_existing_data(self, key: str, value: str, request_date: date) -> bool:
        if not self.mongodb.has_collection(SistrixDomain.COLLECTION_NAME):
            return False

        return 0 < self.mongodb.find(
            SistrixDomain.COLLECTION_NAME,
            {
                key: value,
                'date': datetime.combine(request_date, datetime.min.time())
            },
            cursor=True
        ).count()
