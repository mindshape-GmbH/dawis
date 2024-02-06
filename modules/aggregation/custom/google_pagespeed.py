from database.connection import Connection
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationInvalidError, ConfigurationMissingError
from utilities.thread import ResultThread
from utilities.validator import Validator
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from googleapiclient.http import HttpRequest
from datetime import datetime, timedelta
from dict_hash import sha256
from time import time, sleep
from typing import Sequence
import utilities.datetime as datetime_utility
import dateutil
import re


class _InvalidDataException(Exception):
    def __str__(self):
        return 'Invalid data returned from API, the site is maybe broken'


class GooglePagespeed:
    COLLECTION_NAME = 'google_pagespeed'
    COLLECTION_NAME_RETRY = 'google_pagespeed_retry'

    STRATEGIES_ALLOWED = ['desktop', 'mobile', 'both']
    MAX_PARALLEL_REQUESTS = 10
    MAX_RETRY_COUNT = 3
    SECONDS_BETWEEN_REQUESTS = 3
    SECONDS_BETWEEN_REQUESTS_CHUNKS = 10

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration = configuration
        self.timezone = configuration.databases.timezone
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)
        self.connection = connection
        self.mongodb = connection.mongodb
        self.bigquery = None

    def run(self):
        print('Running Google Pagespeed Module:')
        timer_run = time()
        api_key = None

        if 'bigquery' == self.module_configuration.database:
            self.bigquery = self.connection.bigquery

        if 'apiKey' in self.module_configuration.settings and type(self.module_configuration.settings['apiKey']) is str:
            api_key = self.module_configuration.settings['apiKey']

        if 'configurations' in self.module_configuration.settings and \
            type(self.module_configuration.settings['configurations']) is list:
            for configuration in self.module_configuration.settings['configurations']:
                self._process_configuration_cluster(configuration, api_key, self.module_configuration.database)

        if self.mongodb.has_collection(self.COLLECTION_NAME_RETRY):
            for configuration in self.mongodb.find(self.COLLECTION_NAME_RETRY, {}):
                requests = configuration.pop('retry_requests', [])
                self._process_configuration_requests(configuration, requests, self.module_configuration.database)

        print('\ncompleted: {:s}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def _process_configuration_cluster(self, configuration: dict, api_key: str, database: str):
        strategies = ['DESKTOP']
        cluster = {}

        if 'cluster' in configuration and type(configuration['cluster']) is dict:
            for cluster_name, urls in configuration['cluster'].items():
                for url in urls:
                    if type(url) is not str:
                        raise ConfigurationInvalidError('Invalid url')
                    elif not Validator.validate_url(url):
                        raise ConfigurationInvalidError('Invalid url')

                cluster[cluster_name] = urls

        if 'strategy' in configuration and type(configuration['strategy']) is str:
            if configuration['strategy'] in self.STRATEGIES_ALLOWED:
                if 'both' == configuration['strategy']:
                    strategies = ['DESKTOP', 'MOBILE']
                else:
                    strategies = [str.upper(configuration['strategy'])]
            else:
                raise ConfigurationInvalidError('invalid strategy for pagespeed')

        if 'apiKey' in configuration and type(configuration['apiKey']) is str:
            api_key = configuration['apiKey']
        else:
            configuration['apiKey'] = api_key

        requests = []

        if 'requests' in configuration:
            requests = configuration['retry_requests']
        else:
            for cluster_name, urls in cluster.items():
                for url in urls:
                    for strategy in strategies:
                        requests.append([url, cluster_name, strategy, api_key, self.MAX_RETRY_COUNT])

        self._process_configuration_requests(configuration, requests, database)

    def _process_configuration_requests(self, configuration: dict, requests: list, database: str):
        table_reference = None
        log_table_reference = None
        responses = []
        log = []

        if 'bigquery' == database:
            if 'tablename' in configuration and type(configuration['tablename']) is str:
                table_name = configuration['tablename']
            else:
                raise ConfigurationMissingError('Missing tablename for pagespeed to bigquery')

            dataset_name = None

            if 'dataset' in configuration and type(configuration['dataset']) is str:
                dataset_name = configuration['dataset']

            table_reference = self.connection.bigquery.table_reference(table_name, dataset_name)

            if 'logTablename' in configuration and type(configuration['logTablename']) is str:
                log_table_reference = self.connection.bigquery.table_reference(
                    configuration['logTablename'],
                    dataset_name
                )

        responses, failed_requests, log = self._process_requests(requests, responses, log)

        if 0 < len(failed_requests):
            configuration.pop('cluster', None)
            configuration.pop('retry_requests', None)

            if '_id' in configuration:
                self.mongodb.update_one(
                    self.COLLECTION_NAME_RETRY,
                    configuration['_id'],
                    {'retry_requests': failed_requests}
                )
            else:
                configuration['hash'] = sha256(configuration)
                existing_configuration = None

                if self.mongodb.has_collection(self.COLLECTION_NAME_RETRY):
                    existing_configuration = self.mongodb.find_one(
                        self.COLLECTION_NAME_RETRY,
                        {'hash': configuration['hash']},
                        True
                    )

                if type(existing_configuration) is dict:
                    self.mongodb.update_one(
                        self.COLLECTION_NAME_RETRY,
                        existing_configuration['_id'],
                        {'retry_requests': failed_requests}
                    )
                else:
                    configuration['retry_requests'] = failed_requests
                    self.mongodb.insert_document(
                        self.COLLECTION_NAME_RETRY,
                        {'retry_requests': failed_requests, **configuration}
                    )
        elif '_id' in configuration:
            self.mongodb.delete_one(self.COLLECTION_NAME_RETRY, configuration['_id'])

        if 'bigquery' == database:
            self._process_responses_for_bigquery(responses, table_reference)

            if type(log_table_reference) is TableReference:
                self._process_log_for_bigquery(log, log_table_reference)
        else:
            self._process_responses_for_mongodb(responses)

    def _process_requests(self, requests: list, responses: list, log: list) -> tuple:
        status_code_regex = re.compile(r'status[\s\-_]code:?\s?(\d+)', re.IGNORECASE)
        failed_requests = []

        requests_chunks = [
            requests[i:i + GooglePagespeed.MAX_PARALLEL_REQUESTS]
            for i in range(0, len(requests), GooglePagespeed.MAX_PARALLEL_REQUESTS)
        ]

        for requests_chunk in requests_chunks:
            threads = []

            for request in requests_chunk:
                thread = ResultThread(self._process_pagespeed_api, request[:4], {'retry_counter': request[4]})
                thread.start()
                threads.append(thread)
                sleep(self.SECONDS_BETWEEN_REQUESTS)

            for thread in threads:
                thread.join()
                request = thread.get_arguements()
                retry_counter = thread.get_data('retry_counter')

                if isinstance(thread.exception, Exception) or type(thread.result) is not dict:
                    status_code = None

                    if type(thread.exception) is HttpError:
                        match = status_code_regex.search(thread.exception.__str__())

                        if type(match) is re.Match:
                            status_code = int(match.group(1))

                    log.append({
                        'url': request[0],
                        'cluster': request[1],
                        'strategy': request[2],
                        'date': datetime_utility.now(self.timezone),
                        'statusCode': status_code,
                        'message': thread.exception.__str__()
                    })

                    if 0 < retry_counter:
                        retry_counter = retry_counter - 1
                        failed_requests.append([*request, retry_counter])
                else:
                    response = thread.result
                    responses.append(response)

                    log.append({
                        'url': request[0],
                        'cluster': request[1],
                        'strategy': request[2],
                        'date': datetime_utility.now(self.timezone),
                        'statusCode': response['statusCode'],
                        'message': None
                    })

            if len(requests_chunks) != requests_chunks.index(requests_chunk) + 1:
                sleep(GooglePagespeed.SECONDS_BETWEEN_REQUESTS_CHUNKS)

        return responses, failed_requests, log

    def _process_pagespeed_api(
        self,
        url: str,
        cluster: str,
        strategy: str,
        api_key: str
    ) -> dict:
        pagespeed_api = build(
            'pagespeedonline',
            'v5',
            developerKey=api_key,
            cache_discovery=False
        ).pagespeedapi()

        request: HttpRequest = pagespeed_api.runpagespeed(url=url, strategy=strategy)
        return self._process_response(request.execute(), url, cluster, strategy)

    def _process_responses_for_mongodb(self, responses: Sequence[dict]):
        self.mongodb.insert_documents(GooglePagespeed.COLLECTION_NAME, responses)

    def _process_responses_for_bigquery(self, data: Sequence[dict], table_reference: TableReference):
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field=SqlTypeNames.DATE)

        loading_experience_schema_fields = (
            SchemaField('cls', SqlTypeNames.INTEGER),
            SchemaField('clsGood', SqlTypeNames.FLOAT),
            SchemaField('clsMedium', SqlTypeNames.FLOAT),
            SchemaField('clsBad', SqlTypeNames.FLOAT),
            SchemaField('lcp', SqlTypeNames.INTEGER),
            SchemaField('lcpGood', SqlTypeNames.FLOAT),
            SchemaField('lcpMedium', SqlTypeNames.FLOAT),
            SchemaField('lcpBad', SqlTypeNames.FLOAT),
            SchemaField('fcp', SqlTypeNames.INTEGER),
            SchemaField('fcpGood', SqlTypeNames.FLOAT),
            SchemaField('fcpMedium', SqlTypeNames.FLOAT),
            SchemaField('fcpBad', SqlTypeNames.FLOAT),
            SchemaField('fid', SqlTypeNames.INTEGER),
            SchemaField('fidGood', SqlTypeNames.FLOAT),
            SchemaField('fidMedium', SqlTypeNames.FLOAT),
            SchemaField('fidBad', SqlTypeNames.FLOAT),
            SchemaField('inp', SqlTypeNames.FLOAT),
            SchemaField('inpGood', SqlTypeNames.FLOAT),
            SchemaField('inpMedium', SqlTypeNames.FLOAT),
            SchemaField('inpBad', SqlTypeNames.FLOAT),
        )

        job_config.schema = (
            SchemaField('url', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('strategy', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('date', SqlTypeNames.DATETIME, 'REQUIRED'),
            SchemaField('statusCode', SqlTypeNames.INTEGER),
            SchemaField('cluster', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('labdata', SqlTypeNames.RECORD, 'REQUIRED', fields=(
                SchemaField('cls', SqlTypeNames.FLOAT),
                SchemaField('lcp', SqlTypeNames.FLOAT),
                SchemaField('fcp', SqlTypeNames.FLOAT),
                SchemaField('tbt', SqlTypeNames.FLOAT),
                SchemaField('mpfid', SqlTypeNames.FLOAT),
                SchemaField('ttfb', SqlTypeNames.FLOAT),
                SchemaField('performanceScore', SqlTypeNames.FLOAT),
                SchemaField('serverResponseTime', SqlTypeNames.FLOAT),
                SchemaField('usesTextCompression', SqlTypeNames.FLOAT),
                SchemaField('usesLongCacheTtl', SqlTypeNames.FLOAT),
                SchemaField('domSize', SqlTypeNames.FLOAT),
                SchemaField('offscreenImages', SqlTypeNames.FLOAT),
                SchemaField('usesOptimizedImages', SqlTypeNames.FLOAT),
                SchemaField('usesResponsiveImages', SqlTypeNames.FLOAT),
                SchemaField('renderBlockingResources', SqlTypeNames.FLOAT),
                SchemaField('bootupTime', SqlTypeNames.FLOAT),
                SchemaField('mainthreadWorkBreakdown', SqlTypeNames.FLOAT),
            )),
            SchemaField(
                'originLoadingExperience',
                SqlTypeNames.RECORD,
                'REQUIRED',
                fields=loading_experience_schema_fields
            ),
            SchemaField('loadingExperience', SqlTypeNames.RECORD, fields=loading_experience_schema_fields)
        )

        for data_item in data:
            data_item['date'] = data_item['date'].strftime('%Y-%m-%dT%H:%M:%S.%f')

        load_job = self.bigquery.client.load_table_from_json(data, table_reference, job_config=job_config)
        load_job.result()

    def _process_log_for_bigquery(self, log: Sequence[dict], table_reference: TableReference):
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')

        job_config.schema = (
            SchemaField('url', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('cluster', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('strategy', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('date', SqlTypeNames.DATETIME, 'REQUIRED'),
            SchemaField('statusCode', SqlTypeNames.INTEGER),
            SchemaField('message', SqlTypeNames.STRING),
        )

        for log_item in log:
            log_item['date'] = log_item['date'].strftime('%Y-%m-%dT%H:%M:%S.%f')

        load_job = self.bigquery.client.load_table_from_json(log, table_reference, job_config=job_config)
        load_job.result()

    def _process_response(self, response: dict, url: str, cluster: str, strategy: str) -> dict:
        lighthouse_audits = response.get('lighthouseResult', {}).get('audits', {})
        lighthouse_categories = response.get('lighthouseResult', {}).get('categories', {})
        network_items = lighthouse_audits.get('network-requests', {}).get('details', {}).get('items', [])

        try:
            status_code = network_items[0].get('statusCode', None)
        except IndexError:
            status_code = None

        data = {
            'url': url,
            'strategy': strategy,
            'statusCode': status_code,
            'date': dateutil.parser.parse(response['analysisUTCTimestamp']),
            'cluster': cluster,
            'labdata': {
                'cls': lighthouse_audits.get('cumulative-layout-shift', {}).get('numericValue', None),
                'lcp': lighthouse_audits.get('largest-contentful-paint', {}).get('numericValue', None),
                'fcp': lighthouse_audits.get('first-contentful-paint', {}).get('numericValue', None),
                'tbt': lighthouse_audits.get('total-blocking-time', {}).get('numericValue', None),
                'mpfid': lighthouse_audits.get('max-potential-fid', {}).get('numericValue', None),
                'ttfb': lighthouse_audits.get('server-response-time', {}).get('numericValue'),
                'performanceScore': lighthouse_categories.get('performance', {}).get('score', None),
                'serverResponseTime': lighthouse_audits.get('server-response-time', {}).get('score', None),
                'usesTextCompression': lighthouse_audits.get('uses-text-compression', {}).get('score', None),
                'usesLongCacheTtl': lighthouse_audits.get('uses-long-cache-ttl', {}).get('score', None),
                'domSize': lighthouse_audits.get('dom-size', {}).get('score', None),
                'offscreenImages': lighthouse_audits.get('offscreen-images', {}).get('score', None),
                'usesOptimizedImages': lighthouse_audits.get('uses-optimized-images', {}).get('score', None),
                'usesResponsiveImages': lighthouse_audits.get('uses-responsive-images', {}).get('score', None),
                'renderBlockingResources': lighthouse_audits.get('render-blocking-resources', {}).get('score', None),
                'bootupTime': lighthouse_audits.get('bootup-time', {}).get('score', None),
                'mainthreadWorkBreakdown': lighthouse_audits.get('mainthread-work-breakdown', {}).get('score', None),
            },
            'originLoadingExperience': self._loading_experience_data(response, 'originLoadingExperience'),
            'loadingExperience': None,
        }

        if 'loadingExperience' in response and type(response['loadingExperience']) is dict and (
            'origin_fallback' not in response['loadingExperience'] or
            response['loadingExperience']['origin_fallback'] is not True
        ):
            data['loadingExperience'] = self._loading_experience_data(response, 'loadingExperience')

        if not self._validate_response_data(data):
            raise _InvalidDataException()

        return data

    def _loading_experience_data(self, response, key) -> dict:
        loading_experience = {}
        metrics = response.get(key, {}).get('metrics', None)
        extract_metrics = {
            'CUMULATIVE_LAYOUT_SHIFT_SCORE': 'cls',
            'LARGEST_CONTENTFUL_PAINT_MS': 'lcp',
            'FIRST_CONTENTFUL_PAINT_MS': 'fcp',
            'FIRST_INPUT_DELAY_MS': 'fid',
            'INTERACTION_TO_NEXT_PAINT': 'inp',
        }

        if type(metrics) is dict:
            for extract_metric, data_key in extract_metrics.items():
                metric_data = metrics.get(extract_metric, {})
                metric_distributions = metric_data.get('distributions', [])

                loading_experience[data_key] = metric_data.get('percentile', None)

                for index, key_suffix in enumerate(['Good', 'Medium', 'Bad']):
                    try:
                        loading_experience[data_key + key_suffix] = metric_distributions[index].get('proportion', None)
                    except IndexError:
                        loading_experience[data_key + key_suffix] = None

        return loading_experience

    def _validate_response_data(self, data: dict) -> bool:
        if type(data['date']) is not datetime:
            return False

        if 'labdata' not in data:
            return False

        for data_key in ['url', 'strategy', 'cluster']:
            if type(data[data_key]) is not str:
                return False

        return True
