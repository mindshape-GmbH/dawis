from database.connection import Connection, BigQuery, MongoDB
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import UnknownApiNameOrVersion, HttpError
from csv import reader as csv_reader
from dateutil import parser as date_parser
from os.path import abspath, isfile, realpath
from typing import Sequence

import utilities.datetime as datetime_utility


class GoogleSearchInspection:
    COLLECTION_NAME = 'google_search_inspection'

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration = configuration
        self.timezone = configuration.databases.timezone
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)
        self.connection = connection
        self._mongodb_client = connection.mongodb if self.module_configuration.database != 'bigquery' else None
        self._bigquery_client = connection.bigquery if self.module_configuration.database == 'bigquery' else None

    def run(self):
        print('Running aggregation Google Search Inspection:')

        if 'properties' in self.module_configuration.settings and \
            type(self.module_configuration.settings['properties']) is list:

            for property_configuration in self.module_configuration.settings['properties']:
                credentials = None
                language_code = None

                if 'property' in property_configuration and type(property_configuration['property']) is str:
                    gsc_property = property_configuration['property']
                else:
                    raise ConfigurationMissingError('property is missing')

                if 'urls' in property_configuration and type(property_configuration['urls']) is list:
                    urls = property_configuration['urls']
                elif 'urls' in property_configuration and type(property_configuration['urls']) is str:
                    urls = []
                    csv_file_path = realpath(property_configuration['urls'])

                    if not isfile(csv_file_path):
                        raise ConfigurationMissingError(
                            'CSV path "{:s}" does not exist'.format(property_configuration['urls'])
                        )

                    with open(csv_file_path) as csv_file:
                        for row in csv_reader(csv_file):
                            urls.append(row[0])

                else:
                    raise ConfigurationMissingError('Missing property urls')

                if 'credentials' in property_configuration and type(property_configuration['credentials']) is str:
                    credentials = service_account.Credentials.from_service_account_file(
                        abspath(property_configuration['credentials']),
                        scopes=['https://www.googleapis.com/auth/webmasters.readonly']
                    )

                if 'languageCode' in property_configuration and type(property_configuration['languageCode']) is str:
                    language_code = property_configuration['languageCode']

                api_service = build('searchconsole', 'v1', credentials=credentials, cache_discovery=False)
                responses = self._process_urls(api_service, gsc_property, urls, language_code)
                inspection_results = self._process_responses(responses)

                if 'bigquery' == self.module_configuration.database:
                    dataset_name = None

                    if 'tablename' in property_configuration and type(property_configuration['tablename']) is str:
                        table_name = property_configuration['tablename']
                    else:
                        raise ConfigurationMissingError('Missing tablename for search inspection to bigquery')

                    if 'dataset' in property_configuration and type(property_configuration['dataset']) is str:
                        dataset_name = property_configuration['dataset']

                    table_reference = self.connection.bigquery.table_reference(table_name, dataset_name)
                    self._process_inspection_results_for_bigquery(table_reference, inspection_results)
                else:
                    self._process_inspection_results_for_mongodb(inspection_results)

    def _process_urls(self, api_service: Resource, gsc_property: str, urls: list, language_code: str = None) -> list:
        responses = []

        for url in urls:
            request_date = datetime_utility.now(self.timezone)
            request = {
                'inspectionUrl': url,
                'siteUrl': gsc_property
            }

            if language_code is not None:
                request['languageCode'] = language_code

            try:
                response: dict = api_service.urlInspection().index().inspect(body=request).execute()
            except (UnknownApiNameOrVersion, HttpError) as error:
                print('Error when requesting search inspection ({}}):\n{}'.format(url, error))
                continue

            responses.append(response.update({
                'url': url,
                'requestDate': request_date,
                'languageCode': language_code,
            }))

        return responses

    def _process_responses(self, responses: list) -> list:
        inspection_results = []

        for response in responses:
            request_date = response['requestDate']
            language_code = response['languageCode']
            inspection_result = response['inspectionResult']
            mobile_usability_result = None
            rich_results_result = None
            amp_result = None

            if 'mobileUsabilityResult' in inspection_result:
                issues = None

                if 'issues' in inspection_result['mobileUsabilityResult']:
                    issues = []

                    for issue in inspection_result['mobileUsabilityResult']['issues']:
                        issues.append({
                            'issueType': issue['issueType'],
                            'severity': issue['severity'],
                            'message': issue['message'],
                        })

                mobile_usability_result = {
                    'verdict': inspection_result['mobileUsabilityResult']['verdict'],
                    'issues': issues,
                }

            if 'richResultsResult' in inspection_result:
                rich_results_result = {
                    'verdict': inspection_result['richResultsResult']['verdict'],
                    'detectedItems': None
                }

                if 'detectedItems' in inspection_result['richResultsResult']:
                    detected_items = []

                    for detected_item in inspection_result['richResultsResult']['detectedItems']:
                        items = None

                        if 'items' in detected_item:
                            items = []

                            for item in detected_item['items']:
                                issues = None

                                if 'issues' in item:
                                    issues = []

                                    for issue in item['issues']:
                                        issues.append({
                                            'issueMessage': issue['issueMessage'],
                                            'severity': issue['severity'],
                                        })

                                items.append({
                                    'name': item['name'],
                                    'issues': issues,
                                })

                        detected_items.append({
                            'richResultType': detected_item['richResultType'],
                            'items': items,
                        })

                    rich_results_result['detectedItems'] = detected_items

            if 'ampResult' in inspection_result:
                amp_result = {
                    'verdict': inspection_result['ampResult']['verdict'],
                    'ampUrl': inspection_result['ampResult']['ampUrl'],
                    'robotsTxtState': inspection_result['ampResult']['robotsTxtState'],
                    'indexingState': inspection_result['ampResult']['indexingState'],
                    'ampIndexStatusVerdict': inspection_result['ampResult']['ampIndexStatusVerdict'],
                    'pageFetchState': inspection_result['ampResult']['pageFetchState'],
                    'issues': None
                }

                if 'issues' in inspection_result['ampResult']:
                    issues = []

                    for issue in inspection_result['ampResult']['issues']:
                        issues.append({
                            'issueMessage': issue['issueMessage'],
                            'severity': issue['severity'],
                        })

                    amp_result['issues'] = issues

                last_crawl_time = date_parser.parse(inspection_result['ampResult']['lastCrawlTime'])
                last_crawl_time = last_crawl_time.astimezone(datetime_utility.get_timezone(self.timezone))

                amp_result['lastCrawlTime'] = last_crawl_time

            index_status_result = {
                'verdict': inspection_result['indexStatusResult']['verdict'],
                'coverageState': inspection_result['indexStatusResult']['coverageState'],
                'robotsTxtState': inspection_result['indexStatusResult']['robotsTxtState'],
                'indexingState': inspection_result['indexStatusResult']['indexingState'],
                'lastCrawlTime': None,
                'pageFetchState': inspection_result['indexStatusResult']['pageFetchState'],
                'googleCanonical': None,
                'userCanonical': None,
                'crawledAs': None,
                'referringUrls': None,
                'sitemap': None,
            }

            if 'referringUrls' in inspection_result['indexStatusResult']:
                index_status_result['referringUrls'] = inspection_result['indexStatusResult']['referringUrls']

            if 'sitemap' in inspection_result['indexStatusResult']:
                index_status_result['sitemap'] = inspection_result['indexStatusResult']['sitemap']

            if 'googleCanonical' in inspection_result['indexStatusResult']:
                index_status_result['googleCanonical'] = inspection_result['indexStatusResult']['googleCanonical']

            if 'userCanonical' in inspection_result['indexStatusResult']:
                index_status_result['userCanonical'] = inspection_result['indexStatusResult']['userCanonical']

            if 'crawledAs' in inspection_result['indexStatusResult']:
                index_status_result['crawledAs'] = inspection_result['indexStatusResult']['crawledAs']

            if 'lastCrawlTime' in inspection_result['indexStatusResult']:
                last_crawl_time = date_parser.parse(inspection_result['indexStatusResult']['lastCrawlTime'])
                last_crawl_time = last_crawl_time.astimezone(datetime_utility.get_timezone(self.timezone))

                index_status_result['lastCrawlTime'] = last_crawl_time

            inspection_results.append({
                'url': response['url'],
                'requestDate': request_date,
                'languageCode': language_code,
                'inspectionResultLink': inspection_result['inspectionResultLink'],
                'indexStatusResult': index_status_result,
                'mobileUsabilityResult': mobile_usability_result,
                'richResultsResult': rich_results_result,
                'ampResult': amp_result,
            })

        return inspection_results

    def _process_inspection_results_for_bigquery(
        self,
        table_reference: TableReference,
        inspection_results: Sequence[dict]
    ):
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='requestDate')

        job_config.schema = (
            SchemaField('url', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('languageCode', SqlTypeNames.STRING),
            SchemaField('requestDate', SqlTypeNames.DATETIME, 'REQUIRED'),
            SchemaField('inspectionResultLink', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('indexStatusResult', SqlTypeNames.RECORD, 'REQUIRED', fields=(
                SchemaField('verdict', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('coverageState', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('robotsTxtState', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('indexingState', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('lastCrawlTime', SqlTypeNames.DATETIME),
                SchemaField('pageFetchState', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('googleCanonical', SqlTypeNames.STRING),
                SchemaField('userCanonical', SqlTypeNames.STRING),
                SchemaField('crawledAs', SqlTypeNames.STRING),
                SchemaField('referringUrls', SqlTypeNames.STRING, 'REPEATED'),
                SchemaField('sitemap', SqlTypeNames.STRING, 'REPEATED'),
            )),
            SchemaField('mobileUsabilityResult', SqlTypeNames.RECORD, fields=(
                SchemaField('verdict', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('issues', SqlTypeNames.RECORD, 'REPEATED', fields=(
                    SchemaField('issueType', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('severity', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('message', SqlTypeNames.STRING, 'REQUIRED'),
                )),
            )),
            SchemaField('richResultsResult', SqlTypeNames.RECORD, fields=(
                SchemaField('verdict', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('detectedItems', SqlTypeNames.RECORD, 'REPEATED', fields=(
                    SchemaField('richResultType', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('items', SqlTypeNames.RECORD, 'REPEATED', fields=(
                        SchemaField('name', SqlTypeNames.STRING, 'REQUIRED'),
                        SchemaField('issues', SqlTypeNames.RECORD, 'REPEATED', fields=(
                            SchemaField('issueMessage', SqlTypeNames.STRING, 'REQUIRED'),
                            SchemaField('severity', SqlTypeNames.STRING, 'REQUIRED'),
                        )),
                    )),
                )),
            )),
            SchemaField('ampResult', SqlTypeNames.RECORD, fields=(
                SchemaField('verdict', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('ampUrl', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('robotsTxtState', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('indexingState', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('ampIndexStatusVerdict', SqlTypeNames.STRING, 'REQUIRED'),
                SchemaField('lastCrawlTime', SqlTypeNames.DATETIME, 'REQUIRED'),
                SchemaField('pageFetchState', SqlTypeNames.DATETIME, 'REQUIRED'),
                SchemaField('issues', SqlTypeNames.RECORD, 'REPEATED', fields=(
                    SchemaField('issueMessage', SqlTypeNames.STRING, 'REQUIRED'),
                    SchemaField('severity', SqlTypeNames.STRING, 'REQUIRED'),
                )),
            )),
        )

        for inspection_result in inspection_results:
            inspection_result['requestDate'] = inspection_result['requestDate'].strftime('%Y-%m-%dT%H:%M:%S.%f')

            if inspection_result['indexStatusResult']['lastCrawlTime'] is not None:
                inspection_result['indexStatusResult']['lastCrawlTime'] = \
                    inspection_result['indexStatusResult']['lastCrawlTime'].strftime('%Y-%m-%dT%H:%M:%S.%f')

            if type(inspection_result['ampResult']) is dict:
                inspection_result['ampResult']['lastCrawlTime'] = \
                    inspection_result['ampResult']['lastCrawlTime'].strftime('%Y-%m-%dT%H:%M:%S.%f')

        load_job = self._bigquery_client.client.load_table_from_json(
            inspection_results,
            table_reference,
            job_config=job_config
        )

        load_job.result()

    def _process_inspection_results_for_mongodb(self, inspection_results: Sequence[dict]):
        self._mongodb_client.insert_documents(self.COLLECTION_NAME, inspection_results)
