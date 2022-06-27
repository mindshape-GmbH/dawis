from database.connection import Connection
from utilities.exceptions import ConfigurationMissingError, ConfigurationInvalidError
from utilities.configuration import Configuration
from utilities.html import strip_html
from utilities.validator import Validator
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from lxml import etree
from lxml.html import fromstring as document_from_html, HtmlElement
from datetime import timedelta
from os.path import realpath
from requests import get as get_request, Response
from requests.exceptions import RequestException
from time import time
from typing import Sequence
from yaml import load as load_yaml
from yaml import FullLoader
import utilities.datetime as datetime_utility
import re


class Xpath:
    COLLECTION_NAME = 'xpath'

    DEFAULT_MATCH_SEPERATOR = '|'
    SUPPORTED_OPERATIONS = ('null', 'length', 'wordcount', 'regex_match', 'regex_count')

    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        self.configuration = configuration
        self.timezone = configuration.databases.timezone
        self.module_configuration = configuration.aggregations.get_custom_configuration_aggregation(configuration_key)
        self.connection = connection
        self.mongodb = None
        self.bigquery = None

    def run(self):
        print('Running XPath Module:')
        timer_run = time()
        table_reference = None

        if 'bigquery' == self.module_configuration.database:
            self.bigquery = self.connection.bigquery

            if 'dataset' in self.module_configuration.settings and \
                    type(self.module_configuration.settings['dataset']) is str:
                dataset = self.module_configuration.settings['dataset']
            else:
                raise ConfigurationMissingError('Missing dataset for xpath module settings')

            if 'tablename' in self.module_configuration.settings and \
                    type(self.module_configuration.settings['tablename']) is str:
                tablename = self.module_configuration.settings['tablename']
            else:
                raise ConfigurationMissingError('Missing dataset for xpath module settings')

            table_reference = self.bigquery.table_reference(tablename, dataset)
        else:
            self.mongodb = self.connection.mongodb

        clusters = {}

        if 'clusters' in self.module_configuration.settings and \
                type(self.module_configuration.settings['clusters']) is dict:
            clusters = self._process_clusters(
                self.module_configuration.settings['clusters']
            )

        if 'configurations' in self.module_configuration.settings and \
                type(self.module_configuration.settings['configurations']) is list:
            self._process_configurations(
                self.module_configuration.settings['configurations'],
                clusters,
                self.module_configuration.database,
                table_reference
            )

        print('\ncompleted: {:s}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def _process_clusters(self, clusters: dict) -> dict:
        processed_clusters = {}

        for cluster, subcluster in clusters.items():
            if type(subcluster) is dict:
                processed_clusters[cluster] = subcluster
            elif type(subcluster) is str:
                with open(realpath(subcluster)) as clusterfile:
                    processed_clusters[cluster] = load_yaml(clusterfile, Loader=FullLoader)
            else:
                raise ConfigurationInvalidError('Invalid cluster configuration')

        return processed_clusters

    def _process_configurations(
            self,
            configurations: list,
            clusters: dict,
            database: str,
            table_reference: TableReference
    ):
        data = []

        for configuration in configurations:
            operation = None
            operation_options = None

            if 'query' in configuration and type(configuration['query']) is str:
                query = configuration['query']
            else:
                raise ConfigurationMissingError('Missing xpath query for configuration')

            if 'operation' in configuration and type(configuration['operation']) is str:
                operation = configuration['operation'].lower()

                if operation not in self.SUPPORTED_OPERATIONS:
                    raise ConfigurationInvalidError('Invalid operation for xpath configuration')

            if 'operationOptions' in configuration and type(configuration['operationOptions']) is dict:
                operation_options = configuration['operationOptions']

            if 'name' in configuration and type(configuration['name']) is str:
                name = configuration['name']
            else:
                raise ConfigurationMissingError('Missing xpath name for configuration')

            if 'url' in configuration and type(configuration['url']) is str:
                if not Validator.validate_url(configuration['url']):
                    raise ConfigurationInvalidError('Invalid url in xpath configuartion')

                html = self._get_html_from_url(configuration['url'])

                if type(html) is str:
                    data.append({
                        'url': configuration['url'],
                        'query': query,
                        'name': name,
                        'cluster': None,
                        'date': datetime_utility.now(self.timezone),
                        'elements': self._run_operation_on_elements(
                            self._xpath_query_on_html(html, query),
                            operation,
                            operation_options
                        )
                    })

            elif 'cluster' in configuration:
                clusters_configuration = None

                if type(configuration['cluster']) is dict:
                    clusters_configuration = configuration['cluster']
                elif type(configuration['cluster']) is str:
                    if configuration['cluster'] in clusters:
                        clusters_configuration = clusters[configuration['cluster']]
                    else:
                        cluster, subcluster = configuration['cluster'].split(sep=Xpath.DEFAULT_MATCH_SEPERATOR)

                        if cluster in clusters and subcluster in clusters[cluster]:
                            clusters_configuration = {subcluster: clusters[cluster][subcluster]}

                if type(clusters_configuration) is not dict:
                    raise ConfigurationMissingError('Missing cluster configuration')

                for cluster, urls in clusters_configuration.items():
                    for url in urls:
                        if type(url) is not str:
                            raise ConfigurationInvalidError('Invalid url')
                        elif not Validator.validate_url(url):
                            raise ConfigurationInvalidError('Invalid url')

                        html = self._get_html_from_url(url)

                        if type(html) is str:
                            data.append({
                                'url': url,
                                'query': query,
                                'name': name,
                                'cluster': cluster,
                                'date': datetime_utility.now(self.timezone),
                                'elements': self._run_operation_on_elements(
                                    self._xpath_query_on_html(html, query),
                                    operation,
                                    operation_options
                                )
                            })
            else:
                raise ConfigurationMissingError('Missing url parameter for xpath configuration')

        if 'bigquery' == database:
            self._process_responses_for_bigquery(data, table_reference)
        else:
            self._process_responses_for_mongodb(data)

    def _run_operation_on_elements(
            self,
            elements: list,
            operation: str = None,
            operation_options: dict = None
    ) -> Sequence[dict]:
        processed_elements = []

        for element in elements:
            processed_element = {
                'content': element,
                'operation': operation,
                'result': None,
            }

            if operation is None or 'null' == operation:
                processed_element['operation'] = None
            else:
                processed_element['result'] = getattr(
                    self,
                    '_operation_{}'.format(operation)
                )(element, operation_options)

            processed_elements.append(processed_element)

        return processed_elements

    @staticmethod
    def _operation_length(content: str, options: dict) -> int:
        return len(content)

    @staticmethod
    def _operation_wordcount(content: str, options: dict) -> int:
        regex_characters = re.compile(r'[a-z]', re.IGNORECASE)
        text = strip_html(content)
        words = list(filter(lambda x: 1 < len(x) and regex_characters.match(x), text.split()))

        return len(words)

    def _operation_regex_count(self, content: str, options: dict) -> int:
        return len(self._regex_matches(content, options))

    def _operation_regex_match(self, content: str, options: dict) -> str:
        match_seperator = Xpath.DEFAULT_MATCH_SEPERATOR

        if 'matchSeperator' in options and type(options['matchSeperator']) is str:
            match_seperator = options['matchSeperator']

        result = None
        matches = self._regex_matches(content, options)

        if 0 < len(matches):
            results = []

            for match in matches:
                if type(match) is str:
                    results.append(match)
                elif type(match) is tuple:
                    results.append('(' + '),('.join(match) + ')')

            result = match_seperator.join(results)

        return result

    @staticmethod
    def _regex_matches(content: str, options: dict) -> list:
        if 'expression' in options and type(options['expression']) is str:
            case_sensitive = False

            if 'caseSensitive' in options and type(options['caseSensitive']) is bool:
                case_sensitive = options['caseSensitive']

            regex = re.compile(options['expression'], 0 if case_sensitive else re.IGNORECASE)
        else:
            raise ConfigurationMissingError('Missing expression for regex operation')

        matches = regex.findall(content)
        processed_matches = []

        for match in matches:
            if type(match) is str:
                processed_matches.append(match)
            elif type(match) is tuple:
                processed_matches.append('(' + '),('.join(match) + ')')

        return processed_matches

    @staticmethod
    def _xpath_query_on_html(html: str, xpath_query: str) -> list:
        document: HtmlElement = document_from_html(html)
        elements = []

        for element in document.xpath(xpath_query):
            content = None

            if type(element) is HtmlElement:
                content = ''

                for child in element.xpath('./*'):
                    content += etree.tostring(child, pretty_print=True).decode('utf-8')

                if '' == content and type(element.text) is str and not element.text.isspace():
                    content = element.text
            elif isinstance(element, str):
                content = str(element)

            elements.append(content)

        return elements

    @staticmethod
    def _get_html_from_url(url: str) -> str:
        response_body = None

        try:
            response: Response = get_request(url)

            if 200 == response.status_code and str.startswith(response.headers.get('content-type'), 'text/html'):
                if type(response.content) is bytes:
                    response_body = response.content.decode('utf-8')
                else:
                    response_body = response.content
        except RequestException:
            pass

        return response_body

    def _process_responses_for_mongodb(self, data: Sequence[dict]):
        self.mongodb.insert_documents(Xpath.COLLECTION_NAME, data)

    def _process_responses_for_bigquery(self, data: Sequence[dict], table_reference: TableReference):
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')

        job_config.schema = (
            SchemaField('url', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('cluster', SqlTypeNames.STRING),
            SchemaField('name', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('query', SqlTypeNames.STRING, 'REQUIRED'),
            SchemaField('date', SqlTypeNames.DATETIME, 'REQUIRED'),
            SchemaField('elements', SqlTypeNames.RECORD, 'REPEATED', fields=(
                SchemaField('content', SqlTypeNames.STRING),
                SchemaField('operation', SqlTypeNames.STRING),
                SchemaField('result', SqlTypeNames.STRING),
            )),
        )

        for data_item in data:
            data_item['date'] = data_item['date'].strftime('%Y-%m-%dT%H:%M:%S.%f')

            for element in data_item['elements']:
                if element['result'] is not None:
                    element['result'] = str(element['result'])

        load_job = self.bigquery.client.load_table_from_json(data, table_reference, job_config=job_config)
        load_job.result()
