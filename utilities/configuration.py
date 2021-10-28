from utilities.url import URL
from typing import Sequence
from typing import Dict
import re

DEFAULT_MODULE_RUNTIME_LIMIT = 600


class ConfigurationORM:
    def __init__(
            self,
            connection_url: str = None,
            dbtype: str = None,
            host: str = None,
            port: int = None,
            dbname: str = None,
            username: str = None,
            password: str = None
    ):
        self.url = connection_url
        self.dbtype = dbtype
        self.host = host
        self.port = port
        self.dbname = dbname
        self.username = username
        self.password = password

    @property
    def connection_url(self) -> str:
        if type(self.url) is str:
            connection_string = self.url
        else:
            connection_string = self.dbtype + '://' \
                                + self.username + ':' \
                                + self.password + '@' \
                                + self.host + ':' \
                                + str(self.port) \
                                + '/' + self.dbname

        return re.compile(r'^mysql\+?.*?:', re.IGNORECASE).sub('mysql+pymysql:', connection_string)


class ConfigurationMongoDB:
    def __init__(self, host: str, port: int, dbname: str, username: str, password: str):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.username = username
        self.password = password


class ConfigurationBigQueryDataset:
    def __init__(
            self,
            project: str,
            name: str,
            location: str,
            description: str = None,
            labels: Sequence[Dict[str, str]] = None
    ):
        self.project = project
        self.name = name
        self.location = location
        self.description = description
        self.labels = labels


class ConfigurationBigQuery:
    def __init__(
            self,
            project: str,
            dataset: ConfigurationBigQueryDataset,
            additional_datasets: Dict[str, ConfigurationBigQueryDataset] = None,
            credentials: str = None
    ):
        if additional_datasets is None:
            additional_datasets = {}

        self.project = project
        self.dataset = dataset
        self.credentials = credentials
        self.additional_datasets = additional_datasets


class ConfigurationDatabases:
    def __init__(
            self,
            mongodb: ConfigurationMongoDB,
            orm: ConfigurationORM = None,
            bigquery: ConfigurationBigQuery = None,
            timezone: str = 'UTC',
    ):
        self.timezone = timezone
        self.mongodb = mongodb
        self.orm = orm
        self.bigquery = bigquery


class ConfigurationUrl:
    def __init__(self, url: URL, render: bool):
        self.url = url
        self.render = render


class ConfigurationUrlset:
    def __init__(self, name: str, url_configurations: Sequence[ConfigurationUrl]):
        self.name = name
        self.configuration_urls = url_configurations


class ConfigurationUrlsets:
    def __init__(self, urlsets: Sequence[ConfigurationUrlset]):
        self.urlsets = urlsets

    def urlset_urls(self, urlset_name: str) -> Sequence[URL]:
        urls = []

        for urlset in self.urlsets:
            if urlset_name == urlset.name:
                for configuration_url in urlset.configuration_urls:
                    urls.append(configuration_url.url)

        return urls


class ConfigurationAggregation:
    def __init__(
            self,
            module: str,
            cron: str,
            urlsets: Sequence[str],
            settings: dict,
            database: str,
            runtime_limit: int = DEFAULT_MODULE_RUNTIME_LIMIT
    ):
        self.module = module
        self.cron = cron
        self.urlsets = urlsets
        self.database = database
        self.settings = settings
        self.runtime_limit = runtime_limit


class ConfigurationAggregations:
    def __init__(self, configurations: dict = None):
        if configurations is None:
            configurations = {}

        self.config = configurations

    def get_custom_configuration_aggregation(self, key) -> ConfigurationAggregation:
        return self.config.get(key)


class ConfigurationOperation:
    def __init__(
            self,
            module: str,
            cron: str,
            urlsets: Sequence[str],
            checks: dict,
            database: str,
            settings: dict,
            runtime_limit: int = DEFAULT_MODULE_RUNTIME_LIMIT
    ):
        self.module = module
        self.cron = cron
        self.urlsets = urlsets
        self.checks = checks
        self.database = database
        self.settings = settings
        self.runtime_limit = runtime_limit


class ConfigurationOperations:
    def __init__(self, configurations: dict = None):
        if configurations is None:
            configurations = {}

        self.config = configurations

    def get_custom_configuration_operation(self, key):
        return self.config.get(key)


class Configuration:
    def __init__(
            self,
            configuration_databases: ConfigurationDatabases,
            configuration_urlsets: ConfigurationUrlsets,
            configuration_aggregations: ConfigurationAggregations,
            configuration_operations: ConfigurationOperations,
            configuration_hash: str
    ):
        self.databases = configuration_databases
        self.urlsets = configuration_urlsets
        self.aggregations = configuration_aggregations
        self.operations = configuration_operations
        self.hash = configuration_hash

    def __hash__(self) -> str:
        return self.hash
