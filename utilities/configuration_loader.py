from utilities.exceptions import ExitError, ConfigurationMissingError, ConfigurationInvalidError
from utilities.validator import Validator
from utilities.configuration import ConfigurationOperation
from utilities.configuration import ConfigurationOperations
from utilities.configuration import ConfigurationAggregation
from utilities.configuration import ConfigurationAggregations
from utilities.configuration import ConfigurationUrl
from utilities.configuration import ConfigurationUrlset
from utilities.configuration import ConfigurationUrlsets
from utilities.configuration import ConfigurationORM
from utilities.configuration import ConfigurationMongoDB
from utilities.configuration import ConfigurationDatabases
from utilities.configuration import ConfigurationBigQuery
from utilities.configuration import ConfigurationBigQueryDataset
from utilities.configuration import Configuration
from utilities.configuration import URL
from database.mongodb import MongoDB
from typing import Sequence
from datetime import datetime
from yaml import load
from yaml import FullLoader
from dict_hash import sha256
from os import getcwd, chdir
from os.path import abspath
from os.path import basename
from os.path import dirname
from os.path import realpath
from glob import glob


class ConfigurationLoader:
    _configuration_cache = {}

    @staticmethod
    def load_by_hash(current_configuration: Configuration, configuration_hash: str) -> Configuration:
        existing_configuration = ConfigurationLoader._get_existing_configuration(
            current_configuration.databases.mongodb,
            configuration_hash
        )

        if existing_configuration is None:
            raise ExitError('Configuration for hash could not be found, please check your data integrity')

        return ConfigurationLoader._process_configuration(existing_configuration, current_configuration.databases)

    @staticmethod
    def load_by_file(configuration_file_path: str, save: bool = True) -> Configuration:
        configuration_file_path = configuration_file_path

        with open(configuration_file_path) as configurationFile:
            plain_configuration = load(configurationFile, Loader=FullLoader)

        if plain_configuration is None:
            raise ExitError('Configuration file "' + configuration_file_path + '" is empty or YAML parsing failed')

        configuration_hash = sha256(plain_configuration)

        if configuration_hash in ConfigurationLoader._configuration_cache:
            return ConfigurationLoader._configuration_cache[configuration_hash]

        databases_configuration = ConfigurationLoader._process_configuration_databases(plain_configuration)
        existing_configuration = ConfigurationLoader._get_existing_configuration(
            databases_configuration.mongodb,
            configuration_hash
        )

        current_configuration = ConfigurationLoader._process_configuration(plain_configuration, databases_configuration)

        ConfigurationLoader._configuration_cache[configuration_hash] = current_configuration

        if save:
            if existing_configuration is None:
                saved_configuration = {key: value for key, value in plain_configuration.items() if key != 'databases'}
                saved_configuration['hash'] = configuration_hash
                saved_configuration['file'] = basename(configuration_file_path)
                saved_configuration['date'] = datetime.utcnow()

                with MongoDB(databases_configuration.mongodb) as mongodb:
                    mongodb.insert_document(MongoDB.COLLECTION_NAME_CONFIGURATION, saved_configuration)
            else:
                with MongoDB(databases_configuration.mongodb) as mongodb:
                    mongodb.update_one(
                        MongoDB.COLLECTION_NAME_CONFIGURATION,
                        existing_configuration['_id'],
                        {'date': datetime.utcnow()}
                    )

        return current_configuration

    @staticmethod
    def load_by_config_folder(configuration_folder_path: str = None, save: bool = True) -> Sequence[Configuration]:
        if configuration_folder_path is None:
            configuration_folder_path = realpath(dirname(realpath(__file__)) + '/../config')

        current_path = getcwd()
        chdir(configuration_folder_path)
        configuration_file_paths = glob('*.yaml')
        chdir(current_path)
        configurations = []

        for configuration_file in configuration_file_paths:
            configurations.append(
                ConfigurationLoader.load_by_file(configuration_folder_path + '/' + configuration_file, save)
            )

        return configurations

    @staticmethod
    def load_by_dict(plain_configuration: dict) -> Configuration:
        configuration_hash = sha256(plain_configuration)

        if configuration_hash in ConfigurationLoader._configuration_cache:
            return ConfigurationLoader._configuration_cache[configuration_hash]

        databases_configuration = ConfigurationLoader._process_configuration_databases(plain_configuration)
        current_configuration = ConfigurationLoader._process_configuration(plain_configuration, databases_configuration)

        ConfigurationLoader._configuration_cache[configuration_hash] = current_configuration

        return current_configuration

    @staticmethod
    def _process_configuration(plain_configuration: dict, database_configuration: ConfigurationDatabases):
        try:
            configuration_hash = sha256(
                {key: value for key, value in plain_configuration.items() if key not in ['_id']}
            )

            return Configuration(
                database_configuration,
                ConfigurationLoader._process_configuration_urlsets(plain_configuration),
                ConfigurationLoader._process_configuration_aggregations(plain_configuration),
                ConfigurationLoader._process_configuration_operations(plain_configuration),
                configuration_hash
            )
        except ConfigurationMissingError as error:
            raise ExitError('Missing configuration "' + error.message + '"')
        except ConfigurationInvalidError as error:
            raise ExitError(error.message + ' in configuration')

    @staticmethod
    def _get_existing_configuration(mongodb_configuration: ConfigurationMongoDB, configuration_hash: str):
        existing_configuration = None

        with MongoDB(mongodb_configuration) as mongodb:
            if mongodb.has_collection(MongoDB.COLLECTION_NAME_CONFIGURATION):
                existing_configuration = mongodb.find_one(
                    MongoDB.COLLECTION_NAME_CONFIGURATION,
                    {'hash': configuration_hash},
                    True
                )

        return existing_configuration

    @staticmethod
    def _process_configuration_databases(plain_configuration: dict) -> ConfigurationDatabases:
        key = 'databases'

        if key in plain_configuration and type(plain_configuration[key]) is dict:
            configuration_bigquery = None
            configuration_mysql = None

            if 'bigquery' in plain_configuration[key]:
                configuration_bigquery = ConfigurationLoader._process_configuration_bigquery(plain_configuration)

            if 'orm' in plain_configuration[key]:
                configuration_mysql = ConfigurationLoader._process_configuration_ormdatabase(plain_configuration)

            return ConfigurationDatabases(
                ConfigurationLoader._process_configuration_mongodb(plain_configuration),
                configuration_mysql,
                configuration_bigquery
            )
        else:
            raise ConfigurationMissingError(key)

    @staticmethod
    def _process_configuration_ormdatabase(plain_configuration: dict) -> ConfigurationORM:
        key = 'databases'
        subkey = 'orm'

        if subkey in plain_configuration[key] and type(plain_configuration[key][subkey]) is dict:
            if 'host' not in plain_configuration[key][subkey]:
                raise ConfigurationMissingError(key + ' -> ' + subkey + ' -> host')
            if 'port' not in plain_configuration[key][subkey]:
                raise ConfigurationMissingError(key + ' -> ' + subkey + ' -> port')
            if 'dbname' not in plain_configuration[key][subkey]:
                raise ConfigurationMissingError(key + ' -> ' + subkey + ' -> dbname')
            if 'username' not in plain_configuration[key][subkey]:
                raise ConfigurationMissingError(key + ' -> ' + subkey + ' -> username')
            if 'password' not in plain_configuration[key][subkey]:
                raise ConfigurationMissingError(key + ' -> ' + subkey + ' -> password')

            return ConfigurationORM(
                None,
                plain_configuration[key][subkey]['dbtype'],
                plain_configuration[key][subkey]['host'],
                plain_configuration[key][subkey]['port'],
                plain_configuration[key][subkey]['dbname'],
                plain_configuration[key][subkey]['username'],
                plain_configuration[key][subkey]['password'],
            )
        elif subkey in plain_configuration[key] and type(plain_configuration[key][subkey]) is str:
            return ConfigurationORM(plain_configuration[key][subkey])
        else:
            raise ConfigurationMissingError(key + ' -> ' + subkey)

    @staticmethod
    def _process_configuration_mongodb(plain_configuration: dict) -> ConfigurationMongoDB:
        key = 'databases'
        subkey = 'mongodb'

        if subkey in plain_configuration[key] and type(plain_configuration[key][subkey]) is dict:
            if 'host' not in plain_configuration[key][subkey]:
                raise ConfigurationMissingError(key + ' -> ' + subkey + ' -> host')
            if 'port' not in plain_configuration[key][subkey]:
                raise ConfigurationMissingError(key + ' -> ' + subkey + ' -> port')
            if 'dbname' not in plain_configuration[key][subkey]:
                raise ConfigurationMissingError(key + ' -> ' + subkey + ' -> dbname')
            if 'username' not in plain_configuration[key][subkey]:
                raise ConfigurationMissingError(key + ' -> ' + subkey + ' -> username')
            if 'password' not in plain_configuration[key][subkey]:
                raise ConfigurationMissingError(key + ' -> ' + subkey + ' -> password')

            return ConfigurationMongoDB(
                plain_configuration[key][subkey]['host'],
                plain_configuration[key][subkey]['port'],
                plain_configuration[key][subkey]['dbname'],
                plain_configuration[key][subkey]['username'],
                plain_configuration[key][subkey]['password'],
            )
        else:
            raise ConfigurationMissingError(key + ' -> ' + subkey)

    @staticmethod
    def _process_configuration_bigquery(plain_configuration: dict) -> ConfigurationBigQuery:
        key = 'databases'
        additionalDatasets = {}
        credentials = None

        if 'bigquery' in plain_configuration[key] and type(plain_configuration[key]['bigquery']) is dict:
            if 'project' not in plain_configuration[key]['bigquery']:
                raise ConfigurationMissingError(key + ' -> bigquery -> project')
            else:
                project = plain_configuration[key]['bigquery']['project']

            if 'dataset' not in plain_configuration[key]['bigquery'] and \
                    type(plain_configuration[key]['bigquery']['dataset']) is not dict:
                raise ConfigurationMissingError(key + ' -> bigquery -> dataset')
            else:
                dataset = ConfigurationLoader._process_configuration_bigquerydataset(
                    project,
                    plain_configuration[key]['bigquery']['dataset']
                )

            if 'additionalDatasets' in plain_configuration[key]['bigquery'] and \
                    type(plain_configuration[key]['bigquery']['additionalDatasets']) is dict:
                for additionalDataset, additionalDataset_configuration in \
                        plain_configuration[key]['bigquery']['additionalDatasets'].items():
                    additionalDataset_configuration['name'] = additionalDataset
                    additionalDatasets[additionalDataset] = ConfigurationLoader._process_configuration_bigquerydataset(
                        project,
                        additionalDataset_configuration
                    )

            if 'credentials' in plain_configuration[key]['bigquery']:
                credentials = abspath(plain_configuration[key]['bigquery']['credentials'])

            return ConfigurationBigQuery(project, dataset, additionalDatasets, credentials)
        else:
            raise ConfigurationMissingError(key + ' -> bigquery')

    @staticmethod
    def _process_configuration_bigquerydataset(
        project: str,
        dataset_plain_configuration: dict
    ) -> ConfigurationBigQueryDataset:
        description = None
        labels = {}

        if 'name' not in dataset_plain_configuration and type(dataset_plain_configuration['name']) is not str:
            raise ConfigurationMissingError('databases -> bigquery -> dataset -> name')
        else:
            name = dataset_plain_configuration['name']

        if 'location' not in dataset_plain_configuration and type(dataset_plain_configuration['location']) is not str:
            raise ConfigurationMissingError('databases -> bigquery -> dataset -> location')
        else:
            location = dataset_plain_configuration['location']

        if 'description' in dataset_plain_configuration:
            if type(dataset_plain_configuration['description']) is not str:
                raise ConfigurationMissingError('databases -> bigquery -> dataset -> description')
            else:
                description = dataset_plain_configuration['description']

        if 'labels' in dataset_plain_configuration:
            if type(dataset_plain_configuration['labels']) is not dict:
                raise ConfigurationMissingError('databases -> bigquery -> dataset -> labels')
            else:
                labels = dataset_plain_configuration['labels']

        return ConfigurationBigQueryDataset(project, name, location, description, labels)

    @staticmethod
    def _process_configuration_urlsets(plain_configuration: dict) -> ConfigurationUrlsets:
        if 'urlsets' in plain_configuration and type(plain_configuration['urlsets']) is dict:
            urlsets = plain_configuration['urlsets']
        else:
            raise ConfigurationMissingError('urlsets')

        configuration_urlsets = []

        for urlset_name, urls in urlsets.items():
            configuration_urls = []

            for url_config in urls:
                configuration_urls.append(ConfigurationLoader._process_configuration_url(urlset_name, url_config))

            configuration_urlsets.append(ConfigurationUrlset(urlset_name, configuration_urls))

        return ConfigurationUrlsets(configuration_urlsets)

    @staticmethod
    def _process_configuration_url(urlset_name: str, url_config) -> ConfigurationUrl:
        render = False

        if isinstance(url_config, str):
            url = url_config
        elif isinstance(url_config, dict):
            url = url_config['url'] if 'url' in url_config else ''
            render = True if 'render' in url_config and url_config['render'] is True else False
        else:
            raise ConfigurationInvalidError('Configuration is invalid: see "urlsets -> ' + urlset_name + '" section')

        if not Validator.validate_url(url):
            raise ConfigurationInvalidError('URL is invalid: "urlsets -> ' + urlset_name + ' -> ' + url + '"')

        return ConfigurationUrl(URL(url), render)

    @staticmethod
    def _process_configuration_aggregations(plain_configuration: dict) -> ConfigurationAggregations:
        key = 'aggregations'
        configuration_aggregations = {}

        if key in plain_configuration and type(plain_configuration[key]) is dict:
            for subkey in plain_configuration[key]:
                module = ConfigurationLoader._process_configuration_aggregation(plain_configuration[key], subkey)

                configuration_aggregations.update({subkey: module})

            return ConfigurationAggregations(configuration_aggregations)

        else:
            return ConfigurationAggregations()

    @staticmethod
    def _process_configuration_aggregation(configuration_aggregations: dict, key: str) -> ConfigurationAggregation:
        urlsets = []
        settings = {}
        name = None
        cron = None

        if key in configuration_aggregations and type(configuration_aggregations[key]) is dict:
            if 'urlsets' in configuration_aggregations[key] and type(
                configuration_aggregations[key]['urlsets']
            ) is list:
                urlsets = configuration_aggregations[key]['urlsets']

            if 'settings' in configuration_aggregations[key] and type(
                configuration_aggregations[key]['settings']
            ) is dict:
                settings = configuration_aggregations[key]['settings']

            if 'cron' in configuration_aggregations[key] and type(configuration_aggregations[key]['cron']) is str:
                cron = configuration_aggregations[key]['cron']

            if 'database' in configuration_aggregations[key] and \
                    type(configuration_aggregations[key]['database']) is str:
                database = configuration_aggregations[key]['database']

            name = key

        if name is None:
            raise ConfigurationInvalidError('Invalid aggregation configuration')

        if cron is None:
            raise ConfigurationMissingError('Missing cron command for "' + name + '"')

        return ConfigurationAggregation(name, cron, urlsets, settings)

    @staticmethod
    def _process_configuration_operations(plain_configuration: dict) -> ConfigurationOperations:
        key = 'operations'
        configuration_operations = {}

        if key in plain_configuration and type(plain_configuration[key]) is dict:
            for subkey in plain_configuration[key]:
                module = ConfigurationLoader._process_configuration_operation(plain_configuration[key], subkey)

                configuration_operations.update({subkey: module})

            return ConfigurationOperations(configuration_operations)

        else:
            return ConfigurationOperations()

    @staticmethod
    def _process_configuration_operation(configuration_operations: dict, key: str) -> ConfigurationOperation:
        urlsets = []
        checks = {}
        database = 'orm'
        domains = []
        cron = None

        if key in configuration_operations and type(configuration_operations[key]) is dict:
            if 'urlsets' in configuration_operations[key] and type(configuration_operations[key]['urlsets']) is list:
                urlsets = configuration_operations[key]['urlsets']

            if 'checks' in configuration_operations[key] and type(configuration_operations[key]['checks']) is dict:
                checks = configuration_operations[key]['checks']

            if 'database' in configuration_operations[key] and type(configuration_operations[key]['database']) is str:
                database = configuration_operations[key]['database']

            if 'cron' in configuration_operations[key] and type(configuration_operations[key]['cron']) is str:
                cron = configuration_operations[key]['cron']
            else:
                raise ConfigurationMissingError('Missing cron command for "' + key + '"')

            if 'bigquery' != database and 'orm' != database:
                raise ConfigurationInvalidError('invalid database "' + database + '" for operation module')

            if 'domains' in configuration_operations[key] and type(configuration_operations[key]['domains']) is list:
                domains = configuration_operations[key]['domains']

        return ConfigurationOperation(key, cron, urlsets, checks, database, domains)
