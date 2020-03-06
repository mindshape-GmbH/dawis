from database.connection import Connection
from service.check import Check
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError
from modules.aggregation.custom.html_parser import HtmlParser


class Responseheader:
    def __init__(self, configuration: Configuration, connection: Connection):
        if not connection.has_bigquery() and not connection.has_orm():
            raise ConfigurationMissingError('Missing a database configuration for this operation')

        self.configuration = configuration
        self.mongodb = connection.mongodb
        self.check_service = Check(connection)
        self.responseheader_config = self.configuration.operations.get_custom_configuration_operation('responseheader')

    def run(self):
        if len(self.responseheader_config.urlsets) > 0:
            print('Running operation responseheader:', "\n")

            if not self.mongodb.has_collection(HtmlParser.COLLECTION_NAME):
                return

            for urlset in self.responseheader_config.urlsets:
                print(' - "' + str(urlset['url']) + '":')

                for single_urlset in urlset:
                    urlset_name = urlset[single_urlset]

                    parsed_data = self.mongodb.find(
                        HtmlParser.COLLECTION_NAME,
                        {
                            'urlset': urlset_name,
                            'processed_htmlheadings': {'$exists': False}
                        }
                    )

                    urlset_config = urlset['checks']

                    for data in parsed_data:
                        print('   + ' + str(data['url']))

                        self.check_status_code(data, urlset_config)
                        self.check_content_encoding(data, urlset_config)
                        self.check_cache_control(data, urlset_config)
                        self.check_expires(data, urlset_config)
                        self.check_x_canonical(data, urlset_config)
                        self.check_no_index(data, urlset_config)

                        self.mongodb.update_one(
                            HtmlParser.COLLECTION_NAME,
                            data['_id'],
                            {'processed_responseheader': True}
                        )

                print("\n")

    def check_status_code(self, data: dict, urlset_config: dict):
        if 'status_code' in urlset_config:
            assert_val = urlset_config['status_code']['assert']

            print('      -> check_status_code "' + str(assert_val) + '"', end='')

            valid = False
            if data['status_code'] == assert_val:
                valid = True

            url = data['url']

            self.check_service.add_check(
                self.responseheader_config.database,
                data['urlset'],
                'responseheader-status_code',
                '',
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_content_encoding(self, data: dict, urlset_config: dict):
        if 'content_encoding' in urlset_config:
            assert_val = urlset_config['content_encoding']['assert']
            # transform all headers (key,values) to lowercase
            headers = dict((k.lower(), v.lower()) for k, v in data['headers'].items())

            print('      -> check_content_encoding "' + str(assert_val) + '"', end='')

            valid = False
            if 'content-encoding' in headers:
                if headers['content-encoding'] == assert_val:
                    valid = True

            url = data['url']

            self.check_service.add_check(
                self.responseheader_config.database,
                data['urlset'],
                'responseheader-content_encoding',
                '',
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_cache_control(self, data: dict, urlset_config: dict):
        if 'cache_control' in urlset_config:
            assert_val = urlset_config['cache_control']['assert']
            # transform all headers (key,values) to lowercase
            headers = dict((k.lower(), v.lower()) for k, v in data['headers'].items())

            print('      -> check_cache_control "' + str(assert_val) + '"', end='')

            valid = False
            if 'cache-control' in headers:
                if headers['cache-control'] == assert_val:
                    valid = True

            url = data['url']

            self.check_service.add_check(
                self.responseheader_config.database,
                data['urlset'],
                'responseheader-cache_control',
                '',
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_expires(self, data: dict, urlset_config: dict):
        if 'expires' in urlset_config:
            assert_val = urlset_config['expires']['assert']
            # transform all headers (key,values) to lowercase
            headers = dict((k.lower(), v.lower()) for k, v in data['headers'].items())

            print('      -> check_expires "' + str(assert_val) + '"', end='')

            valid = False
            if 'expires' in headers:
                if headers['expires'] == assert_val:
                    valid = True

            url = data['url']

            self.check_service.add_check(
                self.responseheader_config.database,
                data['urlset'],
                'responseheader-expires',
                '',
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_x_canonical(self, data: dict, urlset_config: dict):
        if 'x_canonical' in urlset_config:
            assert_val = urlset_config['x_canonical']['assert']
            # transform all headers (key,values) to lowercase
            headers = dict((k.lower(), v.lower()) for k, v in data['headers'].items())

            print('      -> check_x_canonical "' + str(assert_val) + '"', end='')

            valid = False
            if 'x-canonical' in headers:
                if headers['x-canonical'] == assert_val:
                    valid = True

            url = data['url']

            self.check_service.add_check(
                self.responseheader_config.database,
                data['urlset'],
                'responseheader-x_canonical',
                '',
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_no_index(self, data: dict, urlset_config: dict):
        if 'no_index' in urlset_config:
            assert_val = urlset_config['no_index']['assert']
            # transform all headers (key,values) to lowercase
            headers = dict((k.lower(), v.lower()) for k, v in data['headers'].items())

            print('      -> check_no_index "' + str(assert_val) + '"', end='')

            valid = False
            if 'no-index' in headers:
                if headers['no-index'] == assert_val:
                    valid = True

            url = data['url']

            self.check_service.add_check(
                self.responseheader_config.database,
                data['urlset'],
                'responseheader-no_index',
                '',
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))
