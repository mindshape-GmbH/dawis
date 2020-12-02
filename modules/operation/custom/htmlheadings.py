from database.connection import Connection
from service.check import Check
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError
from modules.aggregation.custom.html_parser import HtmlParser
from bs4 import BeautifulSoup


class Htmlheadings:
    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        if not connection.has_bigquery() and not connection.has_orm():
            raise ConfigurationMissingError('Missing a database configuration for this operation')

        self.configuration = configuration
        self.module_configuration = configuration.operations.get_custom_configuration_operation(configuration_key)
        self.mongodb = connection.mongodb
        self.check_service = Check(connection)

    def run(self):
        if len(self.module_configuration.urlsets) > 0:
            print('Running operation htmlheadings:', "\n")

            if not self.mongodb.has_collection(HtmlParser.COLLECTION_NAME):
                return

            for urlset in self.module_configuration.urlsets:
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

                        self.check_count_headline_h1(data, urlset_config)

                        self.mongodb.update_one(
                            HtmlParser.COLLECTION_NAME,
                            data['_id'],
                            {'processed_htmlheadings': True}
                        )

                print("\n")

    def check_count_headline_h1(self, data: dict, urlset_config: dict):
        if 'count_headline_h1' in urlset_config:
            assert_val = urlset_config['count_headline_h1']

            print('      -> check_count_headline_h1 "' + str(assert_val) + '"', end='')

            valid = False
            error = ''

            doc = BeautifulSoup(data['body'], "html.parser")
            count_headline = 0

            for headline in doc.select("h1"):
                count_headline += 1

            if count_headline == assert_val:
                valid = True

            if count_headline > 1 and not valid:
                error = 'more than one headline detected'

            url = data['url']

            self.check_service.add_check(
                self.module_configuration.database,
                data['urlset'],
                'htmlheadings-count_headline_h1',
                str(count_headline),
                valid,
                '',
                error,
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))
