from database.connection import Connection
from service.check import Check
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError
from modules.aggregation.custom.crawler import Crawler
from bs4 import BeautifulSoup


class Htmlheadings:
    def __init__(self, configuration: Configuration, connection: Connection):
        if not connection.has_bigquery() and not connection.has_orm():
            raise ConfigurationMissingError('Missing a database configuration for this operation')

        self.configuration = configuration
        self.mongodb = connection.mongodb
        self.check_service = Check(connection)
        self.htmlheadings_config = self.configuration.operations.get_custom_configuration_operation('htmlheadings')

    def run(self):
        if len(self.htmlheadings_config.urlsets) > 0:
            print('Running operation htmlheadings:', "\n")

            if not self.mongodb.has_collection(Crawler.COLLECTION_NAME):
                return

            for urlset in self.htmlheadings_config.urlsets:
                print(' - "' + urlset['url'] + '":')

                for single_urlset in urlset:
                    urlset_name = urlset[single_urlset]

                    crawls = self.mongodb.find(
                        Crawler.COLLECTION_NAME,
                        {
                            'urlset': urlset_name,
                            'processed_htmlheadings': {'$exists': False}
                        }
                    )

                    urlset_config = urlset['checks']

                    for crawl in crawls:
                        print('   + ' + str(crawl['url']))

                        self.check_count_headline_h1(crawl, urlset_config)

                        self.mongodb.update_one(
                            Crawler.COLLECTION_NAME,
                            crawl['_id'],
                            {'processed_htmlheadings': True}
                        )

                print("\n")

    def check_count_headline_h1(self, crawl: dict, urlset_config: dict):
        if 'count_headline_h1' in urlset_config:
            assert_val = urlset_config['count_headline_h1']

            print('      -> check_count_headline_h1 "' + str(assert_val) + '"', end='')

            valid = False
            error = ''

            doc = BeautifulSoup(crawl['body'], "html.parser")
            count_headline = 0

            for headline in doc.select("h1"):
                count_headline += 1

            if count_headline == assert_val:
                valid = True

            if count_headline > 1 and not valid:
                error = 'more than one headline detected'

            url = crawl['url']

            self.check_service.add_check(
                self.htmlheadings_config.database,
                crawl['urlset'],
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
