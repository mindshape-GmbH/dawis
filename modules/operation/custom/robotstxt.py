from database.connection import Connection
from modules.aggregation.custom.robotstxt import Robotstxt as AggregationRobotstxt
from service.check import Check
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError
from modules.aggregation.custom.robotstxt import Robotstxt as RobotstxtAggregationModule
import urllib.robotparser
import requests


class Robotstxt:
    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        if not connection.has_bigquery() and not connection.has_orm():
            raise ConfigurationMissingError('Missing a database configuration for this operation')

        self.configuration = configuration
        self.module_configuration = configuration.operations.get_custom_configuration_operation(configuration_key)
        self.mongodb = connection.mongodb
        self.check_service = Check(connection)
        self.robotsparser = urllib.robotparser.RobotFileParser()

    def run(self):
        if len(self.module_configuration.urlsets) > 0:
            print('Running operation robotstxt:', "\n")

            if not self.mongodb.has_collection(AggregationRobotstxt.COLLECTION_NAME):
                return

            for urlset in self.module_configuration.urlsets:
                for single_urlset in urlset:
                    urlset_name = urlset[single_urlset]

                    robotstxts = self.mongodb.find(
                        RobotstxtAggregationModule.COLLECTION_NAME,
                        {
                            'urlset': urlset_name,
                            'processed_robotstxt': {'$exists': False}
                        }
                    )

                    urlset_config = urlset['checks']

                    for url in self.configuration.urlsets.urlset_urls(urlset_name):
                        urlstr = str(url)
                        if not urlstr.endswith('/robots.txt'):
                            url = url.protocol + '://' + url.domain + str.rstrip(url.path, '/') + '/robots.txt'
                        for robotstxt in robotstxts:
                            if str(robotstxt['url']) == str(url):

                                print(' + ' + str(robotstxt['url']))

                                self.check_status_code(robotstxt, urlset_config)
                                self.check_has_sitemap_xml(robotstxt, urlset_config)

                                self.mongodb.update_one(
                                    RobotstxtAggregationModule.COLLECTION_NAME,
                                    robotstxt['_id'],
                                    {'processed_robotstxt': True}
                                )

                            print("\n")

    def request_url_statuscode(self, url):

        try:
            headers = {
                'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36'
            }

            response = requests.get(url, headers=headers)
            status_code = response.status_code

        except requests.RequestException as error:
            status_code = None

        return status_code

    def check_status_code(self, robotstxt: dict, urlset_config: dict):
        if 'status_code' in urlset_config:
            assert_val = urlset_config['status_code']

            print('      -> check_status_code "' + str(assert_val) + '"', end='')

            valid = False

            if 'status_code' in robotstxt:
                if robotstxt['status_code'] == assert_val:
                    valid = True

            url = robotstxt['url']

            self.check_service.add_check(
                self.module_configuration.database,
                robotstxt['urlset'],
                'robotstxt-status_code',
                robotstxt['body'],
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

    def check_has_sitemap_xml(self, robotstxt: dict, urlset_config: dict):
        if 'has_sitemap_xml' in urlset_config:
            assert_val_has_sitemap = urlset_config['has_sitemap_xml']

            has_sitemap = False

            if 'body' in robotstxt:
                robotsbody = robotstxt['body']
                self.robotsparser.parse(robotsbody.splitlines())

                sitemaps = self.robotsparser.site_maps()
                if sitemaps:
                    has_sitemap = True

                valid = False
                if has_sitemap == assert_val_has_sitemap:
                    valid = True

                url = robotstxt['url']

                self.check_service.add_check(
                    self.module_configuration.database,
                    robotstxt['urlset'],
                    'robotstxt-has_sitemap_xml',
                    str(url),
                    valid,
                    '',
                    '',
                    url.protocol,
                    url.domain,
                    url.path,
                    url.query,
                )

                if sitemaps:
                    for sitemap in sitemaps:

                        error = ''
                        sitemap_200 = False

                        try:
                            headers = {
                                'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.87 Safari/537.36'
                            }

                            response = requests.get(sitemap, headers=headers)
                            status_code = response.status_code
                        except requests.RequestException as err:
                            status_code = None

                        if status_code == 200:
                            sitemap_200 = True

                        if not sitemap_200:
                            error = 'No access to sitemap'

                        self.check_service.add_check(
                            self.module_configuration.database,
                            robotstxt['urlset'],
                            'robotstxt-sitemap_access',
                            sitemap,
                            sitemap_200,
                            '',
                            error,
                            url.protocol,
                            url.domain,
                            url.path,
                            url.query,
                        )
