from database.connection import Connection
from database.bigquery import BigQuery
from database.orm import ORM
from database.orm.tables.checks_urlset import ChecksUrlset
from database.orm.tables.urls_urlset import UrlsUrlset
from utilities.exceptions import ConfigurationMissingError


class Check:
    def __init__(self, connection: Connection):
        self._connection = connection
        self._orm = None
        self._bigquery = None
        self._urlset_checks_table = None
        self._urlset_urls_table = None

        if connection.has_orm():
            self._orm = self._connection.orm
            self._urlset_checks_table = ChecksUrlset(self._orm)
            self._urlset_urls_table = UrlsUrlset(self._orm)

        if connection.has_bigquery():
            self._bigquery = self._connection.bigquery

        self._cached_url_ids = {}

    def add_check(
            self,
            database: str,
            urlset: str,
            check: str,
            value: str,
            valid: bool,
            diff: str,
            error: str,
            url_protocol: str,
            url_domain: str,
            url_path: str,
            url_query: str
    ):
        if 'bigquery' == database:
            if type(self._bigquery) is not BigQuery:
                raise ConfigurationMissingError('Missing a bigquery connection')

            self._bigquery.add_check(urlset, check, str(value), valid, diff, error, url_protocol, url_domain, url_path, url_query)
        else:
            if type(self._orm) is not ORM:
                raise ConfigurationMissingError('Missing a orm connection')

            url = url_protocol + '://' + url_domain + url_path + url_query

            if url in self._cached_url_ids:
                url_id = self._cached_url_ids[url]
            else:
                url_id = self._urlset_urls_table.add(urlset, url_protocol, url_domain, url_path, url_query)

            self._urlset_checks_table.add(urlset, url_id, check, valid, value, diff, error)
