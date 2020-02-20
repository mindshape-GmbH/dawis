from sqlalchemy.engine.base import Connection
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import Boolean
from sqlalchemy import DateTime
from database.orm.dialects.types import UnsignedInt
from utilities.configuration import Configuration
from utilities.exceptions import TableDoesNotExistError


class Tables:
    def __init__(self, connection: Connection, configuration: Configuration):
        self.connection = connection
        self.configuration = configuration
        self.tables = {}
        self._init_tables()

    @staticmethod
    def urlset_tablename(urlset: str):
        return 'urls_' + urlset

    @staticmethod
    def checks_tablename(urlset: str):
        return 'checks_' + urlset

    def _init_tables(self):
        metadata = MetaData(bind=self.connection)

        for configuration_urlset in self.configuration.urlsets.urlsets:
            urlset_urls = Table(
                self.urlset_tablename(configuration_urlset.name),
                metadata,
                Column('id', UnsignedInt, primary_key=True, autoincrement=True, nullable=False),
                Column('protocol', String(8), nullable=False, default=''),
                Column('domain', String(255), nullable=False, default=''),
                Column('path', String(2048), nullable=False, default=''),
                Column('query', String(2048), nullable=False, default=''),
            )

            self.tables[self.urlset_tablename(configuration_urlset.name)] = urlset_urls

            urlset_checks = Table(
                self.checks_tablename(configuration_urlset.name),
                metadata,
                Column('id', UnsignedInt, primary_key=True, autoincrement=True, nullable=False),
                Column('created', DateTime, nullable=False),
                Column('last_checked', DateTime, nullable=False),
                Column('url', UnsignedInt, ForeignKey(urlset_urls.columns.id, ondelete='RESTRICT'), nullable=False),
                Column('check', String(255), nullable=False, default=''),
                Column('value', String(255), nullable=False, default=''),
                Column('valid', Boolean, nullable=False),
                Column('diff', Text),
                Column('error', String(127), nullable=False, default=''),
            )

            self.tables[self.checks_tablename(configuration_urlset.name)] = urlset_checks

    def create_tables(self):
        for table_name, table in self.tables.items():
            table.create(self.connection, checkfirst=True)

    def table(self, table_name) -> Table:
        if table_name in self.tables:
            return self.tables[table_name]

        raise TableDoesNotExistError('The table "' + table_name + '" does not exist')

    def table_urlset_urls(self, urlset_name) -> Table:
        return self.table(self.urlset_tablename(urlset_name))

    def table_urlset_checks(self, urlset_name) -> Table:
        return self.table(self.checks_tablename(urlset_name))
