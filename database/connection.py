from database.mongodb import MongoDB
from database.orm import ORM
from database.bigquery import BigQuery
from utilities.configuration import Configuration
from utilities.configuration import ConfigurationMongoDB
from utilities.configuration import ConfigurationORM
from utilities.configuration import ConfigurationBigQuery
from utilities.exceptions import NoConnectionError


class Connection:
    def __init__(self, configuration: Configuration):
        self._mongodb_configuration = None
        self._orm_configuration = None
        self._bigquery_configuration = None
        self._configuration = configuration
        self._instances = []

        if type(configuration.databases.mongodb) is ConfigurationMongoDB:
            self._mongodb_configuration = configuration.databases.mongodb

        if type(configuration.databases.orm) is ConfigurationORM:
            self._orm_configuration = configuration.databases.orm

        if type(configuration.databases.bigquery) is ConfigurationBigQuery:
            self._bigquery_configuration = configuration.databases.bigquery

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        for instance in self._instances:
            instance.close()

    def has_mongodb(self) -> bool:
        return type(self._mongodb_configuration) is ConfigurationMongoDB

    @property
    def mongodb(self) -> MongoDB:
        if not self.has_mongodb():
            raise NoConnectionError('No MongoDB configuration')

        mongodb = MongoDB(self._mongodb_configuration)
        mongodb.connect()

        self._instances.append(mongodb)

        return mongodb

    def has_orm(self) -> bool:
        return type(self._orm_configuration) is ConfigurationORM

    @property
    def orm(self) -> ORM:
        if not self.has_orm():
            raise NoConnectionError('No ORM configuration')

        orm = ORM(self._configuration)
        orm.connect()

        self._instances.append(orm)

        return orm

    def has_bigquery(self) -> bool:
        return type(self._bigquery_configuration) is ConfigurationBigQuery

    @property
    def bigquery(self) -> BigQuery:
        if not self.has_bigquery():
            raise NoConnectionError('No BigQuery configuration')

        bigquery = BigQuery(self._configuration)
        bigquery.connect()

        self._instances.append(bigquery)

        return bigquery
