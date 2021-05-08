from database.orm.tables import Tables
from utilities.configuration import Configuration
from utilities.configuration import ConfigurationORM
from utilities.exceptions import ConfigurationMissingError
from sqlalchemy import create_engine
from sqlalchemy.engine.result import Result
from sqlalchemy_utils import database_exists, create_database


class ORM:
    def __init__(self, configuration: Configuration):
        self._engine = None
        self._connection = None
        self._configuration = configuration
        self.tables = None
        self._connected = False

        if type(configuration.databases.orm) is ConfigurationORM:
            self._engine = create_engine(configuration.databases.orm.connection_url)
        else:
            raise ConfigurationMissingError('No orm database connection configured')

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def connect(self):
        if not database_exists(self._engine.url):
            create_database(self._engine.url)

        self._connection = self._engine.connect()
        self.tables = Tables(self._connection, self._configuration)
        self._connected = True

    def close(self):
        self._connection.close()
        self._connected = False

    def is_connected(self):
        return self._connected

    def execute(self, statement) -> Result:
        return self._connection.execute(statement)
