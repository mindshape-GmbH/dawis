from database.connection import Connection
from utilities.configuration_loader import ConfigurationLoader
from utilities.path import Path
from modules.runner import run
import pickle

configurations = ConfigurationLoader.load_by_config_folder(None, False)

for configuration in configurations:
    with Connection(configuration) as connection:
        if connection.has_orm():
            connection.orm.tables.create_tables()

        if connection.has_mongodb():
            connection.mongodb.migrations()

    with open(Path.var_folder_path() + '/' + configuration.hash + '.pickle', 'wb') as handle:
        pickle.dump(configuration, handle, protocol=pickle.HIGHEST_PROTOCOL)

    for configuration_key, aggregationModule in configuration.aggregations.config.items():
        run(configuration.hash, configuration_key, aggregationModule.module, 'modules.aggregation.custom')

    for configuration_key, operationModule in configuration.operations.config.items():
        run(configuration.hash, configuration_key, operationModule.module, 'modules.operation.custom')
