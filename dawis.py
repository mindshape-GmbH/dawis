from database.connection import Connection
from utilities.configuration_loader import ConfigurationLoader
from utilities.path import Path
from modules.runner import run
from celery import Celery
from celery.schedules import crontab
from croniter import croniter
from os import environ
import pickle

timezone = environ.get('CELERY_TIMEZONE', 'UTC')
redis = 'redis://{0}:{1}'.format(environ.get('REDIS_HOST', '127.0.0.1'), environ.get('REDIS_PORT', '6379'))

app = Celery('dawis', backend=redis, broker=redis)
app.conf.timezone = timezone

configurations = ConfigurationLoader().load_by_config_folder()

for configuration in configurations:
    with Connection(configuration) as connection:
        if connection.has_orm():
            connection.orm.tables.create_tables()

        if connection.has_mongodb():
            connection.mongodb.migrations()

    with open(Path.var_folder_path() + '/' + configuration.hash + '.pickle', 'wb') as handle:
        pickle.dump(configuration, handle, protocol=pickle.HIGHEST_PROTOCOL)

    for aggregationModule in configuration.aggregations.config.values():
        module = aggregationModule.name
        cron = aggregationModule.cron

        app.autodiscover_tasks(['modules.aggregation.custom'], module)

        if croniter.is_valid(cron) is True:
            cron = str.split(cron, sep=' ')
            app.add_periodic_task(crontab(*cron), run, [configuration.hash, module, 'modules.aggregation.custom'])

    for operationModule in configuration.operations.config.values():
        module = operationModule.name
        cron = operationModule.cron

        app.autodiscover_tasks(['modules.operation.custom'], module)

        if croniter.is_valid(cron) is True:
            cron = str.split(cron, sep=' ')
            app.add_periodic_task(crontab(*cron), run, [configuration.hash, module, 'modules.operation.custom'])
