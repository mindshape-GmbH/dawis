from database.connection import Connection
from utilities.configuration_loader import ConfigurationLoader
from utilities.path import Path
from modules.runner import run
from celery import Celery
from celery.schedules import crontab
from croniter import croniter
from os import environ
import pickle

redis = 'redis://{0}:{1}/{2}'.format(
    environ.get('REDIS_HOST', '127.0.0.1'),
    environ.get('REDIS_PORT', '6379'),
    environ.get('REDIS_DATABASE', '0')
)

app = Celery(
    'dawis-' + environ.get('CELERY_PROJECT', 'project'),
    backend=redis,
    broker=redis,
    broker_connection_retry_on_startup=True
)
app.conf.timezone = environ.get('CELERY_TIMEZONE', 'UTC')


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    configurations = ConfigurationLoader().load_by_config_folder()

    for configuration in configurations:
        with Connection(configuration) as connection:
            if connection.has_orm():
                connection.orm.tables.create_tables()

            if connection.has_mongodb():
                connection.mongodb.migrations()

        with open(Path.var_folder_path() + '/' + configuration.hash + '.pickle', 'wb') as handle:
            pickle.dump(configuration, handle, protocol=pickle.HIGHEST_PROTOCOL)

        for configuration_key, aggregationModule in configuration.aggregations.config.items():
            module = aggregationModule.module
            cron = aggregationModule.cron

            if croniter.is_valid(cron) is True:
                (minute, hour, day_month, month, day_week) = str.split(cron, sep=' ')
                sender.add_periodic_task(
                    crontab(minute, hour, day_week, day_month, month),
                    run_runner.s(configuration.hash, configuration_key, module, 'modules.aggregation.custom'),
                    time_limit=aggregationModule.runtime_limit,
                    name='aggregation_' + configuration_key
                )

        for configuration_key, operationModule in configuration.operations.config.items():
            module = operationModule.module
            cron = operationModule.cron

            if croniter.is_valid(cron) is True:
                (minute, hour, day_month, month, day_week) = str.split(cron, sep=' ')
                sender.add_periodic_task(
                    crontab(minute, hour, day_week, day_month, month),
                    run_runner.s(configuration.hash, configuration_key, module, 'modules.operation.custom'),
                    time_limit=operationModule.runtime_limit,
                    name='operation_' + configuration_key
                )


@app.task
def run_runner(configuration_hash: str, configuration_key: str, module: str, module_namespace: str):
    run(configuration_hash, configuration_key, module, module_namespace)


if __name__ == '__main__':
    app.start()
