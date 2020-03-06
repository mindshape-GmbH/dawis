from database.connection import Connection
from utilities.configuration import Configuration
from utilities.exceptions import ExitError
from utilities.path import Path
from celery import task
import tocamelcase
import importlib
import pickle


@task()
def run(configuration_hash: str, module: str, module_namespace: str):
    with open(Path.var_folder_path() + '/' + configuration_hash + '.pickle', 'rb') as handle:
        configuration = pickle.load(handle)

    if type(configuration) is not Configuration:
        raise ExitError('Could not unserialize configuration')

    custommodule = importlib.import_module('.' + module, package=module_namespace)
    connection = Connection(configuration)

    for customattribute in dir(custommodule):
        if customattribute == tocamelcase.convert(module):
            customclass = getattr(custommodule, customattribute)
            customclass(configuration, connection).run()

    connection.close()
