from utilities.configuration_loader import ConfigurationLoader
from utilities.exceptions import ConfigurationMissingError
from utilities.exceptions import ConfigurationInvalidError
from sys import argv
from os.path import isfile

try:
    arguments = argv[1:]

    if 0 == arguments.__len__():
        print('pass a configuration file as argument')
        exit(1)

    configuration_file = arguments[0]

    if not isfile(configuration_file):
        print('configuration file "' + configuration_file + '" does not exist')
        exit(1)

    ConfigurationLoader.load_by_file(configuration_file, False)
except ConfigurationMissingError as missing_exception:
    print('configuration is missing: "' + missing_exception.message + '"')
    exit(1)
except ConfigurationInvalidError as invalid_exception:
    print('configuration is invalid: "' + invalid_exception.message + '"')
    exit(1)
