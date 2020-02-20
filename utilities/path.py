from os.path import expanduser
from os.path import dirname
from os.path import isfile
from os.path import isdir
from os.path import realpath
from os import makedirs


class Path:

    @classmethod
    def parse_path(cls, path):
        path = expanduser(path)
        path = realpath(path)

        return path

    @classmethod
    def dir(cls, path):
        path = cls.parse_path(path)
        path = dirname(path)

        return path

    @classmethod
    def file_exist(cls, path):
        path = cls.parse_path(path)

        return isfile(path)

    @classmethod
    def var_folder_path(cls) -> str:
        var_folder = realpath(dirname(realpath(__file__)) + '/../var')

        if not isdir(var_folder):
            makedirs(var_folder)

        return var_folder
