class Error(Exception):
    pass


class ExitError(Error):
    def __init__(self, message: str, return_code=1):
        self.message = message
        self.return_code = return_code


class ConfigurationMissingError(Error):
    def __init__(self, message: str):
        self.message = message


class ConfigurationInvalidError(Error):
    def __init__(self, message: str):
        self.message = message


class InvalidResultTypeError(Error):
    def __init__(self, message: str):
        self.message = message


class TableDoesNotExistError(Error):
    def __init__(self, message: str):
        self.message = message


class NoConnectionError(Error):
    def __init__(self, message: str):
        self.message = message
