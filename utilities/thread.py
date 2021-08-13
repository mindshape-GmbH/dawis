from threading import Thread


class ResultThread(Thread):
    def __init__(self, function: callable, arguments: list, data: dict = None):
        super().__init__()

        if data is None:
            data = {}

        self._function = function
        self._arguments = arguments
        self._data = data
        self.result = None
        self.exception = None

    def get_arguements(self) -> list:
        return self._arguments

    def get_data(self, key: str = None):
        if key is not None:
            return self._data[key]

        return self._data

    def run(self) -> None:
        try:
            self.result = self._function(*self._arguments)
        except Exception as error:
            self.exception = error
