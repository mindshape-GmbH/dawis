from threading import Thread


class ResultThread(Thread):
    def __init__(self, function: callable, arguments: list):
        super().__init__()
        self._function = function
        self._arguments = arguments
        self.result = None
        self.exception = None

    def get_arguements(self):
        return self._arguments

    def run(self) -> None:
        try:
            self.result = self._function(*self._arguments)
        except Exception as error:
            self.exception = error
