from datetime import date
from inspect import getmembers
import requests


class ApiError(Exception):
    def __init__(self, message: str):
        self.message = message


class Client:
    # https://www.rankalyst.de/index/apiDocs
    API_URL = 'https://www.rankalyst.de/API'

    ACTION_PROJECTS = 'projects'
    ACTION_PROJECT_KEYWORD_RANKING = 'project_keyword_rankings'
    ACTION_PROJECT_KEYWORD_HISTORY = 'project_keyword_history'

    def __init__(self, api_key: str, username: str):
        self._api_key = api_key
        self._username = username

    def request(self, action: str, parameters: dict = None) -> dict:
        if 1 != len(list(filter(lambda x: x[0].startswith('ACTION_') and x[1] == action, getmembers(Client)))):
            raise ApiError('The action "{:s}" is not supported'.format(action))

        request_url = self.API_URL + '?api_key=' + self._api_key + '&username=' + self._username + '&action=' + action

        if type(parameters) is dict:
            for parameter, value in parameters.items():
                if type(value) is bool:
                    value = '1' if value else '0'
                if type(value) is date:
                    value = '{:%Y-%m-%d}'.format(value)
                if type(value) is not str:
                    value = str(value)

                request_url += '&' + parameter + '=' + value

        response = requests.get(request_url)

        if 200 != response.status_code:
            raise ApiError(
                str(response.status_code) +
                ' ' +
                response.reason +
                ' - see: https://www.rankalyst.de/index/apiDocs'
            )

        try:
            response_data = response.json()
        except ValueError:
            raise ApiError('Error in the JSON response')

        if 'fail' == response_data['status']:
            error_messages = []

            for message in response_data['messages']:
                error_messages.append(message)

            error_messages.append('info: https://www.rankalyst.de/index/apiDocs')

            raise ApiError('\n'.join(error_messages))

        return response_data
