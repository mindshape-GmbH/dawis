from utilities import Validator
from datetime import date
from inspect import getmembers
from urllib.parse import quote
import json
import requests


class ApiError(Exception):
    def __init__(self, message: str):
        self.message = message


class Client:
    # https://developers.wrike.com/
    API_URL = 'https://{host}/api/v4'
    API_HOST_GLOBAL = 'www.wrike.com'
    API_HOST_EU = 'app-eu.wrike.com'
    SHARE_URL = 'https://{host}/open.htm?id={share_id}'

    def __init__(self, api_token: str, api_host: str = API_HOST_GLOBAL):
        if 1 != len(list(filter(lambda x: x[0].startswith('API_HOST_') and x[1] == api_host, getmembers(Client)))):
            raise ApiError('The host "{:s}" does not exist'.format(api_host))

        self._api_token = api_token
        self._api_host = api_host

    def request(self, method: str, method_url: str, parameters: dict = None):
        method = method.upper()

        if method not in ['GET', 'POST', 'PUT', 'DELETE']:
            raise ApiError('The method "{method}" does not exist'.format(method=method))

        request_url = self.API_URL.format(host=self._api_host) + method_url

        if type(parameters) is dict:
            request_url += '?' + '&'.join([
                key + '=' + quote(json.dumps(value) if type(value) in (dict, list) else value)
                for key, value in parameters.items()
            ])

        headers = {
            'Accept': 'application/json',
            'Authorization': 'bearer {token}'.format(token=self._api_token)
        }

        response = requests.request(method, request_url, headers=headers)

        if 200 != response.status_code:
            raise ApiError(
                str(response.status_code) +
                ' ' +
                response.reason +
                ' - see: https://developers.wrike.com/errors/'
            )

        try:
            return response.json()
        except ValueError:
            raise ApiError('Error in the JSON response')

    def get_folder(self, folder_id: str = None, share_id: str = None):
        if folder_id is not None:
            response = self.request('GET', f'/folders/{folder_id}')
        elif share_id is not None:
            response = self.request('GET', '/folders', {
                'permalink': self.SHARE_URL.format(host=self._api_host, share_id=share_id)
            })
        else:
            raise ApiError('You have to pass the exact folder- or share id')

        folder = None

        if 'data' in response and 1 == len(response['data']):
            folder = response['data'][0]

        return folder

    def get_contact(self, email: str):
        response = self.request('GET', '/contacts')

        if not Validator.validate_email(email):
            ApiError('The user email is not valid')

        if 'data' in response and 0 < len(response['data']):
            for contact in response['data']:
                if 'profiles' in contact and 0 < len(contact['profiles']):
                    for profile in contact['profiles']:
                        if (
                            'role' in profile and 'User' == profile['role']
                        ) and (
                            'email' in profile and email == profile['email']
                        ):
                            return contact

        return None

    def create_task(
        self,
        folder_id: str,
        title: str,
        description: str,
        responsibles: list = None,
        date_start: date = None,
        date_end: date = None
    ):
        if 0 == len(title):
            raise ApiError('Missing required title in task data')

        parameters = {'title': title}

        if type(description) is str:
            parameters['description'] = description

        if type(date_start) is date or type(date_end) is date:
            if date_end is None:
                date_end = date_start
            if date_start is None:
                date_start = date_end

            parameters['dates'] = {
                'start': '{:%Y-%m-%d}'.format(date_start),
                'due': '{:%Y-%m-%d}'.format(date_end)
            }

        if type(responsibles) is list and 0 < len(responsibles):
            parameters['responsibles'] = responsibles

        response = self.request('POST', f'/folders/{folder_id}/tasks', parameters)

        if 'data' in response and 1 == len(response['data']):
            task = response['data'][0]
        else:
            raise ApiError('Failed to create task')

        return task
