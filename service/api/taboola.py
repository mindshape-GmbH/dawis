from inspect import getmembers
from datetime import datetime, date, timedelta
from urllib.parse import quote
import re
import requests


class ApiError(Exception):
    def __init__(self, message: str):
        self.message = message


class Client:
    _client_id: str
    _client_secret: str
    _auth_token: str = None
    _auth_token_expires: datetime = None

    # https://developers.taboola.com/backstage-api/reference
    API_URL = 'https://backstage.taboola.com/backstage'

    # https://developers.taboola.com/backstage-api/reference#client-credentials-flow
    ENDPOINT_OAUTH = '/oauth/token'

    # https://developers.taboola.com/backstage-api/reference#reporting-overview
    ENDPOINT_REPORTING_SUMMARY = '/api/1.0/{account_id}/reports/campaign-summary/dimensions/{dimension}'
    ENDPOINT_REPORTING_TOPCAMPAIGNCONTENT = '/api/1.0/{account_id}/reports/top-campaign-content/dimensions/item_breakdown'

    def __init__(self, client_id: str, client_secret: str):
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_auth_token()
        self._endpoint_parameter_regex = re.compile(r'{\w+}')

    def request(
        self, method: str,
        endpoint: str,
        endpoint_parameter: dict = None,
        query_parameters: dict = None,
        data: dict = None
    ):
        if 1 != len(list(filter(lambda x: x[0].startswith('ENDPOINT_') and x[1] == endpoint, getmembers(Client)))):
            raise ApiError('The endpoint "{:s}" does not exist'.format(endpoint))

        if len(self._endpoint_parameter_regex.findall(endpoint)) != len(endpoint_parameter):
            raise ApiError('Invalid amount of parameters for endpoint')

        request_url = self.API_URL + endpoint.format(**endpoint_parameter)

        if self._auth_token_expires <= datetime.utcnow():
            self._refresh_auth_token()

        response = self._request(
            method,
            request_url,
            headers={'Authorization': 'bearer {token}'.format(token=self._auth_token)},
            data=data,
            query_parameters=query_parameters
        )

        return response

    def _refresh_auth_token(self):
        response = self._request(
            'POST',
            self.API_URL + self.ENDPOINT_OAUTH,
            data='client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials'.format(
                client_id=self._client_id,
                client_secret=self._client_secret
            ),
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )

        self._auth_token = response['access_token']
        self._auth_token_expires = datetime.utcnow() + timedelta(seconds=response['expires_in'])

    def _request(
        self,
        method: str,
        url: str,
        query_parameters: dict = None,
        json: dict = None,
        data: str = None,
        headers: dict = None
    ) -> dict:
        response = requests.request(method, url, params=query_parameters, data=data, json=json, headers=headers)

        if 401 == response.status_code:
            self._refresh_auth_token()
            response = requests.request(method, url, params=query_parameters, data=data, json=json, headers=headers)

        if 200 != response.status_code:
            raise ApiError(
                str(response.status_code) +
                ' ' +
                response.reason +
                ' - see: https://developers.taboola.com/backstage-api/reference'
            )

        try:
            return response.json()
        except ValueError:
            raise ApiError('Error in the JSON response')
