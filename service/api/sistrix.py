from inspect import getmembers
import requests


class ApiError(Exception):
    def __init__(self, message: str):
        self.message = message


class Client:
    # https://www.sistrix.de/api/
    API_URL = 'https://api.sistrix.com'
    API_FORMAT = 'json'

    # https://www.sistrix.de/api/domain
    ENDPOINT_DOMAIN = 'domain'
    ENDPOINT_DOMAIN_OVERVIEW = 'domain.overview'
    ENDPOINT_DOMAIN_VISIBILITYINDEX = 'domain.sichtbarkeitsindex'
    ENDPOINT_DOMAIN_VISIBILITYINDEX_OVERVIEW = 'domain.sichtbarkeitsindex.overview'
    ENDPOINT_DOMAIN_PAGES = 'domain.pages'
    ENDPOINT_DOMAIN_AGE = 'domain.age'
    ENDPOINT_DOMAIN_COMPETITORS_SEO = 'domain.competitors.seo'
    ENDPOINT_DOMAIN_COMPETITORS_SEM = 'domain.competitors.sem'
    ENDPOINT_DOMAIN_KEYWORDCOUNT_SEO = 'domain.kwcount.seo'
    ENDPOINT_DOMAIN_KEYWORDCOUNT_SEO_TOP10 = 'domain.kwcount.seo.top10'
    ENDPOINT_DOMAIN_KEYWORDCOUNT_SEM = 'domain.kwcount.sem'
    ENDPOINT_DOMAIN_KEYWORDCOUNT_US = 'domain.kwcount.us'
    ENDPOINT_DOMAIN_RANKING_DISTRIBUTION = 'domain.ranking.distribution'
    ENDPOINT_DOMAIN_SOCIAL_OVERVIEW = 'domain.social.overview'
    ENDPOINT_DOMAIN_SOCIAL_TOP = 'domain.social.top'
    ENDPOINT_DOMAIN_SOCIAL_LATEST = 'domain.social.latest'
    ENDPOINT_DOMAIN_SOCIAL_URL = 'domain.social.url'
    ENDPOINT_DOMAIN_OPPORTUNITIES = 'domain.opportunities'
    ENDPOINT_DOMAIN_IDEAS = 'domain.ideas'

    # https://www.sistrix.de/api/keyword
    ENDPOINT_KEYWORD = 'keyword'
    ENDPOINT_KEYWORD_SEO = 'keyword.seo'
    ENDPOINT_KEYWORD_SEM = 'keyword.sem'
    ENDPOINT_KEYWORD_US = 'keyword.us'
    ENDPOINT_KEYWORD_DOMAIN_SEO = 'keyword.domain.seo'
    ENDPOINT_KEYWORD_DOMAIN_SEM = 'keyword.domain.sem'
    ENDPOINT_KEYWORD_DOMAIN_US = 'keyword.domain.us'

    # https://www.sistrix.de/api/link
    ENDPOINT_LINKS_OVERVIEW = 'links.overview'
    ENDPOINT_LINKS_LIST = 'links.list'
    ENDPOINT_LINKS_LINKTARGETS = 'links.linktargets'
    ENDPOINT_LINKS_LINKTEXTS = 'links.linktexts'

    # https://www.sistrix.de/api/optimizer
    ENDPOINT_OPTIMIZER_PROJECTS = 'optimizer.projects'
    ENDPOINT_OPTIMIZER_PROJECT = 'optimizer.project'
    ENDPOINT_OPTIMIZER_RANKING = 'optimizer.ranking'
    ENDPOINT_OPTIMIZER_VISIBILITY = 'optimizer.visibility'
    ENDPOINT_OPTIMIZER_KEYWORD_SERPS = 'optimizer.keyword.serps'
    ENDPOINT_OPTIMIZER_ONPAGE_OVERVIEW = 'optimizer.onpage.overview'
    ENDPOINT_OPTIMIZER_ONPAGE_CRAWL = 'optimizer.onpage.crawl'
    ENDPOINT_OPTIMIZER_ONPAGE_ISSUE = 'optimizer.onpage.issue'

    # https://www.sistrix.de/api/marketplace
    ENDPOINT_MARKETPLACE_PRODUCT = 'marketplace.product'
    ENDPOINT_MARKETPLACE_PRODUCT_OVERVIEW = 'marketplace.product.overview'
    ENDPOINT_MARKETPLACE_PRODUCT_PRICE = 'marketplace.product.price'
    ENDPOINT_MARKETPLACE_PRODUCT_REVIEWS = 'marketplace.product.reviews'
    ENDPOINT_MARKETPLACE_PRODUCT_KEYWORDS = 'marketplace.product.keywords'

    def __init__(self, api_key: str, api_format: str = 'json'):
        self._api_key = api_key
        self._api_format = api_format

    def request(self, endpoint: str, parameters: dict = None):
        if 1 != len(list(filter(lambda x: x[0].startswith('ENDPOINT_') and x[1] == endpoint, getmembers(Client)))):
            raise ApiError('The endpoint "{:s}" does not exist'.format(endpoint))

        request_url = self.API_URL + '/' + endpoint + '?api_key=' + self._api_key

        if 0 < len(self._api_format):
            request_url += '&format=' + self._api_format

        if type(parameters) is dict:
            for parameter, value in parameters.items():
                if type(value) is bool:
                    value = 'true' if value else 'false'
                if type(value) is not str:
                    value = str(value)

                request_url += '&' + parameter + '=' + value

        response = requests.get(request_url)

        if 200 != response.status_code:
            raise ApiError(
                str(response.status_code) +
                ' ' +
                response.reason +
                ' - see: https://www.sistrix.de/api/error-codes'
            )

        response_data = response.json()

        if 'error' in response_data:
            error_messages = []
            for error in response_data['error']:
                error_messages.append(str(error['error_code']) + ': ' + error['error_message'])

            error_messages.append('info: https://www.sistrix.de/api/error-codes')

            raise ApiError('\n'.join(error_messages))

        try:
            return response_data
        except ValueError:
            raise ApiError('Error in the JSON response')
