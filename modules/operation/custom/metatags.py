from database.connection import Connection
from service.check import Check
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError
from modules.aggregation.custom.html_parser import HtmlParser
from bs4 import BeautifulSoup
import requests
from utilities.url import URL


class Metatags:
    def __init__(self, configuration: Configuration, connection: Connection):
        if not connection.has_bigquery() and not connection.has_orm():
            raise ConfigurationMissingError('Missing a database configuration for this operation')

        self.configuration = configuration
        self.mongodb = connection.mongodb
        self.check_service = Check(connection)
        self.metatags_config = self.configuration.operations.get_custom_configuration_operation('metatags')

    def run(self):
        if len(self.metatags_config.urlsets) > 0:
            print('Running operation metatags:', "\n")

            if not self.mongodb.has_collection(HtmlParser.COLLECTION_NAME):
                return

            for urlset in self.metatags_config.urlsets:
                print(' - "' + str(urlset['url']) + '":')

                for single_urlset in urlset:

                    urlset_name = urlset[single_urlset]

                    parsed_data = self.mongodb.find(
                        HtmlParser.COLLECTION_NAME,
                        {
                            'urlset': urlset_name,
                            'processed_metatags': {'$exists': False}
                        }
                    )

                    urlset_config = urlset['checks']

                    self.check_has_title_duplicates(parsed_data, urlset_name, urlset_config)
                    self.check_has_description_duplicates(parsed_data, urlset_name, urlset_config)

                    for data in parsed_data:
                        print('   + ' + str(data['url']))

                        self.check_has_title(data, urlset_name, urlset_config)
                        self.check_is_title_empty(data, urlset_name, urlset_config)
                        self.check_has_title_changed(data, urlset_name, urlset_config)

                        self.check_has_description(data, urlset_name, urlset_config)
                        self.check_is_description_empty(data, urlset_name, urlset_config)
                        self.check_has_description_changed(data, urlset_name, urlset_config)

                        self.check_has_canonical(data, urlset_name, urlset_config)
                        self.check_canonical_is_self_referencing(data, urlset_name, urlset_config)
                        self.check_canonical_href_200(data, urlset_name, urlset_config)

                        self.mongodb.update_one(
                            HtmlParser.COLLECTION_NAME,
                            data['_id'],
                            {'processed_metatags': True}
                        )

                print("\n")

    # METATAG TITLE

    def get_metatitle(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'title' in urlset_config:
            doc = BeautifulSoup(data['body'], "html.parser")
            titles = doc.find_all("title")

            problem_detected = {'multi': False, 'empty': False}
            if titles:
                if len(titles) > 1:
                    problem_detected['multi'] = True
                else:
                    return titles
            else:
                problem_detected['empty'] = True

            return problem_detected

    def save_problem_multi_title(self, multi: bool, data):
        url = data['url']

        value = ''
        error = ''
        valid = False

        if multi:
            error = 'several titletags on page detected'
        else:
            valid = True

        self.check_service.add_check(
            self.metatags_config.database,
            data['urlset'],
            'metatags-has_multiple_titles',
            value,
            valid,
            '',
            error,
            url.protocol,
            url.domain,
            url.path,
            url.query,
        )

    def check_has_title(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'title' in urlset_config:
            if 'has_title' in urlset_config['title']:
                assert_val = urlset_config['title']['has_title']

                print('      -> check_has_title "' + str(assert_val) + '"', end='')

                valid = False
                multi = False
                empty = False
                titles = {}

                titles = self.get_metatitle(data, urlset_name, urlset_config)
                if 'multi' in titles:
                    if titles['multi']:
                        multi = True
                else:
                    value = ''

                    if titles:
                        for title in titles:
                            if title != '':
                                value = str(title)
                                exists = True
                                if exists == assert_val:
                                    valid = True

                    url = data['url']

                    error = ''
                    if len(titles) == 0 and not valid:
                        error = 'title missing'

                    self.check_service.add_check(
                        self.metatags_config.database,
                        data['urlset'],
                        'metatags-has_title',
                        value,
                        valid,
                        '',
                        error,
                        url.protocol,
                        url.domain,
                        url.path,
                        url.query,
                    )

                    print(' ... has title ' + str(valid))

                self.save_problem_multi_title(multi, data)

    def check_is_title_empty(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'title' in urlset_config:
            if 'is_title_empty' in urlset_config['title']:
                assert_val = urlset_config['title']['is_title_empty']

                print('      -> check_has_title "' + str(assert_val) + '"', end='')

                valid = False

                titles = self.get_metatitle(data, urlset_name, urlset_config)
                value = ''

                empty = False

                for title in titles:
                    value = str(title)
                    if title == '':
                        empty = True
                    if empty == assert_val:
                        valid = True

                url = data['url']

                error = ''
                if empty and valid:
                    error = 'titletag is empty'

                self.check_service.add_check(
                    self.metatags_config.database,
                    data['urlset'],
                    'metatags-is_title_empty',
                    value,
                    valid,
                    '',
                    error,
                    url.protocol,
                    url.domain,
                    url.path,
                    url.query,
                )

                print(' ... is title empty ' + str(valid))

    def check_has_title_changed(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'title' in urlset_config:
            if 'has_title_changed' in urlset_config['title']:
                assert_val = urlset_config['title']['has_title_changed']

                valid = False

                titles_new = self.get_metatitle(data, urlset_name, urlset_config)
                value_new = ''

                if len(titles_new) == 1:
                    for title in titles_new:
                        if title != '':
                            value_new = str(title)

                last_parsed_data = self.mongodb.find_last_sorted(
                    HtmlParser.COLLECTION_NAME,
                    {
                        'url.protocol': data['url'].protocol,
                        'url.domain': data['url'].domain,
                        'url.path': data['url'].path,
                        'url.query': data['url'].query,
                        'processed_metatags': {'$exists': True}
                    },
                    [('date', -1)]
                )

                value_last = ''
                for last_data in last_parsed_data:

                    titles_last = self.get_metatitle(last_data, urlset_name, urlset_config)

                    if len(titles_last) == 1:
                        for title in titles_last:
                            if title != '':
                                value_last = str(title)

                check_result = True
                if value_new == value_last:
                    check_result = False # title has not changed

                if check_result == assert_val:
                    valid = True

                diff = ''
                error = ''
                if not valid and check_result:
                    diff = str(value_last)
                    error = 'title has changed'

                url = data['url']

                self.check_service.add_check(
                    self.metatags_config.database,
                    data['urlset'],
                    'metatags-has_title_changed',
                    value_new,
                    valid,
                    diff,
                    error,
                    url.protocol,
                    url.domain,
                    url.path,
                    url.query,
                )

                print(' ... has title changed ' + str(valid))

    def check_has_title_duplicates(self, parsed_data: dict, urlset_name: str, urlset_config: dict):
        if 'title' in urlset_config:
            if 'has_title_duplicates' in urlset_config['title']:
                assert_val = urlset_config['title']['has_title_duplicates']

                valid = True

                titles_dict = {}

                for data in parsed_data:

                    # dict_key = str(data['url'])

                    doc = BeautifulSoup(data['body'], "html.parser")
                    titles = doc.find_all("title")

                    if len(titles) == 1:
                        for title in titles:
                            if title != '':
                                titles_dict[str(data['url'])] = title

                title_sorted = {}

                # geeksforgeeks.org/python-find-keys-with-duplicate-values-in-dictionary/

                for key, value in titles_dict.items():
                    if value not in title_sorted:
                        title_sorted[value] = [key]
                    else:
                        title_sorted[value].append(key)

                title_duplicates = {}

                for key_title, value_urls in title_sorted.items():
                    if len(title_sorted[key_title]) > 1:
                        title_duplicates[key_title] = value_urls
                    elif len(title_sorted[key_title]) == 1:
                        url = ''
                        for url_str in value_urls:
                            url = URL(url_str)
                        valid = False
                        dup = False
                        if dup == assert_val:
                            valid = True
                        value = str(key_title)
                        urlset = ''
                        for data in parsed_data:
                            urlset = data['urlset']

                        self.check_service.add_check(
                            self.metatags_config.database,
                            urlset,
                            'metatags-has_title_duplicates',
                            value,
                            valid,
                            '',
                            '',
                            url.protocol,
                            url.domain,
                            url.path,
                            url.query,
                        )

                for dup_title in title_duplicates:
                    url = ''
                    for problem_url in title_duplicates[dup_title]:
                        url = URL(problem_url)
                        valid = False
                        dup = True
                        if dup == assert_val:
                            valid = True
                        value = str(dup_title)
                        diff = ''
                        for other_url in title_duplicates[dup_title]:
                            if other_url is not problem_url:
                                if diff == '':
                                    diff += other_url
                                else:
                                    diff += ', ' + other_url

                        urlset = ''
                        for data in parsed_data:
                            urlset = data['urlset']

                        error = ''
                        if dup and not valid:
                            error = 'title duplicates in url-set detected'

                        self.check_service.add_check(
                            self.metatags_config.database,
                            urlset,
                            'metatags-has_title_duplicates',
                            value,
                            valid,
                            diff,
                            error,
                            url.protocol,
                            url.domain,
                            url.path,
                            url.query,
                        )

    # METATAG DESCRIPTION

    def get_metadescription(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'description' in urlset_config:
            doc = BeautifulSoup(data['body'], "html.parser")
            metas = doc.find_all("meta", attrs={'name': 'description'})

            problem_detected = {'multi': False, 'empty': False}
            if metas:
                if len(metas) > 1:
                    problem_detected['multi'] = True
                else:
                    return metas
            else:
                problem_detected['empty'] = True

            return problem_detected

    def save_problem_multi_description(self, multi: bool, data):
        url = data['url']

        value = ''
        error = ''
        valid = False

        if multi:
            error = 'several descriptiontags on page detected'
        else:
            valid = True

        self.check_service.add_check(
            self.metatags_config.database,
            data['urlset'],
            'metatags-has_multiple_descriptions',
            value,
            valid,
            '',
            error,
            url.protocol,
            url.domain,
            url.path,
            url.query,
        )

    def check_has_description(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'description' in urlset_config:
            if 'has_description' in urlset_config['description']:
                assert_val = urlset_config['description']['has_description']

                print('      -> check_has_title "' + str(assert_val) + '"', end='')

                valid = False
                multi = False
                empty = False
                titles = {}

                metas = self.get_metadescription(data, urlset_name, urlset_config)

                if 'multi' in metas:
                    if metas['multi']:
                        multi = True
                else:
                    value = ''
                    for meta in metas:
                        metadescription = meta.get('content')
                        if metadescription != '':
                            value = metadescription
                            exists = True
                            if exists == assert_val:
                                valid = True

                        url = data['url']

                        self.check_service.add_check(
                            self.metatags_config.database,
                            data['urlset'],
                            'metatags-has_description',
                            value,
                            valid,
                            '',
                            '',
                            url.protocol,
                            url.domain,
                            url.path,
                            url.query,
                        )

                        print(' ... has title ' + str(valid))

                self.save_problem_multi_description(multi, data)

    def check_is_description_empty(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'description' in urlset_config:
            if 'is_description_empty' in urlset_config['description']:
                assert_val = urlset_config['description']['is_description_empty']

                print('      -> check_has_title "' + str(assert_val) + '"', end='')

                valid = False

                metas = self.get_metadescription(data, urlset_name, urlset_config)
                empty = False

                if 'multi' in metas:
                    if metas['multi']:
                        return
                else:
                    value = ''
                    for meta in metas:
                        metadescription = meta.get('content')
                        value = metadescription
                        if metadescription == '':
                            empty = True
                        if empty == assert_val:
                            valid = True

                    error = ''
                    if empty and not valid:
                        error = 'description is empty'

                    url = data['url']

                    self.check_service.add_check(
                        self.metatags_config.database,
                        data['urlset'],
                        'metatags-is_description_empty',
                        value,
                        valid,
                        '',
                        error,
                        url.protocol,
                        url.domain,
                        url.path,
                        url.query,
                    )

                    print(' ... ' + str(valid))

    def check_has_description_changed(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'description' in urlset_config:
            if 'has_description_changed' in urlset_config['description']:
                assert_val = urlset_config['description']['has_description_changed']

                valid = False

                descriptions_new = self.get_metadescription(data, urlset_name, urlset_config)
                value_new = ''

                if len(descriptions_new) == 1:
                    for description in descriptions_new:
                        if description != '':
                            value_new = str(description)

                last_parsed_data = self.mongodb.find_last_sorted(
                    HtmlParser.COLLECTION_NAME,
                    {
                        'url.protocol': data['url'].protocol,
                        'url.domain': data['url'].domain,
                        'url.path': data['url'].path,
                        'url.query': data['url'].query,
                        'processed_metatags': {'$exists': True}
                    },
                    [('date', -1)]
                )

                value_last = ''
                for last_data in last_parsed_data:

                    descriptions_last = self.get_metadescription(last_data, urlset_name, urlset_config)

                    if len(descriptions_last) == 1:
                        for description in descriptions_last:
                            if description != '':
                                value_last = str(description)

                check_result = True
                if value_new == value_last:
                    check_result = False # description has not changed

                if check_result == assert_val:
                    valid = True

                diff = ''
                if not valid:
                    diff = str(value_last)

                error = ''
                if not valid and check_result:
                    error = 'description has changed'

                url = data['url']

                self.check_service.add_check(
                    self.metatags_config.database,
                    data['urlset'],
                    'metatags-has_description_changed',
                    value_new,
                    valid,
                    diff,
                    error,
                    url.protocol,
                    url.domain,
                    url.path,
                    url.query,
                )

    def check_has_description_duplicates(self, parsed_data: dict, urlset_name: str, urlset_config: dict):
        if 'description' in urlset_config:
            if 'has_description_duplicates' in urlset_config['description']:
                assert_val = urlset_config['description']['has_description_duplicates']

                valid = True

                descriptions_dict = {}

                for data in parsed_data:

                    # dict_key = str(data['url'])

                    doc = BeautifulSoup(data['body'], "html.parser")
                    descriptions = doc.find_all("meta", attrs={'name': 'description'})

                    if len(descriptions) == 1:
                        for description in descriptions:
                            if description.get('content') != '':
                                descriptions_dict[str(data['url'])] = description.get('content')

                description_sorted = {}

                # geeksforgeeks.org/python-find-keys-with-duplicate-values-in-dictionary/

                for key, value in descriptions_dict.items():
                    if value not in description_sorted:
                        description_sorted[value] = [key]
                    else:
                        description_sorted[value].append(key)

                description_duplicates = {}

                for key_description, value_urls in description_sorted.items():
                    if len(description_sorted[key_description]) > 1:
                        description_duplicates[key_description] = value_urls
                    elif len(description_sorted[key_description]) == 1:
                        url = ''
                        for url_str in value_urls:
                            url = URL(url_str)
                        valid = False
                        dup = False
                        if dup == assert_val:
                            valid = True
                        value = str(key_description)
                        urlset = ''
                        for data in parsed_data:
                            urlset = data['urlset']

                        self.check_service.add_check(
                            self.metatags_config.database,
                            urlset,
                            'metatags-has_description_duplicates',
                            value,
                            valid,
                            '',
                            '',
                            url.protocol,
                            url.domain,
                            url.path,
                            url.query,
                        )

                for dup_description in description_duplicates:
                    url = ''
                    for problem_url in description_duplicates[dup_description]:
                        url = URL(problem_url)
                        valid = False
                        dup = True
                        if dup == assert_val:
                            valid = True
                        value = str(dup_description)
                        diff = ''
                        for other_url in description_duplicates[dup_description]:
                            if other_url is not problem_url:
                                if diff == '':
                                    diff += other_url
                                else:
                                    diff += ', ' + other_url

                        urlset = ''
                        for data in parsed_data:
                            urlset = data['urlset']

                        error = ''
                        if dup and not valid:
                            error = 'description duplicates in url-set detected'

                        self.check_service.add_check(
                            self.metatags_config.database,
                            urlset,
                            'metatags-has_description_duplicates',
                            value,
                            valid,
                            diff,
                            error,
                            url.protocol,
                            url.domain,
                            url.path,
                            url.query,
                        )

    # METATAG CANONICAL

    def get_canonical_href(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'canonical' in urlset_config:

            doc = BeautifulSoup(data['body'], "html.parser")
            links = doc.find_all("link", rel='canonical')
            href = ''

            for link in links:
                href = link['href']

            return href

    def check_has_canonical(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'canonical' in urlset_config:
            if 'has_canonical' in urlset_config['canonical']:
                assert_val = urlset_config['canonical']['has_canonical']

                print('      -> check_has_canonical "' + str(assert_val) + '"', end='')

                valid = False
                exists = False

                canonical_href = self.get_canonical_href(data, urlset_name, urlset_config)
                value = str(canonical_href)
                if canonical_href != '':
                    exists = True
                    if exists == assert_val:
                        valid = True

                url = data['url']

                error = ''
                if not exists and not valid:
                    error = 'no canonical'

                self.check_service.add_check(
                    self.metatags_config.database,
                    data['urlset'],
                    'metatags-has_canonical',
                    value,
                    valid,
                    '',
                    error,
                    url.protocol,
                    url.domain,
                    url.path,
                    url.query,
                )

                print(' ... ' + str(valid))

    def check_canonical_is_self_referencing(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'canonical' in urlset_config:
            if 'canonical_is_self_referencing' in urlset_config['canonical']:
                assert_val = urlset_config['canonical']['canonical_is_self_referencing']

                valid = False
                url = data['url']

                canonical_href = self.get_canonical_href(data, urlset_name, urlset_config)
                value = str(canonical_href)
                if canonical_href != '':
                    if canonical_href == str(url):
                        self_referencing = True
                        if self_referencing == assert_val:
                            valid = True

                self.check_service.add_check(
                    self.metatags_config.database,
                    data['urlset'],
                    'metatags-canonical_is_self_referencing',
                    value,
                    valid,
                    '',
                    '',
                    url.protocol,
                    url.domain,
                    url.path,
                    url.query,
                )

                print(' ... ' + 'self_referencing' + str(valid))

    def check_canonical_href_200(self, data: dict, urlset_name: str, urlset_config: dict):
        if 'canonical' in urlset_config:
            if 'canonical_href_200' in urlset_config['canonical']:
                assert_val = urlset_config['canonical']['canonical_href_200']

                valid = False
                url = data['url']

                response_200 = False
                error = ''
                canonical_href = self.get_canonical_href(data, urlset_name, urlset_config)
                value = str(canonical_href)
                if canonical_href != '':
                    response = requests.get(canonical_href)
                    if response.status_code == 200:
                        response_200 = True
                    else:
                        error = 'href in canonical not valid'
                if response_200 == assert_val:
                    valid = True

                self.check_service.add_check(
                    self.metatags_config.database,
                    data['urlset'],
                    'metatags-canonical_href_200',
                    value,
                    valid,
                    '',
                    error,
                    url.protocol,
                    url.domain,
                    url.path,
                    url.query,
                )

                print(' ... ' + str(valid))
