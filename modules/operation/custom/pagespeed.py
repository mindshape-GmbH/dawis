from database.connection import Connection
from service.check import Check
from utilities.configuration import Configuration
from utilities.url import URL
from utilities.exceptions import ConfigurationMissingError
from modules.aggregation.custom.pagespeed import Pagespeed as PagespeedAggregationModule
import json


class Pagespeed:
    def __init__(self, configuration: Configuration, configuration_key: str, connection: Connection):
        if not connection.has_bigquery() and not connection.has_orm():
            raise ConfigurationMissingError('Missing a database configuration for this operation')

        self.configuration = configuration
        self.module_configuration = configuration.operations.get_custom_configuration_operation(configuration_key)
        self.mongodb = connection.mongodb
        self.check_service = Check(connection)

    def run(self):
        if len(self.module_configuration.checks) > 0:
            print('Running operation pagespeed:', "\n")

            if not self.mongodb.has_collection(PagespeedAggregationModule.COLLECTION_NAME):
                return

            pagespeed_tests = self.mongodb.find(
                PagespeedAggregationModule.COLLECTION_NAME,
                {'processed_pagespeed': {'$exists': False}}
            )

            for pagespeed_test in pagespeed_tests:
                print(' + ' + str(pagespeed_test['url']))

                pagespeed_json_desktop = json.loads(pagespeed_test['desktop']['body'])
                pagespeed_json_mobile = json.loads(pagespeed_test['mobile']['body'])
                urlset_name = pagespeed_test['urlset']
                url = pagespeed_test['url']

                self.check_fcp_score(urlset_name, url, 'fcp_score', pagespeed_json_desktop, 'desktop')
                self.check_fcp_score(urlset_name, url, 'fcp_score', pagespeed_json_mobile, 'mobile')
                self.check_fcp_display(urlset_name, url, 'fcp_display', pagespeed_json_desktop, 'desktop')
                self.check_fcp_display(urlset_name, url, 'fcp_display', pagespeed_json_mobile, 'mobile')

                self.check_tti_score(urlset_name, url, 'tti_score', pagespeed_json_desktop, 'desktop')
                self.check_tti_score(urlset_name, url, 'tti_score', pagespeed_json_mobile, 'mobile')
                self.check_tti_display(urlset_name, url, 'tti_display', pagespeed_json_desktop, 'desktop')
                self.check_tti_display(urlset_name, url, 'tti_display', pagespeed_json_mobile, 'mobile')

                self.check_ttfb_score(urlset_name, url, 'ttfb_score', pagespeed_json_desktop, 'desktop')
                self.check_ttfb_score(urlset_name, url, 'ttfb_score', pagespeed_json_mobile, 'mobile')
                self.check_ttfb_display(urlset_name, url, 'ttfb_display', pagespeed_json_desktop, 'desktop')
                self.check_ttfb_display(urlset_name, url, 'ttfb_display', pagespeed_json_mobile, 'mobile')

                self.check_performance_score(
                    urlset_name,
                    url,
                    'performance_score',
                    pagespeed_json_desktop,
                    'desktop'
                )

                self.check_performance_score(
                    urlset_name,
                    url,
                    'performance_score',
                    pagespeed_json_mobile,
                    'mobile')

                self.check_uses_optimized_images(
                    urlset_name,
                    url,
                    'uses_optimized_images',
                    pagespeed_json_desktop,
                    'desktop'
                )

                self.check_uses_optimized_images(
                    urlset_name,
                    url,
                    'uses_optimized_images',
                    pagespeed_json_mobile,
                    'mobile'
                )

                self.check_render_blocking_resources(
                    urlset_name,
                    url,
                    'render_blocking_resources',
                    pagespeed_json_desktop,
                    'desktop'
                )

                self.check_render_blocking_resources(
                    urlset_name,
                    url,
                    'render_blocking_resources',
                    pagespeed_json_mobile,
                    'mobile'
                )

                self.check_uses_text_compression(
                    urlset_name,
                    url,
                    'uses_text_compression',
                    pagespeed_json_desktop,
                    'desktop'
                )

                self.check_uses_text_compression(
                    urlset_name,
                    url,
                    'uses_text_compression',
                    pagespeed_json_mobile,
                    'mobile'
                )

                self.check_uses_long_cache_ttl(
                    urlset_name,
                    url,
                    'uses_long_cache_ttl',
                    pagespeed_json_desktop,
                    'desktop'
                )

                self.check_uses_long_cache_ttl(
                    urlset_name,
                    url,
                    'uses_long_cache_ttl',
                    pagespeed_json_mobile,
                    'mobile'
                )

                self.check_unminified_css(urlset_name, url, 'unminified_css', pagespeed_json_desktop, 'desktop')
                self.check_unminified_css(urlset_name, url, 'unminified_css', pagespeed_json_mobile, 'mobile')

                self.check_unminified_js(urlset_name, url, 'unminified_js', pagespeed_json_desktop, 'desktop')
                self.check_unminified_js(urlset_name, url, 'unminified_js', pagespeed_json_mobile, 'mobile')

                self.mongodb.update_one(
                    PagespeedAggregationModule.COLLECTION_NAME,
                    pagespeed_test['_id'],
                    {'processed_pagespeed': True}
                )

            print("\n")

    def check_fcp_score(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['first-contentful-paint']['score'])
                if result >= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-fcp_score_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_fcp_display(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['first-contentful-paint']['numericValue'])
                if result <= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-fcp_display_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_tti_score(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['interactive']['score'])
                if result >= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-time_to_interactive_score_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_tti_display(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['interactive']['numericValue'])
                if result <= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-time_to_interactive_display_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_ttfb_score(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['time-to-first-byte']['score'])
                if result >= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-ttfb_score_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_ttfb_display(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['time-to-first-byte']['numericValue'])
                if result <= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-ttfb_display_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_performance_score(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['categories']['performance']['score'])
                if result >= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-performance_score_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_render_blocking_resources(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['render-blocking-resources']['score'])
                if result >= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-render_blocking_resources_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_uses_optimized_images(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['uses-optimized-images']['score'])
                if result >= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-uses_optimized_images_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_uses_text_compression(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['uses-text-compression']['score'])
                if result >= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-uses_text_compression_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_uses_long_cache_ttl(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['uses-long-cache-ttl']['score'])
                if result >= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-uses_long_cache_ttl_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_unminified_css(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''
            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['unminified-css']['score'])
                if result >= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-unminified_css_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))

    def check_unminified_js(self, urlset_name: str, url: URL, check: str, j: dict, strategy: str):
        if check in self.module_configuration.checks:
            assert_val = self.module_configuration.checks[check][strategy]

            print('      -> check_' + check + ' "' + str(assert_val) + '"', end='')

            valid = False
            result = ''

            if 'lighthouseResult' in j:
                result = float(j['lighthouseResult']['audits']['unminified-javascript']['score'])
                if result >= assert_val:
                    valid = True

            self.check_service.add_check(
                self.module_configuration.database,
                urlset_name,
                'pagespeed-unminified_javascript_' + strategy,
                str(result),
                valid,
                '',
                '',
                url.protocol,
                url.domain,
                url.path,
                url.query,
            )

            print(' ... ' + str(valid))
