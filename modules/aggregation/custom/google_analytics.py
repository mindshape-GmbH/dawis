from database.connection import Connection
from database.bigquery import BigQuery
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError, ConfigurationInvalidError
from googleapiclient.discovery import build
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.table import TableReference, TimePartitioning, TimePartitioningType
from google.cloud.bigquery.schema import SchemaField
from google.oauth2 import service_account
from pandas import DataFrame
from os.path import abspath
from datetime import datetime, date, timedelta
from time import time
import logging
import re


class _DataAlreadyExistError(Exception):
    pass


class _DataNotAvailableYet(Exception):
    pass


class GoogleAnalytics:
    COLLECTION_NAME = 'google_analytics'

    DEFAULT_DAYS_AGO = 1

    # All dimensions and metrics: https://ga-dev-tools.appspot.com/dimensions-metrics-explorer/
    INT_DIMENSIONS_METRICS = [
        'users',
        'newUsers',
        '1dayUsers',
        '7dayUsers',
        '14dayUsers',
        '28dayUsers',
        '30dayUsers',
        'sessions',
        'bounces',
        'uniqueDimensionCombinations',
        'hits',
        'organicSearches',
        'impressions',
        'adClicks',
        'goalXXStarts',
        'goalStartsAll',
        'goalXXCompletions',
        'goalCompletionsAll',
        'goalXXAbandons',
        'goalAbandonsAll',
        'entrances',
        'pageviews',
        'uniquePageviews',
        'exits',
        'contentGroupUniqueViewsXX',
        'searchResultViews',
        'searchUniques',
        'searchSessions',
        'searchDepth',
        'searchRefinements',
        'searchExits',
        'pageLoadTime',
        'pageLoadSample',
        'domainLookupTime',
        'pageDownloadTime',
        'redirectionTime',
        'serverConnectionTime',
        'serverResponseTime',
        'speedMetricsSample',
        'domInteractiveTime',
        'domContentLoadedTime',
        'domLatencyMetricsSample',
        'screenviews',
        'uniqueScreenviews',
        'totalEvents',
        'uniqueEvents',
        'eventValue',
        'sessionsWithEvent',
        'transactions',
        'itemQuantity',
        'uniquePurchases',
        'internalPromotionClicks',
        'internalPromotionViews',
        'productAddsToCart',
        'productCheckouts',
        'productDetailViews',
        'productListClicks',
        'productListViews',
        'productRefunds',
        'productRemovesFromCart',
        'quantityAddedToCart',
        'quantityCheckedOut',
        'quantityRefunded',
        'quantityRemovedFromCart',
        'totalRefunds',
        'socialInteractions',
        'uniqueSocialInteractions',
        'userTimingValue',
        'userTimingSample',
        'exceptions',
        'fatalExceptions',
        'dcmFloodlightQuantity',
        'dcmClicks',
        'dcmImpressions',
        'adsenseAdUnitsViewed',
        'adsenseAdsViewed',
        'adsenseAdsClicks',
        'adsensePageImpressions',
        'adsenseExits',
        'totalPublisherImpressions',
        'totalPublisherMonetizedPageviews',
        'totalPublisherClicks',
        'backfillImpressions',
        'backfillMonetizedPageviews',
        'backfillClicks',
        'dfpImpressions',
        'dfpMonetizedPageviews',
        'dfpClicks',
        'cohortActiveUsers',
        'cohortTotalUsers',
        'cohortTotalUsersWithLifetimeCriteria',
        'dbmClicks',
        'dbmConversions',
        'dbmImpressions',
        'dsClicks',
        'dsImpressions',
    ]

    INT_DIMENSIONS_METRICS_REGEX = [
        r'^goal\w*Starts',
        r'^goal\w*Completions',
        r'^goal\w*Abandons',
        r'^contentGroupUniqueViews\w*$',
    ]

    FLOAT_DIMENSIONS_METRICS = [
        'percentNewSessions',
        'sessionsPerUser',
        'bounceRate',
        'sessionDuration',
        'avgSessionDuration',
        'adCost',
        'CPM',
        'CPC',
        'CTR',
        'costPerTransaction',
        'costPerGoalConversion',
        'costPerConversion',
        'RPC',
        'ROAS',
        'goalXXValue',
        'goalValueAll',
        'goalValuePerSession',
        'goalXXConversionRate',
        'goalConversionRateAll',
        'goalXXAbandonRate',
        'goalAbandonRateAll',
        'pageValue',
        'entranceRate',
        'pageviewsPerSession',
        'timeOnPage',
        'avgTimeOnPage',
        'exitRate',
        'avgSearchResultViews',
        'percentSessionsWithSearch',
        'avgSearchDepth',
        'percentSearchRefinements',
        'searchDuration',
        'avgSearchDuration',
        'searchExitRate',
        'searchGoalXXConversionRate',
        'searchGoalConversionRateAll',
        'goalValueAllPerSearch',
        'avgPageLoadTime',
        'avgDomainLookupTime',
        'avgPageDownloadTime',
        'avgRedirectionTime',
        'avgServerConnectionTime',
        'avgServerResponseTime',
        'avgDomInteractiveTime',
        'avgDomContentLoadedTime',
        'screenviewsPerSession',
        'timeOnScreen',
        'avgScreenviewDuration',
        'avgEventValue',
        'eventsPerSessionWithEvent',
        'transactionsPerSession',
        'transactionRevenue',
        'revenuePerTransaction',
        'transactionRevenuePerSession',
        'transactionShipping',
        'transactionTax',
        'totalValue',
        'revenuePerItem',
        'itemRevenue',
        'itemsPerPurchase',
        'localTransactionRevenue',
        'localTransactionShipping',
        'localTransactionTax',
        'localItemRevenue',
        'buyToDetailRate',
        'cartToDetailRate',
        'internalPromotionCTR',
        'localProductRefundAmount',
        'localRefundAmount',
        'productListCTR',
        'productRefundAmount',
        'productRevenuePerPurchase',
        'refundAmount',
        'revenuePerUser',
        'transactionsPerUser',
        'socialInteractionsPerSession',
        'avgUserTimingValue',
        'exceptionsPerScreenview',
        'fatalExceptionsPerScreenview',
        'metricXX',
        'dcmFloodlightRevenue',
        'dcmCPC',
        'dcmCTR',
        'dcmCost',
        'dcmROAS',
        'dcmRPC',
        'adsenseRevenue',
        'adsenseCTR',
        'adsenseECPM',
        'adsenseViewableImpressionPercent',
        'adsenseCoverage',
        'totalPublisherCoverage',
        'totalPublisherImpressionsPerSession',
        'totalPublisherViewableImpressionsPercent',
        'totalPublisherCTR',
        'totalPublisherRevenue',
        'totalPublisherRevenuePer1000Sessions',
        'totalPublisherECPM',
        'backfillCoverage',
        'backfillImpressionsPerSession',
        'backfillViewableImpressionsPercent',
        'backfillCTR',
        'backfillRevenue',
        'backfillRevenuePer1000Sessions',
        'backfillECPM',
        'dfpCoverage',
        'dfpImpressionsPerSession',
        'dfpViewableImpressionsPercent',
        'dfpCTR',
        'dfpRevenue',
        'dfpRevenuePer1000Sessions',
        'dfpECPM',
        'cohortAppviewsPerUser',
        'cohortAppviewsPerUserWithLifetimeCriteria',
        'cohortGoalCompletionsPerUser',
        'cohortGoalCompletionsPerUserWithLifetimeCriteria',
        'cohortPageviewsPerUser',
        'cohortPageviewsPerUserWithLifetimeCriteria',
        'cohortRetentionRate',
        'cohortRevenuePerUser',
        'cohortRevenuePerUserWithLifetimeCriteria',
        'cohortSessionDurationPerUser',
        'cohortSessionDurationPerUserWithLifetimeCriteria',
        'cohortSessionsPerUser',
        'cohortSessionsPerUserWithLifetimeCriteria',
        'dbmCPA',
        'dbmCPC',
        'dbmCPM',
        'dbmCTR',
        'dbmCost',
        'dbmROAS',
        'dsCPC',
        'dsCTR',
        'dsCost',
        'dsProfit',
        'dsReturnOnAdSpend',
        'dsRevenuePerClick',
    ]

    FLOAT_DIMENSIONS_METRICS_REGEX = [
        r'^goal\w*Starts$',
        r'^goal\w*Value$',
        r'^goal\w*ConversionRate$',
        r'^goal\w*AbandonRate$',
        r'^searchGoal\w*ConversionRate$',
        r'^metric\w*$',
        r'^calcMetric_\w*$',
    ]

    def __init__(self, configuration: Configuration, connection: Connection):
        self.configuration = configuration
        self.connection = connection
        self.mongodb = connection.mongodb
        self.bigquery = None

    def run(self):
        print('Running Google Analytics Module:')
        timer_run = time()

        module_configuration = self.configuration.aggregations.get_custom_configuration_aggregation('google_analytics')

        if 'configurations' in module_configuration.settings and \
                type(module_configuration.settings['configurations']) is list:
            for configuration in module_configuration.settings['configurations']:
                self._process_configuration(configuration, module_configuration.database)

        print('\ncompleted: {:s}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def _process_configuration(self, configuration: dict, database: str):
        credentials = None
        dimensions = None
        metrics = None
        segment_id = None
        table_reference = None

        if 'credentials' in configuration and type(configuration['credentials']) is str:
            credentials = service_account.Credentials.from_service_account_file(
                abspath(configuration['credentials']),
                scopes=['https://www.googleapis.com/auth/analytics.readonly']
            )

        logging.getLogger('googleapiclient.discovery').setLevel(logging.ERROR)
        self.api_service = build('analyticsreporting', 'v4', credentials=credentials, cache_discovery=False)

        if 'bigquery' == database:
            dataset = None

            if type(self.bigquery) is not BigQuery:
                self.bigquery = self.connection.bigquery

            if 'dataset' in configuration and type(configuration['dataset']) is str:
                dataset = configuration['dataset']

            if 'tablename' in configuration and type(configuration['tablename']) is str:
                table_reference = self.bigquery.table_reference(configuration['tablename'], dataset)
            else:
                raise ConfigurationMissingError('You have to set at least a table if you want to use bigquery')

        if 'dimensions' in configuration and type(configuration['dimensions']) is list:
            dimensions = configuration['dimensions']

        if 'metrics' in configuration and type(configuration['metrics']) is list:
            metrics = configuration['metrics']

        if 'dateDaysAgo' in configuration and type(configuration['dateDaysAgo']) is int:
            request_date = date.today() - timedelta(days=configuration['dateDaysAgo'])
        else:
            request_date = date.today() - timedelta(days=GoogleAnalytics.DEFAULT_DAYS_AGO)

        if 'segmentId' in configuration and (
                type(configuration['segmentId']) is str or
                type(configuration['segmentId']) is int or
                type(configuration['segmentId']) is float
        ):
            segment_id = str(configuration['segmentid'])

        if 'views' in configuration and type(configuration['views']) is list:
            for view in configuration['views']:
                try:
                    self._import_view(
                        int(view),
                        dimensions,
                        metrics,
                        segment_id,
                        request_date,
                        database,
                        table_reference
                    )

                    print(' - OK')
                except _DataAlreadyExistError:
                    print(' - EXISTS')

    def _import_view(
            self,
            view: int,
            dimensions: list,
            metrics: list,
            segment_id: str,
            request_date: date,
            database: str,
            table_reference: TableReference = None
    ):
        print('  View: {:d} ({:%Y-%m-%d})'.format(view, request_date), end='')

        if 'bigquery' == database and self._bigquery_check_has_existing_data(
                view,
                table_reference,
                request_date
        ):
            raise _DataAlreadyExistError()
        elif 'mongodb' == database and self._mongodb_check_has_existing_data(view, request_date):
            raise _DataAlreadyExistError()

        next_page_token = None

        while True:
            request = {
                'reportRequests': [
                    {
                        'viewId': str(view),
                        'samplingLevel': 'LARGE',
                        'dimensions': [{'name': dimension} for dimension in dimensions],
                        'metrics': [{'expression': metric} for metric in metrics],
                        'dateRanges': [{
                            'startDate': request_date.strftime('%Y-%m-%d'),
                            'endDate': request_date.strftime('%Y-%m-%d')
                        }],
                        'pageSize': 100000,
                    }]
            }

            if next_page_token is not None:
                request['reportRequests'][0]['pageToken'] = str(next_page_token)

            if segment_id is not None and 0 < len(segment_id):
                request['reportRequests'][0]['segments'] = [{'segmentId': segment_id}]

            response = self.api_service.reports().batchGet(body=request).execute()

            if 'reports' not in response or 0 == len(response['reports']):
                break

            column_headers = self._process_column_header(response['reports'][0]['columnHeader'])
            column_headers = [
                header for headers_list in [
                    column_headers['dimensions'],
                    column_headers['metrics']
                ] for header in headers_list
            ]

            if len(column_headers) > len(list(set(column_headers))):
                raise ConfigurationInvalidError('There are duplicates in your metrics/dimensions list')

            data = []

            for row in response['reports'][0]['data']['rows']:
                data.append([y for x in [row['dimensions'], row['metrics'][0]['values']] for y in x])

            dataframe = DataFrame(data, columns=column_headers)

            dataframe['view'] = view
            dataframe['date'] = request_date

            for column in [column for column in dataframe.columns if 'date' != column]:
                dataframe[column] = dataframe[column].astype(self._get_type_for_field(column))

            if 'bigquery' == database:
                self._process_response_rows_for_bigquery(dataframe, table_reference)
            else:
                dataframe['date'] = datetime.combine(request_date, datetime.min.time())
                self.mongodb.insert_documents(GoogleAnalytics.COLLECTION_NAME, dataframe.to_dict('records'))

            if 'nextPageToken' in response['reports'][0]:
                next_page_token = response['reports'][0]['nextPageToken']
            else:
                break

    @staticmethod
    def _process_column_header(column_header: dict) -> dict:
        ga_prefix_regex = re.compile(r'^ga:', re.IGNORECASE)

        return {
            'dimensions': [re.sub(ga_prefix_regex, '', dimension) for dimension in column_header['dimensions']],
            'metrics': [
                re.sub(ga_prefix_regex, '', metric_item['name'])
                for metric_item in column_header['metricHeader']['metricHeaderEntries']
            ]
        }

    def _process_response_rows_for_bigquery(
            self,
            dataframe: DataFrame,
            table_reference: TableReference
    ):
        job_config = LoadJobConfig()
        job_config.destination = table_reference
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')
        job_config.schema = [
            self._get_schema_for_field(column) for column in list(dataframe.columns.values)
        ]

        load_job = self.bigquery.client.load_table_from_dataframe(
            dataframe,
            table_reference,
            job_config=job_config
        )

        load_job.result()

    def _get_type_for_field(self, column: str) -> object:
        if 'date' == column:
            return date
        elif 'view' == column or column in self.INT_DIMENSIONS_METRICS or 0 < len(
                [column for regex in self.INT_DIMENSIONS_METRICS_REGEX if re.match(regex, column)]
        ):
            return int
        elif column in self.FLOAT_DIMENSIONS_METRICS or 0 < len(
                [column for regex in self.FLOAT_DIMENSIONS_METRICS_REGEX if re.match(regex, column)]
        ):
            return float

        return str

    def _get_schema_for_field(self, column: str) -> SchemaField:
        field_type = 'STRING'
        field_mode = 'REQUIRED'

        concrete_field_type = self._get_type_for_field(column)

        if date == concrete_field_type:
            field_type = 'DATE'
        elif int == concrete_field_type:
            field_type = 'INT64'
        elif float == concrete_field_type:
            field_type = 'FLOAT64'

        return SchemaField(column, field_type, field_mode)

    def _bigquery_check_has_existing_data(
            self,
            view: int,
            table_reference: TableReference,
            request_date: date
    ) -> bool:
        if not self.bigquery.has_table(table_reference.table_id, table_reference.dataset_id):
            return False

        query_job = self.bigquery.query(
            'SELECT COUNT(*) FROM `{}.{}` WHERE date = "{:%Y-%m-%d}" AND view = {:d}'.format(
                table_reference.dataset_id,
                table_reference.table_id,
                request_date,
                view
            )
        )

        count = 0

        for row in query_job.result():
            count = row[0]

        return 0 < count

    def _mongodb_check_has_existing_data(self, view: int, request_date: date) -> bool:
        if not self.mongodb.has_collection(GoogleAnalytics.COLLECTION_NAME):
            return False

        return 0 < self.mongodb.find(
            GoogleAnalytics.COLLECTION_NAME,
            {
                'view': view,
                'date': datetime.combine(request_date, datetime.min.time())
            },
            cursor=True
        ).count()
