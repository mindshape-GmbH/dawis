from database.connection import Connection
from database.bigquery import BigQuery
from utilities.configuration import Configuration
from utilities.exceptions import ConfigurationMissingError
from googleapiclient.discovery import build, Resource
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import LoadJobConfig, TimePartitioning, TimePartitioningType, TableReference, SchemaField
from google.cloud.bigquery.job import WriteDisposition
from google.oauth2 import service_account
from pandas import DataFrame, Series, concat
from dict_hash import sha256
from os.path import abspath
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from time import time
from calendar import monthrange


class _DataAlreadyExistError(Exception):
    pass


class _DataNotAvailableYet(Exception):
    pass


class GoogleSearchConsole:
    COLLECTION_NAME = 'google_search_console'
    COLLECTION_NAME_CACHE = 'google_search_console_cache'
    COLLECTION_NAME_RETRY = 'google_search_console_retry'
    ROW_LIMIT = 25000
    DEFAULT_DIMENSIONS = ['page', 'device', 'query', 'country']
    DEFAULT_SEARCHTYPES = ['web', 'image', 'video']
    DEFAULT_SEARCHTYPE = 'web'

    def __init__(self, configuration: Configuration, connection: Connection):
        self.configuration = configuration
        self.connection = connection
        self.mongodb = connection.mongodb
        self.bigquery = None

    def run(self):
        print('Running aggregation GSC Importer:')
        timer_run = time()

        configuration = self.configuration.aggregations.get_custom_configuration_aggregation('google_search_console')
        import_properties = []

        if self.mongodb.has_collection(GoogleSearchConsole.COLLECTION_NAME_RETRY):
            for retry in self.mongodb.find(
                    GoogleSearchConsole.COLLECTION_NAME_RETRY,
                    {'module': 'aggregation'},
                    True
            ):
                del retry['module']
                retry['requestDate'] = retry['requestDate'].date()
                import_properties.append(retry)

        if 'properties' in configuration.settings and type(configuration.settings['properties']) is list:
            for property_configuration in configuration.settings['properties']:
                credentials = None
                request_days_ago = 3
                dimensions = GoogleSearchConsole.DEFAULT_DIMENSIONS
                search_types = GoogleSearchConsole.DEFAULT_SEARCHTYPES
                previous_data = []

                if 'property' in property_configuration and type(property_configuration['property']) is str:
                    gsc_property = property_configuration['property']
                else:
                    raise ConfigurationMissingError('property is missing')

                if 'credentials' in property_configuration and type(property_configuration['credentials']) is str:
                    credentials = property_configuration['credentials']

                if 'dateDaysAgo' in property_configuration and type(property_configuration['dateDaysAgo']) is int:
                    request_days_ago = property_configuration['dateDaysAgo']

                if 'dimensions' in property_configuration and type(property_configuration['dimensions']) is int:
                    dimensions = property_configuration['dimensions']

                if 'searchTypes' in property_configuration and type(property_configuration['searchTypes']) is list:
                    search_types = property_configuration['searchTypes']

                if 'previousData' in property_configuration and \
                        type(property_configuration['previousData']) is list:
                    previous_data = property_configuration['previousData']

                if 'aggregationType' in property_configuration and \
                        type(property_configuration['aggregationType']) is str:
                    aggregation_type = property_configuration['aggregationType']
                else:
                    aggregation_type = ''

                request_date = date.today() - timedelta(days=request_days_ago)
                table_name = None
                dataset_name = None

                if 'bigquery' == configuration.database:
                    if 'tablename' in property_configuration and type(property_configuration['tablename']) is str:
                        table_name = property_configuration['tablename']
                    else:
                        raise ConfigurationMissingError('Missing tablename for gsc import to bigquery')

                    if 'dataset' in property_configuration and type(property_configuration['dataset']) is str:
                        dataset_name = property_configuration['dataset']

                    if type(self.bigquery) is not BigQuery:
                        self.bigquery = self.connection.bigquery

                import_property = {
                    'credentials': credentials,
                    'property': gsc_property,
                    'requestDate': request_date,
                    'dimensions': dimensions,
                    'searchTypes': search_types,
                    'previousData': previous_data,
                    'aggregationType': aggregation_type,
                    'database': configuration.database,
                    'tableName': table_name,
                    'datasetName': dataset_name,
                }

                if 0 == len(list(filter(lambda x: x == import_property, import_properties))):
                    import_properties.append(import_property)

        for import_property in import_properties:
            try:
                credentials = None

                if 'credentials' in import_property and type(import_property['credentials']) is str:
                    credentials = service_account.Credentials.from_service_account_file(
                        abspath(import_property['credentials']),
                        scopes=['https://www.googleapis.com/auth/webmasters.readonly']
                    )

                api_service = build('webmasters', 'v3', credentials=credentials, cache_discovery=False)

                self.import_property(
                    api_service,
                    import_property['property'],
                    import_property['requestDate'],
                    import_property['dimensions'],
                    import_property['searchTypes'],
                    import_property['previousData'],
                    import_property['aggregationType'],
                    import_property['database'],
                    import_property['tableName'],
                    import_property['datasetName']
                )

                if '_id' in import_property:
                    self.mongodb.delete_one(GoogleSearchConsole.COLLECTION_NAME_RETRY, import_property['_id'])
            except _DataAlreadyExistError:
                print(' !!! already exists')
            except _DataNotAvailableYet:
                print(' !!! not available yet')

                existing_retry = None

                if self.mongodb.has_collection(GoogleSearchConsole.COLLECTION_NAME_RETRY):
                    existing_retry = self.mongodb.find_one(GoogleSearchConsole.COLLECTION_NAME_RETRY, {
                        'property': import_property['property'],
                        'requestDate': datetime.combine(import_property['requestDate'], datetime.min.time()),
                    })

                if existing_retry is None:
                    self.mongodb.insert_document(GoogleSearchConsole.COLLECTION_NAME_RETRY, {
                        'module': 'aggregation',
                        'credentials': import_property['credentials'],
                        'property': import_property['property'],
                        'requestDate': datetime.combine(import_property['requestDate'], datetime.min.time()),
                        'dimensions': import_property['dimensions'],
                        'searchTypes': import_property['searchTypes'],
                        'previousData': import_property['previousData'],
                        'aggregationType': import_property['aggregationType'],
                        'database': import_property['database'],
                        'tableName': import_property['tableName'],
                        'datasetName': import_property['datasetName'],
                    })

        print('\ncompleted: {:s}'.format(str(timedelta(seconds=int(time() - timer_run)))))

    def import_property(
            self,
            api_service: Resource,
            gsc_property: str,
            request_date: date,
            dimensions: list,
            search_types: list,
            previous_data: list,
            aggregation_type: str,
            database: str,
            table_name: str,
            dataset_name: str = None
    ):
        table_reference = self.bigquery.table_reference(table_name, dataset_name)
        previous_dates = {}
        cache_hash = sha256({'property': gsc_property, 'dimensions': dimensions, 'date': datetime.now().isoformat()})

        print(' - Property: "{:s}"'.format(gsc_property), end='')

        for previous_data_item in previous_data:
            if 'year' == previous_data_item:
                previous_date = request_date - relativedelta(years=1)
                previous_dates['PreviousYear'] = {
                    'startDate': previous_date,
                    'endDate': previous_date,
                }
            if 'month' == previous_data_item:
                previous_date = request_date - relativedelta(months=1)
                previous_dates['PreviousMonth'] = {
                    'startDate': previous_date.replace(day=1),
                    'endDate': previous_date.replace(day=monthrange(previous_date.year, previous_date.month)[1]),
                }
            if 'week' == previous_data_item:
                previous_date = request_date - relativedelta(weeks=1)
                previous_dates['PreviousWeek'] = {
                    'startDate': previous_date,
                    'endDate': previous_date,
                }
            if 'day' == previous_data_item:
                previous_date = request_date - relativedelta(days=1)
                previous_dates['PreviousDay'] = {
                    'startDate': previous_date,
                    'endDate': previous_date,
                }

        if 'bigquery' == database and self._bigquery_check_has_existing_data(
                gsc_property,
                table_reference,
                request_date
        ):
            raise _DataAlreadyExistError()
        elif 'mongodb' == database and self._mongodb_check_has_existing_data(gsc_property, request_date):
            raise _DataAlreadyExistError()

        print('\n   + {:%Y-%m-%d} -> {:%Y-%m-%d}'.format(request_date, request_date), end='')

        timer_base = time()

        for search_type in search_types:
            iteration_count = 0

            while True:
                request = {
                    'startDate': request_date.strftime('%Y-%m-%d'),
                    'endDate': request_date.strftime('%Y-%m-%d'),
                    'searchType': search_type,
                    'dimensions': dimensions,
                    'rowLimit': GoogleSearchConsole.ROW_LIMIT,
                    'startRow': GoogleSearchConsole.ROW_LIMIT * iteration_count
                }

                if 0 < len(aggregation_type):
                    request['aggregationType'] = aggregation_type

                response = api_service.searchanalytics().query(siteUrl=gsc_property, body=request).execute()

                if 'rows' not in response:
                    if 0 == iteration_count and (len(search_types) - 1) == search_types.index(search_type):
                        cache_entry = self.mongodb.find_one(
                            GoogleSearchConsole.COLLECTION_NAME_CACHE,
                            {'hash': cache_hash},
                            True
                        )

                        if cache_entry is None:
                            raise _DataNotAvailableYet()

                    break

                self._cache_rows(
                    cache_hash,
                    gsc_property,
                    response['rows'],
                    previous_dates,
                    request_date,
                    dimensions,
                    search_type
                )

                if len(response['rows']) < GoogleSearchConsole.ROW_LIMIT:
                    break

                iteration_count = iteration_count + 1

        print(' - OK - {:s}'.format(str(timedelta(seconds=int(time() - timer_base)))))

        for previous_data_column, previous_date in previous_dates.items():
            print(
                '   + {:%Y-%m-%d} -> {:%Y-%m-%d}'.format(
                    previous_date['startDate'],
                    previous_date['endDate']
                ),
                end=''
            )

            timer_previous = time()

            for search_type in search_types:
                iteration_count = 0

                while True:
                    request = {
                        'startDate': previous_date['startDate'].strftime('%Y-%m-%d'),
                        'endDate': previous_date['endDate'].strftime('%Y-%m-%d'),
                        'searchType': search_type,
                        'dimensions': dimensions,
                        'rowLimit': GoogleSearchConsole.ROW_LIMIT,
                        'startRow': GoogleSearchConsole.ROW_LIMIT * iteration_count
                    }

                    if 0 < len(aggregation_type):
                        request['aggregationType'] = aggregation_type

                    response = api_service.searchanalytics().query(siteUrl=gsc_property, body=request).execute()

                    if 'rows' not in response:
                        break

                    self._add_previous_data(
                        cache_hash,
                        search_type,
                        previous_data_column,
                        response['rows'],
                        dimensions,
                    )

                    if len(response['rows']) < GoogleSearchConsole.ROW_LIMIT:
                        break

                    iteration_count = iteration_count + 1

            print(' - OK - {:s}'.format(str(timedelta(seconds=int(time() - timer_previous)))))

        offset = 0

        while True:
            rows = self.mongodb.find(
                GoogleSearchConsole.COLLECTION_NAME_CACHE,
                {'hash': cache_hash},
                True,
                GoogleSearchConsole.ROW_LIMIT,
                offset
            )

            if 0 == len(rows):
                break

            offset += GoogleSearchConsole.ROW_LIMIT

            self._import_rows(database, rows, table_reference)

        self._clear_cache(cache_hash)

    def _add_previous_data(
            self,
            cache_hash: str,
            search_type: str,
            previous_data_column: str,
            rows: list,
            dimensions: list,
    ):
        clicks_column = 'clicks' + previous_data_column
        impressions_column = 'impressions' + previous_data_column

        for previous_row in rows:
            current_row = self.mongodb.find_one(
                GoogleSearchConsole.COLLECTION_NAME_CACHE,
                {
                    'hash': cache_hash,
                    'searchType': search_type,
                    'dimensions': self._process_dimensions_column(previous_row['keys'], dimensions)
                }
            )

            if type(current_row) is dict:
                previous_clicks = None
                previous_impressions = None

                if 'clicks' in previous_row and previous_row['clicks'] is not None:
                    previous_clicks = previous_row['clicks']

                if 'impressions' in previous_row and previous_row['impressions'] is not None:
                    previous_impressions = previous_row['impressions']

                if previous_clicks is not None or previous_impressions is not None:
                    self.mongodb.update_one(
                        GoogleSearchConsole.COLLECTION_NAME_CACHE,
                        current_row['_id'],
                        {
                            clicks_column: previous_clicks,
                            impressions_column: previous_impressions
                        }
                    )

    def _cache_rows(
            self,
            cache_hash: str,
            gsc_property: str,
            rows: list,
            previous_dates: dict,
            request_date: date,
            dimensions: list,
            search_type: str
    ):
        documents = []
        mongodb = self.connection.mongodb
        previous_clicks_columns = {
            'clicks' + previous_date_column: None
            for previous_date_column, _ in previous_dates.items()
        }
        previous_impressions_columns = {
            'impressions' + previous_date_column: None
            for previous_date_column, _ in previous_dates.items()
        }

        for row in rows:
            documents.append({
                **{
                    'hash': cache_hash,
                    'property': gsc_property,
                    'date': datetime.combine(request_date, datetime.min.time()),
                    'clicks': row['clicks'],
                    'impressions': row['impressions'],
                    'ctr': row['ctr'],
                    'position': row['position'],
                    'searchType': search_type,
                    'dimensions': self._process_dimensions_column(row['keys'], dimensions),
                },
                **previous_clicks_columns,
                **previous_impressions_columns,
            })

        mongodb.insert_documents(GoogleSearchConsole.COLLECTION_NAME_CACHE, documents)

    def _clear_cache(self, cache_hash: str):
        self.mongodb.get_collection(GoogleSearchConsole.COLLECTION_NAME_CACHE).delete_many({'hash': cache_hash})

    def _import_rows(
            self,
            database: str,
            rows: list,
            table_reference: TableReference = None
    ):
        for row in rows:
            row.pop('_id')
            row.pop('hash')

        if 'bigquery' == database:
            self.process_response_rows_for_bigquery(rows, table_reference)
        else:
            self.mongodb.insert_documents(GoogleSearchConsole.COLLECTION_NAME, rows)

    def process_response_rows_for_bigquery(
            self,
            rows: list,
            table_reference: TableReference
    ):
        rows_dataframe = DataFrame.from_records(rows)

        rows_dataframe = concat([rows_dataframe, rows_dataframe['dimensions'].apply(Series)], axis=1, join='inner')
        rows_dataframe = rows_dataframe.drop(['dimensions'], axis=1)
        rows_dataframe['date'] = rows_dataframe['date'].apply(lambda x: x.date())

        job_config = LoadJobConfig()
        job_config.destination = table_reference
        job_config.write_disposition = WriteDisposition.WRITE_APPEND
        job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field='date')
        job_config.schema = [
            self._get_schema_for_field(column) for column in list(rows_dataframe.columns.values)
        ]

        try:
            load_job = self.bigquery.client.load_table_from_dataframe(
                rows_dataframe,
                table_reference,
                job_config=job_config
            )

            load_job.result()
        except BadRequest as error:
            print(error.errors)

    @staticmethod
    def _get_schema_for_field(column: str):
        field_type = 'STRING'
        field_mode = 'REQUIRED'

        if 'date' == column:
            field_type = 'DATE'

        if column.startswith('impressions') \
                or column.startswith('clicks') \
                or column.startswith('ctr') \
                or column.startswith('position'):
            field_type = 'FLOAT64'

        if -1 < column.find('Previous'):
            field_mode = 'NULLABLE'

        return SchemaField(column, field_type, field_mode)

    def _bigquery_check_has_existing_data(
            self,
            gsc_property: str,
            table_reference: TableReference,
            request_date: date
    ) -> bool:
        if not self.bigquery.has_table(table_reference.table_id, table_reference.dataset_id):
            return False

        query_job = self.bigquery.query(
            'SELECT COUNT(*) FROM `' + table_reference.dataset_id + '.' + table_reference.table_id + '` ' +
            'WHERE date = "' + request_date.strftime('%Y-%m-%d') + '" ' +
            'AND property = "' + gsc_property + '"'
        )

        count = 0

        for row in query_job.result():
            count = row[0]

        return 0 < count

    def _mongodb_check_has_existing_data(self, gsc_property: str, request_date: date) -> bool:
        if not self.mongodb.has_collection(GoogleSearchConsole.COLLECTION_NAME):
            return False

        return 0 < self.mongodb.find(
            GoogleSearchConsole.COLLECTION_NAME,
            {
                'property': gsc_property,
                'date': datetime.combine(request_date, datetime.min.time())
            },
            True
        ).count()

    @staticmethod
    def _process_dimensions_column(dimension_column: list, dimensions_list: list):
        return {dimensions_list[index]: dimension for index, dimension in enumerate(dimension_column)}
