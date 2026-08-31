from google.cloud.bigquery.client import Client
from google.cloud.bigquery.dataset import DatasetReference
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.job import LoadJobConfig, WriteDisposition
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import TimePartitioning, TimePartitioningType
from google.oauth2 import service_account
from os.path import abspath
from typing import Sequence


def create_client(project: str, credentials_file: str) -> Client:
    credentials = service_account.Credentials.from_service_account_file(abspath(credentials_file))
    return Client(project, credentials)


def run_query(query: str, result_function: callable):
    query_job = bigquery_client.query(query)
    row_iterator = query_job.result(page_size=25000)

    for page in row_iterator.pages:
        result_data = []

        for row in page:
            result_item = {}

            for column, value in row.items():
                result_item[column] = value

            result_data.append(result_item)

        result_function(result_data)


def upload_pagespeed_data(data: Sequence[dict]):
    job_config = LoadJobConfig()
    job_config.write_disposition = WriteDisposition.WRITE_APPEND
    job_config.time_partitioning = TimePartitioning(type_=TimePartitioningType.DAY, field=SqlTypeNames.DATE)

    loading_experience_schema_fields = (
        SchemaField('cls', SqlTypeNames.INTEGER),
        SchemaField('clsGood', SqlTypeNames.FLOAT),
        SchemaField('clsMedium', SqlTypeNames.FLOAT),
        SchemaField('clsBad', SqlTypeNames.FLOAT),
        SchemaField('lcp', SqlTypeNames.INTEGER),
        SchemaField('lcpGood', SqlTypeNames.FLOAT),
        SchemaField('lcpMedium', SqlTypeNames.FLOAT),
        SchemaField('lcpBad', SqlTypeNames.FLOAT),
        SchemaField('fcp', SqlTypeNames.INTEGER),
        SchemaField('fcpGood', SqlTypeNames.FLOAT),
        SchemaField('fcpMedium', SqlTypeNames.FLOAT),
        SchemaField('fcpBad', SqlTypeNames.FLOAT),
        SchemaField('fid', SqlTypeNames.INTEGER),
        SchemaField('fidGood', SqlTypeNames.FLOAT),
        SchemaField('fidMedium', SqlTypeNames.FLOAT),
        SchemaField('fidBad', SqlTypeNames.FLOAT),
        SchemaField('inp', SqlTypeNames.FLOAT),
        SchemaField('inpGood', SqlTypeNames.FLOAT),
        SchemaField('inpMedium', SqlTypeNames.FLOAT),
        SchemaField('inpBad', SqlTypeNames.FLOAT),
    )

    job_config.schema = (
        SchemaField('url', SqlTypeNames.STRING, 'REQUIRED'),
        SchemaField('strategy', SqlTypeNames.STRING, 'REQUIRED'),
        SchemaField('date', SqlTypeNames.DATETIME, 'REQUIRED'),
        SchemaField('statusCode', SqlTypeNames.INTEGER),
        SchemaField('cluster', SqlTypeNames.STRING, 'REQUIRED'),
        SchemaField('labdata', SqlTypeNames.RECORD, 'REQUIRED', fields=(
            SchemaField('cls', SqlTypeNames.FLOAT),
            SchemaField('lcp', SqlTypeNames.FLOAT),
            SchemaField('fcp', SqlTypeNames.FLOAT),
            SchemaField('tbt', SqlTypeNames.FLOAT),
            SchemaField('mpfid', SqlTypeNames.FLOAT),
            SchemaField('ttfb', SqlTypeNames.FLOAT),
            SchemaField('performanceScore', SqlTypeNames.FLOAT),
            SchemaField('serverResponseTime', SqlTypeNames.FLOAT),
            SchemaField('usesTextCompression', SqlTypeNames.FLOAT),
            SchemaField('usesLongCacheTtl', SqlTypeNames.FLOAT),
            SchemaField('domSize', SqlTypeNames.FLOAT),
            SchemaField('offscreenImages', SqlTypeNames.FLOAT),
            SchemaField('usesOptimizedImages', SqlTypeNames.FLOAT),
            SchemaField('usesResponsiveImages', SqlTypeNames.FLOAT),
            SchemaField('renderBlockingResources', SqlTypeNames.FLOAT),
            SchemaField('bootupTime', SqlTypeNames.FLOAT),
            SchemaField('mainthreadWorkBreakdown', SqlTypeNames.FLOAT),
        )),
        SchemaField(
            'originLoadingExperience',
            SqlTypeNames.RECORD,
            'REQUIRED',
            fields=loading_experience_schema_fields
        ),
        SchemaField('loadingExperience', SqlTypeNames.RECORD, fields=loading_experience_schema_fields)
    )

    for data_item in data:
        data_item['date'] = data_item['date'].strftime('%Y-%m-%dT%H:%M:%S.%f')

    load_job = bigquery_client.load_table_from_json(data, target_table_reference, job_config=job_config)
    load_job.result()


project = 'project-id'
dataset = 'yourdataset'
source_table = 'old_pagespeed_table'
target_table = 'new_pagespeed_table'

bigquery_client = create_client(project, './your-credentials-file-path.json')

target_table_reference = DatasetReference(
    project,
    dataset
).table(target_table)

run_query('SELECT * FROM `{dataset}.{table}`'.format(dataset=dataset, table=source_table), upload_pagespeed_data)
