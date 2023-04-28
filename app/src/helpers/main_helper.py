from src import config
from google.cloud import storage, bigquery
from google.cloud.bigquery import ExternalConfig, Table


def upload_files_to_gcs(files):
    client = storage.Client()
    bucket = client.get_bucket(config.FILES_BUCKET)

    for file in files:
        blob = bucket.blob(file.filename)
        blob.upload_from_file(file)


def create_external_bq_table(file_names):
    external_tables_id = []

    for file_name in file_names:
        external_config = ExternalConfig('CSV')
        external_config.source_uris = f'gs://{config.FILES_BUCKET}/{file_name}'
        external_config.skip_leading_rows = 1
        external_config.autodetect = True

        table_id = f'{config.PROJECT_NAME}.globant.{file_name.replace(".csv", "_EXT")}'

        table = Table(table_id)
        table.external_data_configuration = external_config

        bq_client = bigquery.Client()
        bq_client.create_table(table)
        external_tables_id.append(table_id)

    return external_tables_id

def create_bq_table(file_names):
    for file_name in file_names:
        table_id = f'{config.PROJECT_NAME}.globant.{file_name.replace(".csv", "")}'
        uri = f'gs://{config.FILES_BUCKET}/{file_name}'
        job_config = bigquery.LoadJobConfig(skip_leading_rows=1, autodetect=True, source_format=bigquery.SourceFormat.CSV)

        client = bigquery.Client()
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()




def create_materialized_bq_table(ext_table_names):
    for ext_table in ext_table_names:
        job_config = bigquery.QueryJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.use_legacy_sql = False
        job_config.destination = ext_table.replace('_EXT', '')

        query = f'SELECT * FROM {ext_table}'

        client = bigquery.Client()
        client.query(query=query, job_config=job_config)

def export_dataset_tables_to_gcs():
    client = bigquery.Client()
    tables = client.list_tables(config.BIG_QUERY_DATASET)

    for table in tables:
        destination_uri = f"gs://{config.BACKUP_BUCKET}/{table.table_id}.avro"
        extract_job_config = bigquery.ExtractJobConfig()
        extract_job_config.destination_format = 'AVRO'
        extract_job = client.extract_table(table, destination_uri, job_config=extract_job_config, location="US")
        extract_job.result()


def import_dataset_tables_from_gcs():
    bq_client = bigquery.Client()
    gcs_client = storage.Client()

    blobs = gcs_client.list_blobs(config.BACKUP_BUCKET)
    for blob in blobs:
        table_id = f"{config.PROJECT_NAME}.{config.BIG_QUERY_DATASET}.{blob.name.replace('.avro', '')}"

        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, source_format=bigquery.SourceFormat.AVRO)
        uri = f"gs://{config.BACKUP_BUCKET}/{blob.name}"

        load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()