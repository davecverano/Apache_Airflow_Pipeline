"""Composer DAG: download a CSV, load it to BigQuery, query it, export the result.

Required Airflow environment variables:
    project_name   GCP project ID
    bucket_name    GCS bucket to create/use
    env_dags_path  path to the Composer dags folder (where scripts/ lives)
    data_url       URL of the CSV to download
    source_table   BigQuery table holding the song data, e.g. example1.spotify_songs
"""
import datetime
import os

from airflow import models
from airflow.contrib.operators import bigquery_operator, bigquery_to_gcs
from airflow.operators import bash_operator

PROJECT = os.environ["project_name"]
BUCKET = os.environ["bucket_name"]
DAGS_PATH = os.environ["env_dags_path"]
SOURCE_TABLE = "`{}.{}`".format(PROJECT, os.environ["source_table"])

# Most popular non-pop artist for each year.
SQL = """
SELECT s.year, s.artist, s.popularity AS most_popular
FROM {table} AS s
INNER JOIN (
    SELECT year, MAX(popularity) AS most_popular
    FROM {table}
    WHERE NOT CONTAINS_SUBSTR(genre, "pop")
    GROUP BY year
) AS t
ON s.year = t.year AND s.popularity = t.most_popular
ORDER BY year DESC
""".format(table=SOURCE_TABLE)

DATASET_TABLE = "{}:demo_dataset.demo_table".format(PROJECT)
OUT_FILE = "gs://{}/dags/data/out_data.csv".format(BUCKET)

default_dag_args = {
    # Fixed start date: it is evaluated every time the DAG is parsed.
    "start_date": datetime.datetime(2018, 1, 1),
}

with models.DAG(
    "composer_test_dag",
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:

    create_bucket = bash_operator.BashOperator(
        task_id="create_bucket",
        bash_command="bash {{ params.dags_path }}/scripts/create_bucket.sh "
                     "{{ params.project }} {{ params.bucket }}",
        params={"project": PROJECT, "bucket": BUCKET, "dags_path": DAGS_PATH},
    )

    url_to_gcs = bash_operator.BashOperator(
        task_id="url_to_gcs",
        bash_command="bash {{ params.dags_path }}/scripts/url_to_gcs.sh "
                     "{{ params.data_url }} {{ params.bucket }}",
        params={"bucket": BUCKET, "data_url": os.environ["data_url"], "dags_path": DAGS_PATH},
    )

    load_query_table = bigquery_operator.BigQueryOperator(
        task_id="load_query_table",
        sql=SQL,
        destination_dataset_table=DATASET_TABLE,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        allow_large_results=True,
        use_legacy_sql=False,
    )

    transfer_table = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id="transfer_table",
        source_project_dataset_table=DATASET_TABLE,
        destination_cloud_storage_uris=[OUT_FILE],
        export_format="CSV",
    )

    create_bucket >> url_to_gcs >> load_query_table >> transfer_table
