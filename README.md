# Airflow Pipeline on Google Cloud Composer

A practice data pipeline built with Apache Airflow on Google Cloud Composer. It downloads a CSV of Spotify songs, loads it into BigQuery, runs an analysis query and exports the result back to Cloud Storage.

![DAG runs](https://user-images.githubusercontent.com/49330823/180494629-e4ac049b-dd49-416e-9a6c-e7200607b98c.JPG)
![Airflow tree view](https://user-images.githubusercontent.com/49330823/180496356-58840724-9034-4b4f-b233-3ce284b5e393.JPG)

*(Yes, there were a lot of failed runs on the way. 😆)*

## How it works

The `composer_test_dag` DAG runs daily:

1. **create_bucket**: creates the GCS bucket if it doesn't exist (`scripts/create_bucket.sh`)
2. **url_to_gcs**: streams the CSV from a URL into the bucket (`scripts/url_to_gcs.sh`)
3. **load_query_table**: queries BigQuery for the most popular non-pop artist per year and writes the result to `demo_dataset.demo_table`
4. **transfer_table**: exports that table to `gs://<bucket>/dags/data/out_data.csv`

`data/in_data.csv` is the sample input (2,000 songs, 1999–2020) and `data/out_data.csv` is the output it produced.

`airflow_monitoring.py` is the liveness-check DAG that Cloud Composer adds to every environment.

## Repository layout

```
DAG.py                  the pipeline DAG
airflow_monitoring.py   Composer's default health-check DAG
scripts/                bash helpers called by the DAG
data/                   sample input and output
```

## Setup

1. Create a Cloud Composer (Airflow 1.10) environment.
2. Load `data/in_data.csv` into a BigQuery table, e.g. `example1.spotify_songs`.
3. Set these Airflow environment variables on the Composer environment:

   | Variable | Example |
   |---|---|
   | `project_name` | `my-gcp-project` |
   | `bucket_name` | `my-pipeline-bucket` |
   | `env_dags_path` | `/home/airflow/gcs/dags` |
   | `data_url` | URL of the CSV to download |
   | `source_table` | `example1.spotify_songs` |

4. Upload `DAG.py` and the `scripts/` folder to the environment's `dags/` bucket.

## Built with

Apache Airflow · Google Cloud Composer · BigQuery · Cloud Storage · Bash · Python
