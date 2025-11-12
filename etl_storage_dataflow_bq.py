from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

PROJECT_ID = "qwiklabs-gcp-00-29278fa02636"
GCS_BUCKET = "etl-storage-bucket-qwiklabs-gcp-00-29278fa02636"
BQ_TABLE = f"{PROJECT_ID}:etl_dataset.transformed_data"

default_args = {
    'start_date': datetime(2025, 11, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='daily_etl_storage_to_bq',
    schedule_interval='0 12 * * *',  
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=['etl','dataflow']
) as dag:

    run_dataflow = DataflowStartFlexTemplateOperator(
        task_id='run_dataflow_job',
        location='europe-west1',
        body={
            "launchParameter": {
                "jobName": "etl-storage-to-bq-{{ ts_nodash }}",
                "containerSpecGcsPath": "gs://dataflow-templates/latest/flex/File_to_BigQuery_Flex",
                "parameters": {
                    "inputFilePattern": f"gs://{GCS_BUCKET}/input/*.csv",
                    "outputTable": BQ_TABLE,
                    "bigQueryLoadingTemporaryDirectory": f"gs://{GCS_BUCKET}/temp/",
                    "delimiter": ",",
                    "schemaFilePath": f"gs://{GCS_BUCKET}/schema/schema.json",
                    "skipLeadingRows": "0"
                }
            }
        }
    )

    check_bq = BigQueryCheckOperator(
        task_id='check_bq_data',
        sql=f"SELECT COUNT(*) FROM `{PROJECT_ID}.etl_dataset.transformed_data`",
        use_legacy_sql=False
    )

    run_dataflow >> check_bq
