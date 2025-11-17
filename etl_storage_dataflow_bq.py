from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

PROJECT_ID = "qwiklabs-gcp-01-bbbed41eaadf"
GCS_BUCKET = "qwiklabs-gcp-01-bbbed41eaadf"
BQ_TABLE = f"{PROJECT_ID}:etl_dataset.transformed_data"
DATAFLOW_REGION = 'us-east4' # Define the Dataflow region

default_args = {
    'start_date': datetime(2025, 11, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='daily_etl_storage_to_bq',
    schedule_interval='0 12 * * *',  # 12:00 PM UTC daily
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=['etl', 'dataflow']
) as dag:

    run_dataflow = DataflowStartFlexTemplateOperator(
        task_id='run_dataflow_job',
        location=DATAFLOW_REGION, # This is correct
        body={
            "launchParameter": {
                "jobName": "etl-storage-to-bq-{{ ts_nodash.replace('T', '-') }}",
                
                # --- FIX IS ON THIS LINE ---
                # Use the regional template bucket AND the correct file name
                "containerSpecGcsPath": f"gs://dataflow-templates-{DATAFLOW_REGION}/latest/flex/File_to_BigQuery",
                
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
        # This SQL checks if the table has at least 1 row.
        sql=f"SELECT COUNT(*) > 0 FROM `{PROJECT_ID}.etl_dataset.transformed_data`",
        use_legacy_sql=False
    )

    # Set the task dependency
    run_dataflow >> check_bq