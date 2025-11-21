from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import json

# --- CONFIG ---
LAMBDA_FUNCTION_NAME = 'nyctaxi-dev-func-ingest'
GLUE_RAW_CRAWLER     = 'nyctaxi-dev-crawl-raw'
GLUE_JOB_NAME        = 'nyctaxi-dev-job-transform'
GLUE_PROCESSED_CRAWLER = 'nyctaxi-dev-crawl-processed'
AWS_REGION           = 'ap-southeast-1'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
}

with DAG(
    dag_id='nyctaxi_pipeline_master',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['production', 'nyc_taxi'],
) as dag:

    wait_for_ingest = BashOperator(
        task_id='wait_for_data',
        bash_command='sleep 200',
    )

    # 1. Trigger Lambda
    ingest_task = LambdaInvokeFunctionOperator(
        task_id='1_ingest_lambda',
        function_name=LAMBDA_FUNCTION_NAME,
        invocation_type='Event',
        payload=json.dumps({"trigger": "airflow"}),
        region_name=AWS_REGION,
    )

    # 2. Crawler Raw
    crawl_raw_task = GlueCrawlerOperator(
        task_id='2_crawl_raw',
        config={'Name': GLUE_RAW_CRAWLER},
        wait_for_completion=True,
        region_name=AWS_REGION,
    )

    # 3. Glue ETL Job
    etl_task = GlueJobOperator(
        task_id='3_run_glue_job',
        job_name=GLUE_JOB_NAME,
        wait_for_completion=True,
        verbose=True,
        region_name=AWS_REGION,
    )

    # 4. Crawler Processed
    crawl_proc_task = GlueCrawlerOperator(
        task_id='4_crawl_processed',
        config={'Name': GLUE_PROCESSED_CRAWLER},
        wait_for_completion=True,
        region_name=AWS_REGION,
    )

    ingest_task >> wait_for_ingest >>  crawl_raw_task >> etl_task >> crawl_proc_task

