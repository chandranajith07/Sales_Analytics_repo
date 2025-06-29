# airflow/dags/retail_sales_pipeline.py

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum


def detect_sales_drop(threshold, **context):
    from google.cloud import bigquery
    client = bigquery.Client()

    query = """
        SELECT
            CURRENT_DATE() AS today,
            SUM(today_sales) AS today_sales,
            SUM(yesterday_sales) AS yesterday_sales,
            SAFE_DIVIDE(SUM(today_sales), SUM(yesterday_sales)) - 1 AS pct_change
        FROM (
            SELECT
                CASE WHEN sale_date = CURRENT_DATE() THEN amount ELSE 0 END AS today_sales,
                CASE WHEN sale_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) THEN amount ELSE 0 END AS yesterday_sales
            FROM `deep-chimera-459105-q6.sales_dataset.sales_fact`
        )
    """
    result = client.query(query).to_dataframe()
    pct_change = result["pct_change"].iloc[0]

    if pct_change < -threshold:
        context['ti'].xcom_push(key='alert_message', value=f"Sales dropped by {pct_change:.2%} compared to yesterday!")
        return True
    return False


def generate_alert_message(**context):
    msg = context['ti'].xcom_pull(task_ids='check_sales_drop', key='alert_message')
    context['ti'].xcom_push(key='pubsub_message', value={"data": {"message": msg}})


def publish_to_pubsub(**context):
    from google.cloud import pubsub_v1

    project_id = 'deep-chimera-459105-q6'
    topic_id = 'sales-alerts'
    message = context['ti'].xcom_pull(task_ids='generate_alert_message', key='pubsub_message')

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    data = message['data']['message'].encode("utf-8")
    publisher.publish(topic_path, data=data)


default_args = {
    'owner': 'retail_team',
    'depends_on_past': False,
    'retries': 1
}

with DAG(
    dag_id='retail_sales_pipeline',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # daily at 1AM
    start_date=days_ago(1),
    catchup=False,
    tags=['sales', 'retail'],
) as dag:

    list_sales_files = GCSListObjectsOperator(
        task_id='list_sales_files',
        bucket='retail_sales_bucket_01',
        prefix='daily/',
        gcp_conn_id='google_cloud_default'
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id='load_sales_to_bq',
        bucket='retail_sales_bucket_01',
        source_objects=['daily/sales_{{ macros.ds_add(ds, -1) }}.csv'],
        destination_project_dataset_table='deep-chimera-459105-q6.sales_dataset.sales_staging',
        schema_fields=[
            {'name': 'sale_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'store_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'product_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'sale_date', 'type': 'DATE', 'mode': 'REQUIRED'}
        ],
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
        gcp_conn_id='google_cloud_default'
    )

    run_dbt_transform = BigQueryInsertJobOperator(
        task_id='run_dbt_transforms',
        configuration={
            "query": {
                "query": "CALL `deep-chimera-459105-q6.sales_dataset.sp_transform_sales`()",
                "useLegacySql": False
            }
        },
        location='US'
    )

    check_sales_drop = PythonOperator(
        task_id='check_sales_drop',
        python_callable=detect_sales_drop,
        op_kwargs={'threshold': 0.2},
        provide_context=True
    )

    generate_alert_task = PythonOperator(
        task_id='generate_alert_message',
        python_callable=generate_alert_message,
        provide_context=True,
    )

    send_alert = PythonOperator(
        task_id='send_alert',
        python_callable=publish_to_pubsub,
        provide_context=True,
        trigger_rule='one_success'
    )

    list_sales_files >> load_to_bq >> run_dbt_transform >> check_sales_drop >> generate_alert_task >> send_alert
