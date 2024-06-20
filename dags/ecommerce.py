from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from utils.functions import generate_extract_tasks


PROJECT_ID = "steam-key-426305-n4"
BUCKET_NAME="steam-key-426305-n4-olist-ecommerce"
source_objects = [
    "olist_customers_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
]

args = {
    'owner': 'chaichana'
}


with DAG(
    'olist_ecommerce',
    default_args=args,
    start_date=days_ago(1),
    catchup=False,
    tags=["ecommerce"]
) as dag:

    with TaskGroup(group_id='gcs_to_bigquery') as task_group_1:
        for source_object in source_objects:
            generate_extract_tasks(
                bucket=BUCKET_NAME,
                source_object=source_object,
                dag=dag,
            )




