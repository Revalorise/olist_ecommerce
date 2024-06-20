from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def generate_extract_tasks(bucket: str,
                           source_object: str,
                           dag):
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id=f"gcs_to_bigquery_{source_object}",
        bucket=bucket,
        source_objects=source_object,
        destination_project_dataset_table=f"olist_ecommerce.{source_object}",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )