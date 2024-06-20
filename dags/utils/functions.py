from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

def generate_extract_tasks(bucket: str,
                           source_object: str, dag):
    table_name = source_object.split(".")[0]
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id=f"gcs_to_bigquery_{source_object}",
        gcp_conn_id="google_cloud_default",
        bucket=bucket,
        source_objects=source_object,
        destination_project_dataset_table=f"raw_olist_ecommerce.{table_name}",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )