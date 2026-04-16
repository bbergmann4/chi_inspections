import os
import json
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
import subprocess

def get_bq_client_from_env():
    """
    Creates a BigQuery client using service account JSON
    stored in an environment variable.
    """
    sa_json = os.environ.get("GCP_SERVICE_CRED")
    if not sa_json:
        raise ValueError("Missing GCP_SERVICE_CRED GitHub Secret or environment variable")

    credentials_info = json.loads(sa_json)
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info
    )

    project_id = credentials_info.get("project_id")
    return bigquery.Client(credentials=credentials, project=project_id)

def drop_tables(dataset_id, table_ids):
    """
    Drops a list of tables from a dataset.

    Args:
        dataset_id (str): Dataset name (not full path)
        table_ids (list[str]): List of table names
    """
    client = get_bq_client_from_env()

    for table_id in table_ids:
        full_table_id = f"{client.project}.{dataset_id}.{table_id}"
        try:
            client.delete_table(full_table_id, not_found_ok=True)
            print(f"Deleted table: {full_table_id}")
        except Exception as e:
            print(f"Error deleting {full_table_id}: {e}")

def delete_dataset(dataset_id):
    client = get_bq_client_from_env()
    dataset_ref = f"{client.project}.{dataset_id}"

    client.delete_dataset(dataset_ref, delete_contents=False, not_found_ok=True)
    print(f"Deleted dataset: {dataset_ref}")


def main():
    tables = [('ingest', 'inspections_raw'), 
                    ('ingest', '_dlt_loads'),
                    ('ingest', '_dlt_pipeline_state'),
                    ('ingest', '_dlt_version'),
                    ('stage','inspection'), 
                    ('stage', 'licensee'), 
                    ('stage', 'location'), 
                    ('report', 'inspection_by_licensee'), 
                    ('report','inspection_by_month')]
    datasets = ['ingest', 'stage','report']
    print("WARNING!  This script will delete all tables created in this pipeline.  Make sure you have backed up any data you want to keep before running this script.")
    print("Process will delete the contents and then drop the following tables:")
    for table in tables:
        print(table[0]+"."+table[1])
    print("Then it will permanently delete the following datasets")
    for dataset in datasets:
        print(dataset)
    consent = input("Press Y to proceeed: ")
    if consent.upper() != "Y":
        sys.exit("Aborting cleanup process.")
     
     ## Optional:  Run bruin clean to remove temp before deleting the main tables and datasets.  Will disable .bruin/uv.  
    """
    try:
        command = ["bruin", "clean"]
        timeout_seconds = 30 
        result = subprocess.run(command, timeout=timeout_seconds, text=True, capture_output=True)
        print(result.stdout)
    except Exception as e:
        print(f"Error: executing bruin clean: {e}")
    """
    # Drop tables and delete datasets
    for table in tables:
        print(f"Cleaning up table: {table[0]}.{table[1]}")
        drop_tables(table[0], [table[1]])
    for dataset in datasets:
        print(f"Cleaning up dataset: {dataset}")
        delete_dataset(dataset)



if __name__ == "__main__":
    main()
