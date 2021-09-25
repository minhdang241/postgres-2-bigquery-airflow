import argparse
import logging
import os

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s")

def load_data_from_GCS(uri: str, table_id: str, job_config):

    # Make an API request.
    load_job = client.load_table_from_uri(
                uri, table_id, job_config=job_config
    )  

    # Waits for the job to complete.
    load_job.result()  
    destination_table = client.get_table(table_id)  # Make an API request.
    logging.info("Loaded {} rows.".format(destination_table.num_rows))


if __name__ == "__main__":
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument('--table_name', type=str,
            default="Random")

    parser.add_argument('--gcs_uri', type=str,
            default="schools.csv")
    args = parser.parse_args()
    GCS_SERVICE_ACCOUNT = os.getenv('GCS_SERVICE_ACCOUNT')
    GCS_BUCKET_NAME =  os.getenv('GCS_BUCKET_NAME')
    GCS_PROJECT = os.getenv('GCS_PROJECT')
    GCS_DATASET = os.getenv('GCS_DATASET')

    logging.debug(GCS_SERVICE_ACCOUNT)
    logging.debug(GCS_BUCKET_NAME)
    logging.debug(GCS_PROJECT)
    logging.debug(GCS_DATASET)

    credentials = service_account.Credentials.from_service_account_file(GCS_SERVICE_ACCOUNT)

    client = bigquery.Client(credentials=credentials)

    table_id = f"{GCS_PROJECT}.{GCS_DATASET}.{args.table_name}"

    job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("school_id", "INT64"),
                bigquery.SchemaField("code", "STRING"),
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("outcome", "STRING"),
                bigquery.SchemaField("level", "STRING"),
                bigquery.SchemaField("course_id", "INT64"),
                bigquery.SchemaField("skills", "STRING"),
                bigquery.SchemaField("embeddings", "STRING"),
                bigquery.SchemaField("preprocessed_description", "STRING"),
                bigquery.SchemaField("updated_at", "TIMESTAMP"),
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
    )

    uri = f"gs://postgres-2-bigquery/{args.gcs_uri}"
    logging.debug(uri)
    load_data_from_GCS(uri, table_id, job_config)
    logging.debug("Load data successfully")
