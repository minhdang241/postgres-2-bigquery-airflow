import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Union

import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

#TODO: Split file to multiple smaller files

dag_config = Variable.get("postgre-2-bigquery-config", deserialize_json=True)

dag = DAG(
        dag_id="postgres_2_bigquery",
        start_date=datetime(2021,9,25),
        schedule_interval="@daily"
)

# dump db component
def _dump_table(db_info: Dict[str, str], *, table_name: str, file_path: str):
    """Dump data from a particular table of PostgreSQL to CSV file
    Parameters
    ----------
    db_info: info includes DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
    table_name: name of the dumped table
    file_path: path to the saved file
    """
    import logging
    import psycopg2

    logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s")

    conn = None
    try:
        conn = psycopg2.connect(
                    database=db_info['DB_NAME'],
                    user=db_info['DB_USER'],
                    password=db_info['DB_PASSWORD'],
                    host=db_info['DB_HOST'],
                    port=db_info['DB_PORT']
                )
        cur = conn.cursor()

        with open(file_path, "w") as f:
            sql = f'COPY "{table_name}" TO STDOUT WITH CSV HEADER'
            cur.copy_expert(sql, f)
        cur.close()
    except Exception as err:
        logging.error(err)
        logging.debug("Failed to interact with DB")
    finally:
        if conn is not None:
            conn.close()
            logging.info('DB connection closed')

db_info = dict()
db_info['DB_USER'] = dag_config.get("DB_USER")
db_info['DB_PASSWORD'] = dag_config.get("DB_PASSWORD")
db_info['DB_HOST'] = dag_config.get("DB_HOST")
db_info['DB_PORT'] = dag_config.get("DB_PORT")
db_info['DB_NAME'] = dag_config.get("DB_NAME")

dump_db = PythonOperator(
        task_id="dump_db",
        python_callable=_dump_table,
    	op_args=[db_info],
    	op_kwargs={'table_name': 'Courses', 'file_path': '/Users/minhdang/Desktop/De/project/dags/artifacts/courses.csv'},
        dag=dag,
)


# upload file component
def _upload_file(file_path: Union[str, Path], file_name: str):
    """Upload a file to google cloud storage

    Parameters
    ----------
    file_name: name of file when uploading to google cloud storage
    file_path: local path of the file
    bucket: bucket object

    Returns
    -------
    bool
        True if upload file successfully, otherwise False
    """
    import logging
    from google.cloud import storage
    from google.oauth2 import service_account
    
    logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s")
    
    GCS_SERVICE_ACCOUNT = dag_config.get('GCS_SERVICE_ACCOUNT')
    GCS_BUCKET_NAME = dag_config.get('GCS_BUCKET_NAME')
    credentials = service_account.Credentials.from_service_account_file(GCS_SERVICE_ACCOUNT)
    client = storage.Client(credentials=credentials)
    bucket = client.get_bucket(GCS_BUCKET_NAME)

    ok = True
    try:
        with open(f"{file_path}", "rb") as f:
            blob = bucket.blob(file_name)
            blob.upload_from_file(f)
    except Exception as err:
        logging.error(err)
        ok = False
    return ok

upload_file = PythonOperator(
        task_id="upload_file",
        python_callable=_upload_file,
        op_args=['/Users/minhdang/Desktop/De/project/dags/artifacts/courses.csv', 'courses.csv'],
        dag=dag
    )


# Load data component
def _load_data(uri: str, table_name: str, gcs_info):
    """load data to BigQuery from GCS

    Parameters
    ----------
    uri: path to file stored in GCS
    table_name: name used to create table in BigQuery
    gcs_info: GCS required info
    """

    import logging
    from google.cloud import bigquery
    from google.oauth2 import service_account
    
    logging.basicConfig(level=logging.DEBUG,
                            format="%(asctime)s %(levelname)s %(name)s %(threadName)s : %(message)s")
    
    credentials = service_account.Credentials.from_service_account_file(gcs_info['GCS_SERVICE_ACCOUNT'])

    client = bigquery.Client(credentials=credentials)

    table_id = f"{gcs_info['GCS_PROJECT']}.{gcs_info['GCS_DATASET']}.{table_name}"
    
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

    # Make an API request.
    load_job = client.load_table_from_uri(
                uri, table_id, job_config=job_config
    )

    # Waits for the job to complete.
    load_job.result()
    destination_table = client.get_table(table_id)  # Make an API request.
    logging.info("Loaded {} rows.".format(destination_table.num_rows))

gcs_info = dict()
gcs_info['GCS_SERVICE_ACCOUNT'] = dag_config.get('GCS_SERVICE_ACCOUNT')
gcs_info['GCS_BUCKET_NAME']=  dag_config.get('GCS_BUCKET_NAME')
gcs_info['GCS_PROJECT'] = dag_config.get('GCS_PROJECT')
gcs_info['GCS_DATASET'] = dag_config.get('GCS_DATASET')

#TODO: remove hardcode here
uri = f"gs://{gcs_info['GCS_BUCKET_NAME']}/courses.csv"

load_data = PythonOperator(
        task_id="load_data",
        python_callable=_load_data,
        op_args=[uri, "NewCourses", gcs_info],
        dag=dag
    )


dump_db >> upload_file >> load_data 
