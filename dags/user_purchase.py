# import libraries
import airflow
from airflow import models
from datetime import timedelta, datetime
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

# config
BUCKET_PATH = models.Variable.get("bucket_path")
PROJECT_ID = models.Variable.get("project_id")
BUCKET_NAME = BUCKET_PATH[BUCKET_PATH.rindex('/')+1:]

# define default argument
default_args = {
    "start_date": datetime(2010, 12, 1),
    "depends_on_past": True,
    "dataflow_default_options": {
        "project": PROJECT_ID,
        "temp_location": BUCKET_PATH + "/tmp/",
        "numWorkers": 1,
    },
}

# instantiate the dag
with models.DAG(
    'user_behaviour_orchestration',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    # Task 1
    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=BUCKET_NAME,
        source_objects=["user_purchase/{{ds}}/temp_filtered_user_purchase.csv"],
        destination_project_dataset_table=PROJECT_ID+".mydataset.user_purchase${{ds_nodash}}",
        # write the data to mydataset.user_purchase where _partitioneddate = {{ds}}
        source_format="csv",
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'InvoiceNo', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'StockCode', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'detail', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'Quantity', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'InvoiceDate', 'type': 'DATE', 'mode': 'REQUIRED'},
            {'name': 'UnitPrice', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
            {'name': 'customerid', 'type': 'INT64', 'mode': 'REQUIRED'},
            {'name': 'Country', 'type': 'STRING', 'mode': 'REQUIRED'},
        ],
        # time_partitioning= {'type':'day','field':'InvoiceDate'},
        write_disposition="WRITE_TRUNCATE",
        create_disposition='CREATE_IF_NEEDED',
        wait_for_downstream=True,
        depends_on_past=True
    )
    # Task 2
    pivoting_for_country_level = BigQueryOperator(
        task_id='pivoting_for_country_level',
        sql="""
        SELECT
            InvoiceDate,  
            Country,
            COUNT(*) AS Quantity,
            SUM(UnitPrice) As GMV
        FROM 
            `"""+PROJECT_ID+""".mydataset.user_purchase`
        WHERE _PARTITIONDATE = '{{ds}}'
        GROUP BY 1,2
        """,
        write_disposition='WRITE_APPEND',
        destination_dataset_table=f"{PROJECT_ID}.mydataset.country_sales",
        use_legacy_sql=False,
        dag=dag
    )

    # Task 3 (dummy task)
    end_of_data_pipeline = DummyOperator(task_id='end_of_data_pipeline')

    # define the dependecies
    gcs_to_bq >> pivoting_for_country_level >> end_of_data_pipeline