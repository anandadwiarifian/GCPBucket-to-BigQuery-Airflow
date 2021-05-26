# GCPBucket-to-BigQuery-Airflow
Using Airflow in Google Composer to orchestrate loading transaction data from GCP Bucket to a partitioned table in BigQuery, then create a new table to summarize the data in country level 
![image](https://user-images.githubusercontent.com/47022822/119700489-79e25e80-be7d-11eb-94eb-3e83cc0964a7.png)

### GCP Setup
1. Create a service account in `IAM & Admin`
2. Create a Google Cloud Environment in `Composer`
3. Create a bucket in `Cloud Storage`
4. Define the variables in `Environment Variables`. The variables are:
  * project_id
  * bucket_path
  * gce_region
  * gce_zone
5. Define the same variables in `Admin, Variables` inside `Airflow Web UI`

## Bigquery tables setup
Run the [sql script](/setup/create_tables.sql) in the bigquery to create the tables. 

### DAGs and data
1. Upload the content in `data` folder to `gs://BUCKET_NAME/user_purchase`
2. Upload the DAG `user_behaviour.py` file to DAGs folder in the created environment

## How to use
In the `Airflow Web UI`, click `On` for user_behaviour DAG

## DAG Explaination
You can see the script for the DAG with comments [here](/dags/user_purchase.py).
Overall, the flow is like this
- gcs_to_bq:

The transactional data is stored to multiple folders based on the invoice date. This task load the invoice date's transaction data to `user_purchase` table in bigquery. 

The table will consist of all transaction data, historic to uptodate, and is partitioned by the ingestion time, which in this case equals to invoice date.

- pivoting_for_country_level:

Summarize the transaction data for that day in country level then load it to a new table `country_sales`.

- end_of_data_pipeline:

The final task that doesn't do anything (dummy task)

The screenshot of GCS:
![bucket](https://user-images.githubusercontent.com/47022822/119700424-646d3480-be7d-11eb-9930-61912cb35fe9.PNG)

The screenshot of the airflow GUI:
![dag_tree_view](https://user-images.githubusercontent.com/47022822/119700444-69ca7f00-be7d-11eb-8dba-a7158a36e039.PNG)

The screenshot of the BigQuery:
![user_purchase_1](https://user-images.githubusercontent.com/47022822/119700459-6fc06000-be7d-11eb-9321-203993cbb909.PNG)
![country_sales_1](https://user-images.githubusercontent.com/47022822/119700452-6c2cd900-be7d-11eb-8f3f-d0a8daf28d44.PNG)


