import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from sparkify_etl_dag import default_args


with DAG('drop_tables_dag', 
         start_date=datetime.datetime.now(),
         default_args=default_args,
         max_active_runs=1) as dag:
    
    PostgresOperator(
        task_id="drop_tables",
        dag=dag,
        postgres_conn_id="redshift",
        sql='drop_tables.sql'
    )