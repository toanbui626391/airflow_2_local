from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from helpers.helper_funcs import *

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definition with multiple parameters
@dag(
    dag_id = 'example_dag_with_params_v5',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 1),
    catchup=True
)
def example_dag_with_params():
    
    @task
    def get_data_range_fact_table(data_range, **kwargs):
        # Accessing the execution_date (automatically passed from DAG)
        return helper_get_data_range_fact_table(data_range, **kwargs)  

    @task
    def process_data(data):
        # Processing data based on extracted data
        return helper_process_data(data)
    
    # Setting task dependencies and passing parameters
    data_range = 7
    extracted_data = get_data_range_fact_table(data_range)
    process_data.expand(data=extracted_data)

# Instantiate the DAG
dag = example_dag_with_params()
