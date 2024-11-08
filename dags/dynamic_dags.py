from airflow.decorators import dag, task
from datetime import datetime

# Define a reusable DAG template
def create_dag(dag_id, schedule, start_date):
    @dag(dag_id=dag_id, schedule_interval=schedule, start_date=start_date, catchup=False)
    def template_dag():
        
        @task
        def start_task():
            print("Starting...")

        @task
        def end_task():
            print("Ending...")

        # Task dependencies
        start_task() >> end_task()

    return template_dag()

# Generate multiple DAGs using the template
for i in range(1, 4):
    dag_id = f"example_dag_{i}"
    schedule = "@daily"
    start_date = datetime(2023, 1, 1)
    globals()[dag_id] = create_dag(dag_id, schedule, start_date)
