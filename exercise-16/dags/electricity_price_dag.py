from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'sherif',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'electricity_price_monitor',
    default_args=default_args,
    description='Electricity price monitoring workflow',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['electricity', 'monitoring'],
)

def check_price_status():
    print("Checking electricity prices...")
    print("Current price: 12.5 cents/kWh")
    return "Price check completed"

start_task = BashOperator(
    task_id='start_monitoring',
    bash_command='echo "Starting monitoring at $(date)"',
    dag=dag,
)

check_prices = PythonOperator(
    task_id='check_prices',
    python_callable=check_price_status,
    dag=dag,
)

end_task = BashOperator(
    task_id='end_monitoring',
    bash_command='echo "Monitoring completed at $(date)"',
    dag=dag,
)

start_task >> check_prices >> end_task
