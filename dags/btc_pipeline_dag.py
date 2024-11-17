from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.btc_functions import fetch_binance_data, dump_to_questdb

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "btc_usdt_pipeline",
    default_args=default_args,
    description="Fetch Binance BTC/USDT data and load into QuestDB",
    schedule_interval="0 * * * *",  # Every hour
    start_date=datetime(2023, 11, 1),
    catchup=False,
) as dag:

    # Task to fetch Binance data
    def fetch_data():
        # Parameters for fetching Binance data
        api_key = "your_api_key"
        secret_key = "your_secret_key"
        symbol = "BTCUSDT"
        interval = "1m"
        start_time = "2023-11-01 00:00:00"
        end_time = "2023-11-02 00:00:00"
        return fetch_binance_data(symbol, interval, start_time, end_time, api_key, secret_key)

    fetch_task = PythonOperator(
        task_id="fetch_binance_data",
        python_callable=fetch_data,
    )

    # Task to load data into QuestDB
    def load_data(**kwargs):
        # Retrieve the DataFrame from XCom
        ti = kwargs["ti"]
        df = ti.xcom_pull(task_ids="fetch_binance_data")
        db_config = {
            "dbname": "qdb",
            "user": "admin",
            "password": "quest",
            "host": "questdb",
            "port": 8812,
        }
        dump_to_questdb(df, "btc_usdt", db_config)

    load_task = PythonOperator(
        task_id="load_to_questdb",
        python_callable=load_data,
        provide_context=True,
    )

    # Define task dependencies
    fetch_task >> load_task

