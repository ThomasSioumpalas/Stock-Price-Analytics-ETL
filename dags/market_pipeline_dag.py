from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore

DBT = "/home/airflow/.local/bin/dbt"
DBT_PROJECT = "/opt/airflow/market_pipeline"
DBT_FLAGS = f"--profiles-dir {DBT_PROJECT}"

default_args = {
    "owner": "ace",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="market_pipeline",
    description="Daily AAPL market data pipeline: Kafka → Databricks → dbt",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 6 * * 1-5",   # 6:00 AM Mon-Fri
    catchup=False,
    tags=["market", "kafka", "databricks", "dbt"],
) as dag:

    # ── TASK 1: PRODUCER ──────────────────────────────────────────
    def run_producer():
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "producer",
            "/opt/airflow/scripts/producer.py"
        )
        producer_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(producer_module)
        producer_module.fetch_and_produce()

    fetch_and_produce_task = PythonOperator(
        task_id="fetch_and_produce",
        python_callable=run_producer,
    )

    # ── TASK 2: CONSUMER ──────────────────────────────────────────
    def run_consumer():
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "consumer",
            "/opt/airflow/scripts/consumer.py"
        )
        consumer_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(consumer_module)

    consume_to_databricks_task = PythonOperator(
        task_id="consume_to_databricks",
        python_callable=run_consumer,
    )

    # ── TASK 3: DBT STAGING ───────────────────────────────────────
    # no env= → inherits full container environment
    # DATABRICKS_HOST, TOKEN, HTTP_PATH already injected by docker-compose
    dbt_staging_task = BashOperator(
        task_id="dbt_staging",
        bash_command=f"cd {DBT_PROJECT} && {DBT} run --select stg_market_prices {DBT_FLAGS}",
    )

    # ── TASK 4: DBT INTERMEDIATE ──────────────────────────────────
    dbt_intermediate_task = BashOperator(
        task_id="dbt_intermediate",
        bash_command=f"cd {DBT_PROJECT} && {DBT} run --select int_market_prices {DBT_FLAGS}",
    )

    # ── TASK 5: DBT MARTS ─────────────────────────────────────────
    dbt_marts_task = BashOperator(
        task_id="dbt_marts",
        bash_command=f"cd {DBT_PROJECT} && {DBT} run --select mart_price_history mart_alerts {DBT_FLAGS}",
    )

    # ── TASK 6: DBT TEST ──────────────────────────────────────────
    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT} && {DBT} test {DBT_FLAGS}",
    )

    # ── TASK ORDER ────────────────────────────────────────────────
    (
        fetch_and_produce_task
        >> consume_to_databricks_task
        >> dbt_staging_task
        >> dbt_intermediate_task
        >> dbt_marts_task
        >> dbt_test_task
    )