import datetime as dt
import yaml

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from scripts.operators import Logger


default_args = {
    'owner': 'Швейников Андрей',
    'retries': 4,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
    "ODS.ACCOUNTS_MODEL",
    default_args=default_args,
    description="Обновление слоя ODS.",
    start_date=dt.datetime(2023, 12, 29),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['ODS', 'Задание_1'],
) as dag:

    with open('/opt/airflow/dags/to_raw_dag_config.yaml', 'r') as file:
        dags = yaml.safe_load(file)

    await_dags = []

    for _, config in dags.items():
        await_dags.append(
            ExternalTaskSensor(
                task_id=f"{config['dag_id']}_wait",
                external_dag_id=config['dag_id'],
                external_task_id='Завершение',
            )
        )

    start = Logger(
        task_id='Старт',
        dwh_con='dwh',
        log_table='LOGS.DAG_LOGS',
        info='start',
    )

    tables = ('MD_CURRENCY_D', 'MD_EXCHANGE_RATE_D',
              'MD_ACCOUNT_D', 'FT_POSTING_F', 'FT_BALANCE_F',
              'MD_LEDGER_ACCOUNT_S')
    tasks = []

    for table in tables:
        tasks.append(
            PostgresOperator(
                task_id=f'{table}_upsert',
                postgres_conn_id='dwh',
                sql=f"scripts/ods/ODS.{table}.sql",
            )
        )

    end = Logger(
        task_id='Завершение',
        dwh_con='dwh',
        log_table='LOGS.DAG_LOGS',
        info='end',
    )

    await_dags >> start >> tasks >> end
