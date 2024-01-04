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
    "DDS.ACCOUNTS_MODEL",
    default_args=default_args,
    description="Обновление слоя DDS.",
    start_date=dt.datetime(2023, 12, 29),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    tags=['DDS', 'Задание_1'],
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

    MD_CURRENCY_D_upsert = PostgresOperator(
        task_id='MD_CURRENCY_D_upsert',
        postgres_conn_id='dwh',
        sql="scripts/dds/DDS.MD_CURRENCY_D.sql",
    )

    MD_EXCHANGE_RATE_D_upsert = PostgresOperator(
        task_id='MD_EXCHANGE_RATE_D_upsert',
        postgres_conn_id='dwh',
        sql="scripts/dds/DDS.MD_EXCHANGE_RATE_D.sql",
    )

    MD_ACCOUNT_D_upsert = PostgresOperator(
        task_id='MD_ACCOUNT_D_upsert',
        postgres_conn_id='dwh',
        sql="scripts/dds/DDS.MD_ACCOUNT_D.sql",
    )

    FT_POSTING_F_upsert = PostgresOperator(
        task_id='FT_POSTING_F_upsert',
        postgres_conn_id='dwh',
        sql="scripts/dds/DDS.FT_POSTING_F.sql",
    )

    FT_BALANCE_F_upsert = PostgresOperator(
        task_id='FT_BALANCE_F_upsert',
        postgres_conn_id='dwh',
        sql="scripts/dds/DDS.FT_BALANCE_F.sql",
    )

    MD_LEDGER_ACCOUNT_S_upsert = PostgresOperator(
        task_id='MD_LEDGER_ACCOUNT_S_upsert',
        postgres_conn_id='dwh',
        sql="scripts/dds/DDS.MD_LEDGER_ACCOUNT_S.sql",
    )

    end = Logger(
        task_id='Завершение',
        dwh_con='dwh',
        log_table='LOGS.DAG_LOGS',
        info='end',
    )

    await_dags >> start >> [MD_CURRENCY_D_upsert, MD_LEDGER_ACCOUNT_S_upsert]
    MD_CURRENCY_D_upsert >> [MD_EXCHANGE_RATE_D_upsert, MD_ACCOUNT_D_upsert]
    MD_LEDGER_ACCOUNT_S_upsert >> MD_ACCOUNT_D_upsert
    MD_ACCOUNT_D_upsert >> [FT_POSTING_F_upsert, FT_BALANCE_F_upsert] >> end
