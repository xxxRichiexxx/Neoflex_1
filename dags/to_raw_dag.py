import datetime as dt
import yaml

from airflow import DAG

from scripts.operators import CsvToPostgresOperator, Logger


with open('/opt/airflow/dags/to_raw_dag_config.yaml', 'r') as file:
    dags = yaml.safe_load(file)

for dag_id, config in dags.items():

    default_args = {
        'owner': 'Швейников Андрей',
        'retries': 4,
        'retry_delay': dt.timedelta(minutes=30),
    }

    with DAG(
        f"{config['dag_id']}",
        default_args=default_args,
        description=f"Загрузка данных в {config['dwh_table']}.",
        start_date=config['start_date'],
        schedule_interval=config['schedule_interval'],
        catchup=True,
        max_active_runs=1,
        tags=config['tags'],
    ) as globals()[dag_id]:

        start = Logger(
            task_id='Старт',
            dwh_con='dwh',
            log_table='LOGS.DAG_LOGS',
            info='start',
        )

        get_csv_data = CsvToPostgresOperator(
            task_id=f"load_to_{config['dwh_table']}",
            csv_path=config['csv_path'],
            chunksize=config['chunksize'],
            dwh_con='dwh',
            dwh_table=config['dwh_table'],
            log_table='LOGS.DAG_LOGS',
        )

        end = Logger(
            task_id='Завершение',
            dwh_con='dwh',
            log_table='LOGS.DAG_LOGS',
            info='end',
        )

        start >> get_csv_data >> end
