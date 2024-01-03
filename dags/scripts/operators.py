import pandas as pd
import psycopg2
import psycopg2.extras
import datetime as dt

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base import BaseHook


class CsvToPostgresOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        csv_path,
        dwh_con,
        dwh_table,
        log_table,
        separator=';',
        encoding='cp866',
        chunksize=50000,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.csv_path = csv_path
        self.chunksize = chunksize
        self.encoding = encoding

        dwh_con = BaseHook.get_connection(dwh_con)

        self.conn = psycopg2.connect(
            host=dwh_con.host,
            port=dwh_con.port,
            dbname='bank',
            user=dwh_con.login,
            password=dwh_con.password,
        )

        self.dwh_table = dwh_table
        self.separator = separator
        self.log_table = log_table
        self.ts = dt.datetime.now()

    def execute(self, context):
        self.log.info('ETL has started')
        print('Downloading file from ', self.csv_path)

        df = pd.read_csv(
            self.csv_path,
            sep=self.separator,
            # encoding_errors='replace',
            dtype=str,
            chunksize=self.chunksize,
            encoding=self.encoding,
        )

        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    f"""
                        TRUNCATE TABLE {self.dwh_table};
                    """
                )

                rows = 0
                insert_stmt = f"INSERT INTO {self.dwh_table} VALUES %s"
                for chank in df:
                    chank['ts'] = self.ts
                    print('Got Dataframe:', chank, chank.dtypes)
                    psycopg2.extras.execute_values(cur, insert_stmt, chank.values)
                    rows += len(chank)

                log_data = (context['dag'].dag_id, f'Recorded {rows} rows', self.ts)
                values = ','.join(['%s']*len(log_data))

                insert_stmt = f"""INSERT INTO {self.log_table} (dag_id, info, ts) VALUES ({values})"""
                cur.execute(insert_stmt, log_data)

        self.log.info('ETL has finished')


class Logger(BaseOperator):

    @apply_defaults
    def __init__(self, dwh_con, log_table, info, *args, **kwargs):
        super().__init__(*args, **kwargs)

        dwh_con = BaseHook.get_connection(dwh_con)

        self.conn = psycopg2.connect(
            host=dwh_con.host,
            port=dwh_con.port,
            dbname='bank',
            user=dwh_con.login,
            password=dwh_con.password,
        )

        self.log_table = log_table
        self.info = info
        self.ts = dt.datetime.now()
        self.query = """INSERT INTO {log_table} (dag_id, info, ts) VALUES ({values})"""

    def execute(self, context):
        self.log.info('The log recording has started')

        data = (context['dag'].dag_id, self.info, self.ts)
        values = ','.join(['%s']*len(data))

        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(
                    self.query.format(
                        log_table=self.log_table,
                        values=values,
                    ),
                    data,
                )
        self.log.info('The log recording has ended')
