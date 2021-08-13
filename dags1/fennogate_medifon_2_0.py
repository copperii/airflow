"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
"""
Update version to test/fix migration to airflow 2.x
"""
#import cx_Oracle
#from airflow.hooks import OracleHook




from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pandas as pd
import os
import ast
from sqlalchemy import create_engine
from cx_Oracle import makedsn
from airflow.providers.oracle.operators.oracle import OracleOperator
class OscarOracleHook(OracleHook):
    def get_uri(self):
        print("OscarOracleHook get_uri")
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        login = ''
        host = conn.host[:conn.host.find("/")]
        service_name = conn.host[conn.host.find("/")+1:]
        if conn.login:
            login = '{conn.login}:{conn.password}@'.format(conn=conn)
        #service_name = ast.literal_eval(conn.get_extra())["service_name"]
        dsn = makedsn(host, conn.port, service_name=service_name)
        return '{conn.conn_type}://{login}{dsn}'.format(conn=conn, login=login, dsn=dsn)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(2019, 10, 2, 4, 30),
    "start_date": datetime(2021, 11, 11, 4, 30),
    # "email": ["airflow@airflow.com"],
    # "email_on_failure": False,
    # "email_on_retry": False,
    # "retries": 1,
    "retry_delay": timedelta(days=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


def start(ds, **kwargs):
    return "Alku"


def read_csvs_to_db(ds, **kwargs):
    #ds= '2019_11_01'
    print("!! read_csvs_to_db !!")
    df = pd.DataFrame()
    print("df = {0}".format(df))
    print("ds = {0}".format(ds))
    path = os.path.join(os.sep, "data", "fennogate")
    print("path = {0}".format(path))
    for i in os.listdir(path):

        if i.find(ds.replace("-", "_")) > -1:
            try:
                df2 = pd.read_csv(os.path.join(path, i), names=[
                                  'astunnus', 'nimtunnus', 'eratunnus', 'toimpvm', 'toimmaara', 'myynti', 'alennus'], header=1, decimal=",")
                df2['toimpvm'] = pd.to_datetime(
                    df2['toimpvm'], format="%Y-%m-%d")
                df2['astunnus'] = df2['astunnus'].astype('category')
                df2['nimtunnus'] = df2['nimtunnus'].astype('category')
                df2['eratunnus'] = df2['eratunnus'].astype('category')
                print("df2 = {0}".format(df2))
                df = pd.concat([df, df2])
            except:  # jos ei sisaltoa
                pass

    engine = OscarOracleHook(
        oracle_conn_id="ora_fennogate2").get_sqlalchemy_engine()
    print("---------")
    print(df)
    print("---------")
    df.to_sql('OSC_MEDIFONMYYNTI', con=engine, if_exists="append", index=False)
    #df.to_sql('OSC_FENNOTESTI', con=con, if_exists="append", index=False)
    #df3 = pd.read_sql("SELECT tunnus, nimi from lkust", con = con )
    #df.to_sql('OSC_FENNOTESTI', con=engine, if_exists="append", index=False)

    # print(df3)

    return "Loppu"


dag = DAG("fennogate_medifon_2_0", default_args=default_args,
          schedule_interval=timedelta(days=1))

# t1, t2 and t3 are examples of tasks created by instantiating operators

t2 = PythonOperator(task_id="read_csvs_to_db",
                    python_callable=read_csvs_to_db, dag=dag, provide_context=True)


# DailyDeliveries_2019_11_05_010503.csv
