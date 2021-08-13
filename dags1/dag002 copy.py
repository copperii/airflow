"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
"""
This version is designed for airflow 2.x
"""

# Other function imports
from datetime import datetime, timedelta
from random import randint
from textwrap import dedent
from airflow.utils.dates import days_ago

# The DAG object; allways needed
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# luodaan suoritettavat funktiot


def _choose_best_model(ti):
    # haetaan ti-objektista tietoa
    accuracies = ti.xcom_pull(task_ids=[
        "training_model_A",
        "training_model_B",
        "training_model_C"
    ])
    best_accuracy = max(accuracies)
    if (best_accuracy > 8):
        return 'accurate'
    return 'inaccurate'


def _training_model():
    # tämä tieto menee ti objektiin
    return randint(1, 10)

# määritellään DAG
# eka unique id, sitten aloituspäivä,
# kuinka usein ajetaan ... cron expression
# poista ei-ajetut päivämäärät catchup=False


with DAG(
    # unique id
    'dag002',
    default_args=default_args,
    description='A simple test DAG',
    # kuinka usein ajetaan ... cron expression
    # schedule_interval=timedelta(days=1),
    schedule_interval="@daily",
    start_date=days_ago(2),
    # start_date=datetime(2021, 2, 1),
    tags=['Testi versio 2'],
    # poista ei-ajetut päivämäärät catchup=False (käytännöllinen jos aloitus kauan sitten)
    catchup=False
) as dag:

    # tasks go here
    training_model_A = PythonOperator(
        # unique id inside DAG
        task_id="training_model_A",
        python_callable=_training_model
    )

    training_model_B = PythonOperator(
        # unique id inside DAG
        task_id="training_model_B",
        python_callable=_training_model
    )

    training_model_C = PythonOperator(
        # unique id inside DAG
        task_id="training_model_C",
        python_callable=_training_model
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_choose_best_model
    )

    accurate = BashOperator(
        task_id="accurate",
        bash_command="echo 'accurate'"
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

    t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )

    # using Jinja templating https://jinja.palletsprojects.com/en/2.11.x/
    templated_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )

    [training_model_A, training_model_B,
        training_model_C] >> choose_best_model >> [accurate, inaccurate]
