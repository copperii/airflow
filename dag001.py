from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime
from random import randint

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


with DAG("dag001", start_date=datetime(2021, 1, 1),
         schedule_interval="@daily", catchup=False) as dag:

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
