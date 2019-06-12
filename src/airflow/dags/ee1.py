from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import ee

ee.Initialize()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 6, 1),
    "email": ["kfisher@wcs.org"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': datetime(2018, 6, 3),
}

dag = DAG("ee1", default_args=default_args, schedule_interval=timedelta(1))


def print_srtm(ds, **kwargs):
    image = ee.Image('srtm90_v4')
    info = image.getInfo()
    print(info)
    return info


get_srtm_info = PythonOperator(
    task_id='print_srtm',
    provide_context=True,
    python_callable=print_srtm,
    dag=dag,
)

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)

t2.set_upstream(get_srtm_info)
