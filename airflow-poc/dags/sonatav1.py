from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from constants.dag_default_constants import DEFAULT_DAGRUN_TIMEOUT
from constants.string_constants import START_DATE, CATCHUP, HIGH_RESOURCE, SLA
from utils.dag_definition_utils import get_default_args
from constants.airflow_constants import AIRFLOW_PATH_VARIABLE_NAME
from sonata_kiscore.s3_copy import saving_files

default_args = get_default_args()
default_args.pop(START_DATE)
default_args.pop(CATCHUP)


def get_parameter(**kwargs):
    ti = kwargs['ti']
    load_type = kwargs['dag_run'].conf.get('load_type')
    end_folder = kwargs['dag_run'].conf.get('end_folder')

    if load_type == None:
        load_type = "increamental_load"
    if end_folder == None:
        end_folder = "increamental"
    print(load_type)
    print(end_folder)
    ti.xcom_push(key='load_type', value=load_type)
    ti.xcom_push(key='end_folder', value=end_folder)


def save(**kwargs):
    saving_files(kwargs['ti'].xcom_pull(task_ids="get_parameters", key="end_folder"), "historical_data")


dag = DAG(dag_id="sonata_ki_Score", schedule_interval="45 22 * * *", default_args=default_args,
          start_date=datetime(2022, 8, 4), catchup=False, dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT)

airflow_path_variable = Variable.get(AIRFLOW_PATH_VARIABLE_NAME)
folder = os.path.join(os.environ.get('AIRFLOW_HOME'), 'indium')

parameters = PythonOperator(task_id='get_parameters', python_callable=get_parameter, provide_context=True, dag=dag)

# sonata_kiscore_cassandra_task=BashOperator(task_id='sonata_kiscore_cassandra_task', bash_command=f'python3 {folder}/main.py {{{{ti.xcom_pull(task_ids="get_parameters",key="load_type")}}}} {{{{ti.xcom_pull(task_ids="get_parameters",key="end_folder")}}}} ', dag=dag)

sonata_kiscore_cassandra_task_1 = BashOperator(task_id='sonata_kiscore_cassandra_task_1',
                                               bash_command=f'python3 {folder}/main.py "onetime_load" {{{{ti.xcom_pull(task_ids="get_parameters",key="end_folder")}}}} ',
                                               dag=dag)

sonata_kiscore_cassandra_task_2 = BashOperator(task_id='sonata_kiscore_cassandra_task_2',
                                               bash_command=f'python3 {folder}/main.py "incremental_load" {{{{ti.xcom_pull(task_ids="get_parameters",key="end_folder")}}}} ',
                                               dag=dag)

saving_file = PythonOperator(task_id='saving_files', python_callable=save, provide_context=True, dag=dag)

sonata_postgres_push = BashOperator(task_id='sonata_kiscore_cassandra_task',
                                    bash_command=f'python3 {folder}/sonata_risk_management_postgress_pipeline.py  ',
                                    dag=dag)

parameters >> sonata_kiscore_cassandra_task_1 >> sonata_kiscore_cassandra_task_2 >> saving_file >> sonata_postgres_push