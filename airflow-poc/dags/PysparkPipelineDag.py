from __future__ import annotations

# [START tutorial]
# [START import_module]
import json
from textwrap import dedent

import pendulum

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

# [END import_module]

def start(**kwargs):
    ti = kwargs["ti"]
    print("starting the pipeline.")
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    ti.xcom_push("order_data", data_string)
    ti.xcom_push("partner", "sonata")


# [END extract_function]

# [START transform_function]
def select_partner(**kwargs):
    ti = kwargs["ti"]
    extract_data_string = ti.xcom_pull(task_ids="start_task", key="order_data")
    order_data = json.loads(extract_data_string)

    total_order_value = 0
    for value in order_data.values():
        total_order_value += value

    total_value = {"total_order_value": total_order_value}
    total_value_json_string = json.dumps(total_value)
    ti.xcom_push("total_order_value", total_value_json_string)


# [END transform_function]

# [START load_function]
def load_job_params(**kwargs):
    ti = kwargs["ti"]
    total_value_string = ti.xcom_pull(task_ids="select_partner_task", key="total_order_value")
    total_order_value = json.loads(total_value_string)

    print(total_order_value)


# [START instantiate_dag]
with DAG(
    "pyspark-pipeline",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 2},
    # [END default_args]
    description="pyspark-pipeline",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["pyspark-pipeline"],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [END load_function]

    # [START main_flow]
    start_task = PythonOperator(
        task_id="start_task",
        params={"x": 10},
        python_callable=start,
    )
    start_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

    select_partner_task = PythonOperator(
        task_id="select_partner_task",
        python_callable=select_partner,
    )
    select_partner_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )

    load_job_params_task = PythonOperator(
        task_id="load_job_params_task",
        python_callable=load_job_params,
    )
    load_job_params_task.doc_md = dedent(
        """\
    #### load_job_params_task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )


    start_task >> select_partner_task >> load_job_params_task

# [END main_flow]

