from datetime import datetime

from airflow import DAG
from hexa_factory import get_dag_definitions_by_template
from hexa_operators import HexaKubernetesPodOperator, create_report_operators
from kubernetes.client import models as k8s

documentation = """
### Papermill Pipeline

The papermill pipeline runs a notebook and store the result into another notebook.

Invoke with:

- in_notebook: str) notebook source (could be s3://...)
- out_notebook: str) notebook to generate (could be s3://...)
- parameters: dict[str, Any]) parameters to inject into notebook

For example:
```
{
    "in_notebook": "s3://hexa-bucket/project/notebook.ipynb",
    "out_notebook": "s3://hexa-bucket/project/output/2020-01-01_notebook_output.ipynb",
    "parameters": {
        "quarter": "2020-01",
        "country": "be"
    }
}
```
"""


def build_papermill_dag(definition):
    dag = DAG(
        definition["dag_id"],
        description=documentation,
        schedule_interval=definition["schedule"],
        start_date=datetime(2021, 8, 21),
        catchup=False,
    )

    if "output_dir" in definition["static_config"]:
        env_vars = [
            k8s.V1EnvVar(
                name="OUTPUT_DIR",
                value=definition["static_config"]["output_dir"],
            )
        ]
    else:
        env_vars = []

    papermill = HexaKubernetesPodOperator(
        image="blsq/openhexa-papermill",
        arguments=[
            "-i",
            "{{ dag_run.conf.get('in_notebook', '%s') }}"
            % definition["static_config"].get("in_notebook"),
            "-o",
            "{{ dag_run.conf.get('out_notebook', '%s') }}"
            % definition["static_config"].get("out_notebook"),
        ],
        name="papermill",
        task_id="task-papermill",
        credentials_url=definition["credentials_url"],
        token=definition["token"],
        dag=dag,
    )

    if "output_dir" in definition["static_config"]:
        create_report_operators(
            parent_tasks=[papermill],
            headline="Papermill ({{ dag.dag_id }})",
            info="This OpenHexa pipeline run a notebook with Papermill",
            output_dir=definition["static_config"]["output_dir"],
            email_address=definition["report_email"],
            credentials_url=definition["credentials_url"],
            token=definition["token"],
        )

    return dag


for dag_definition in get_dag_definitions_by_template("PAPERMILL"):
    dag = build_papermill_dag(dag_definition)
    # https://airflow.apache.org/docs/apache-airflow/stable/faq.html#how-to-create-dags-dynamically
    # DAGs should be added in globals
    globals()[dag.dag_id] = dag
