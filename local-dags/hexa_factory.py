import json
from logging import getLogger

from airflow.models import Variable

logger = getLogger(__name__)

"""
What is a dag definition?
=========================


To communicate between openhexa and airflow's dags code, we serialize the dags
info into JSON and store it in airflow variables.

The format is the following:

```
DEFINITIONS = [
    {
        "dag_id": str, name of the dag
        "static_config": opaque object, configuration for the dag builder
        "report_email": str, email to send the success/failure report to
        "schedule": maybe(str), schedule, cron-like format when to run
    }, ...
]
```

### static_config

Each pipeline has two configs:
 - a static configuration, declared once, available for the builder
 - a runtime configuration, added by a human when running the pipeline manualy,
   unavailable at build time. Must be templated by {{ dag_run.conf... }}

Each automated DAG will have a runtime configuration empty. Since all DAG
should be able to run "by itself", all DAG should use static_config. But there
is great value to enable end user to trigger DAG manually with specific
parameters. So DAG builder's designer must use some complicated substitution
pattern. For example, let's say CHIRPS pipeline define a output_dir for all
automated run but an enduser should be able to run the same pipeline, for
the same country, with a different output_dir:

```
arguments = [
    ...
    "--output_dir",
    "{{ dag_run.conf.get('output_dir', '%s') }}" % definition["static_config"].get("output_dir"),
    ...
]
```

This enable the output_dir to be set by runtime config, then by static_config
if absent of runtime config.

The format of static_config is owned by the specific DAG template, neither
openhexa nor airflow assume any format, except that it must serialize to json.
"""


def get_dag_definitions_by_template(code: str):
    try:
        dag_definitions = json.loads(Variable.get(f"TEMPLATE_{code}_DAGS"))
    except Exception:
        logger.exception("airflow variables")
    else:
        for dag_definition in dag_definitions:
            yield dag_definition
