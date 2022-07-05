from datetime import datetime
from logging import getLogger

from airflow import DAG
from hexa_arguments import flag, multi, path, single
from hexa_factory import get_dag_definitions_by_template
from hexa_operators import HexaKubernetesPodOperator, create_report_operators
from kubernetes.client import models as k8s

logger = getLogger(__name__)


# Config
# At a later stage, this config should come from an API call on OpenHexa
# DHIS2_CONFIG should look like this:
# {
#   "instances": [
#     {
#       "code": "play",
#       "url": "https://play.dhis2.org/2.36.0",
#       "username": "admin",
#       "password": "district",
#     }
#   ]
# }
documentation = """
### DHIS2 Extraction Pipeline

The `dhis2-extraction` pipeline downloads data from a DHIS2 instance via the API and transforms it into formatted CSV files.

The program supports S3, GCS, and local paths for outputs.

The pipeline is composed of two distinct tasks:

- The `download` task uses the DHIS2 API to query the requested data and stores it in raw form in Cloud Storage
- The `transform` task processes the raw data depending on the pipeline parameters

The pipeline code itself can be found in the [hexa-pipelines](https://github.com/BLSQ/openhexa-pipelines) directory.

#### Usage

The extracted data, along with the raw data and the metadata will be extracted in the directory specified by
`output_dir`. This parameter should point to a cloud storage location such as S3.

Three API request modes are supported :

- `analytics` (default)
- `raw`

The `analytics` mode allows to access analytical, aggregated data in DHIS2. The analytics resource is
powerful as it lets you query and retrieve data aggregated along all available data dimensions. For instance,
you can ask the analytics resource to provide the aggregated data values for a set of data elements, periods
and organisation units (`api/analytics` endpoint in the DHIS2 Web API).

The `raw` mode allows to access raw data values -- without going through the analytics tables. It is
useful when accessing data which are not yet in the analytics tables, or to access data values which are
not available in the analytics tables, such as the `lastUpdated` value (`api/dataValueSet` endpoint in the
DHIS2 Web API).

At least one temporal dimension is required: either `periods` or `start_date` and `end_date`.

At least one data dimension is required (unless you are in `metadata_only` mode, see below):

- `data_element`
- `data_element_groups`
- `indicators`
- `indicator_groups`
- `dataset`

At least one org unit dimension is required:

- `organisation_units`
- `organisation_unit_groups`
- `organisation_unit_levels`

A minimal working config for the [official DHIS2 demo instance](https://play.dhis2.org/) would be:

```json
{
  "periods": [
    "2020",
    "2021"
  ],
  "output_dir": "s3://bucket-name/target-directory/",
  "data_elements": [
    "fClA2Erf6IO",
    "vI2csg55S9C"
  ],
  "organisation_units": [
    "ZpE2POxvl9P",
    "LFpl1falVZi"
  ]
}
```

Alternatively, if you only need the metadata:

```json
{
  "periods": [
    "2020",
    "2021"
  ],
  "output_dir": "s3://bucket-name/target-directory/",
  "metadata_only": true
}
```

#### Parameter reference

- `output_dir`: a valid S3 path
- `start`: an ISO8601 date (to be used with end)
- `end`: an ISO8601 date (to be used with start)
- `periods`: a list of periods using the DHIS2 period format
- `organisation_units`: a list of DHIS2 organisation unit IDs
- `organisation_unit_groups`: a list of DHIS2 organisation unit group IDs
- `organisation_unit_levels`: a list of DHIS2 organisation unit level IDs
- `datasets`: a list of DHIS2 dataset IDs
- `data_elements`: a list of DHIS2 data element IDs
- `data_element_groups`: a list of DHIS2 data element group IDs
- `indicators`: a list of DHIS2 indicator IDs
- `indicator_groups`: a list of DHIS2 indicator group IDs
- `attribute_option_combos`: a list of DHIS2 attribute option combo IDs
- `category_option_combos`: a list of DHIS2 category option combo IDs
- `programs`: a list of DHIS2 program IDs
- `programs`: a list of DHIS2 program IDs
- `children`: include children of selected org units (false by default)
- `mode`: DHIS2 API endpoint (analytics (default), or raw)
- `empty_rows`: add empty rows for missing data (false by default)
- `metadata_only`: only download metadata (false by default)
- `overwrite`: overwrite existing files (false by default)

TODO:

- Clarify how `start` / `end` and `periods` interact

#### Output

The output is organized as follows:

* `extract.csv`
* `raw_data/`
* `metadata/`

`extract.csv` is a table with one row per data element, org. unit and period. The following columns are created:

* `dx_uid`, `dx_name`: data element or indicator UID and name
* `dx_type`: data element or indicator
* `coc_uid`, `coc_name`: category option combo UID and name
* `period`: period in DHIS2 format
* `ou_uid`: org. unit UID
* `level_{1,2,3,4,5}_uid`, `level_{1,2,3,4,5}_name`: org. unit UID and name for each hierarchical level
"""


def build_dhis2_dag(definition):
    dag = DAG(
        definition["dag_id"],
        description=documentation,
        schedule_interval=definition["schedule"],
        start_date=datetime(2021, 8, 21),
        catchup=False,
    )

    download = HexaKubernetesPodOperator(
        dag=dag,
        image="blsq/openhexa-dhis2-extraction",
        arguments=[
            "dhis2extraction",
            "download",
            path("-o", "output_dir", suffix="/raw_data"),
            single("--start", "start"),
            single("--end", "end"),
            multi("--period", "periods"),
            multi("-ou", "organisation_units"),
            multi("-oug", "organisation_unit_groups"),
            multi("-lvl", "organisation_unit_levels"),
            multi("-ds", "datasets"),
            multi("-de", "data_elements"),
            multi("-deg", "data_element_groups"),
            multi("-in", "indicators"),
            multi("-ing", "indicator_groups"),
            multi("-aoc", "attribute_option_combos"),
            multi("-coc", "category_option_combos"),
            multi("-prg", "programs"),
            flag(("--children", "--no-children"), "children", default=False),
            single("--mode", "mode"),
            flag("--metadata-only", "metadata_only", default=False),
            flag("--overwrite", "overwrite", default=False),
        ],
        name="pipelines-dhis2-download",
        task_id="dhis2-download",
        credentials_url=definition["credentials_url"],
        token=definition["token"],
        resources=k8s.V1ResourceRequirements(
            limits={"memory": "8000M"}, requests={"memory": "100M"}
        ),
    )

    transform = HexaKubernetesPodOperator(
        dag=dag,
        image="blsq/openhexa-dhis2-extraction",
        arguments=[
            "dhis2extraction",
            "transform",
            path("-i", "output_dir", suffix="/raw_data"),
            path("-o", "output_dir"),
            flag("--empty-rows", "empty_rows", default=False),
            flag("--overwrite", "overwrite", default=False),
        ],
        name="pipelines-dhis2-transform",
        task_id="dhis2-transform",
        credentials_url=definition["credentials_url"],
        token=definition["token"],
        resources=k8s.V1ResourceRequirements(
            limits={"memory": "8000M"}, requests={"memory": "100M"}
        ),
    )

    download >> transform

    # create_report_operators(
    #     parent_tasks=[transform],
    #     headline="DHIS2 Extraction ({{ dag.dag_id }})",
    #     info="This OpenHexa pipelines extracts data from a DHIS2 instance.",
    #     output_dir="{{ dag_run.conf['output_dir'] }}",
    #     email_address=definition["report_email"],
    #     credentials_url=definition["credentials_url"],
    #     token=definition["token"],
    # )
    return dag


for dag_definition in get_dag_definitions_by_template("DHIS2"):
    dag = build_dhis2_dag(dag_definition)
    # https://airflow.apache.org/docs/apache-airflow/stable/faq.html#how-to-create-dags-dynamically
    # DAGs should be added in globals
    if dag:
        globals()[dag.dag_id] = dag
