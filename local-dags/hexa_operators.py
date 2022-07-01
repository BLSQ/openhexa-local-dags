import os
import typing

from airflow.exceptions import AirflowSkipException
from airflow.kubernetes.secret import Secret
from airflow.models import BaseOperator, Variable
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s


class HexaKubernetesPodOperator(KubernetesPodOperator):
    """Thin layer on top of KubernetesPodOperator:

    - Pre-configures the operator with the proper namespace, pull policy, requests, etc...
    - Injects a series of env vars & secrets for Sentry and AWS credentials
    """

    def __init__(
        self,
        arguments: typing.Optional[typing.List[str]] = None,
        env_vars: typing.List[k8s.V1EnvVar] = None,
        secrets: typing.Optional[typing.List[Secret]] = None,
        resources: typing.Optional[k8s.V1ResourceRequirements] = None,
        startup_timeout_seconds: typing.Optional[int] = None,
        token: str = None,
        credentials_url: str = None,
        skip_condition: str = None,
        **kwargs,
    ):
        if arguments is not None:
            # if arguments end in .json, will be templated -> add a safety ending space to
            # stop nested templating, see: https://github.com/apache/airflow/issues/10451
            arguments = [
                arg + " " if arg.endswith(".json") else arg for arg in arguments
            ]
        secrets = [] if secrets is None else secrets
        env_vars = [] if env_vars is None else env_vars

        # Sentry
        secrets.append(
            Secret("env", "SENTRY_DSN", "hexa-airflow-pod-credentials", "SENTRY_DSN")
        )
        if "SENTRY_ENVIRONMENT" in os.environ:
            env_vars.append(
                k8s.V1EnvVar(
                    name="SENTRY_ENVIRONMENT", value=os.environ["SENTRY_ENVIRONMENT"]
                )
            )

        if token:
            env_vars.append(
                k8s.V1EnvVar(
                    name="HEXA_PIPELINE_TOKEN",
                    value=token,
                )
            )
        if credentials_url:
            env_vars.append(
                k8s.V1EnvVar(
                    name="HEXA_CREDENTIALS_URL",
                    value=credentials_url,
                )
            )

        # Schedule worker pods on highmem nodes
        # (We use the same values as the Jupyter single-user server pods for now)
        # TODO: use generic taints / tolerations / affinity values
        tolerations = [
            k8s.V1Toleration(
                key="hub.jupyter.org_dedicated",
                operator="Equal",
                value="user",
                effect="NoSchedule",
            ),
        ]
        affinity = k8s.V1Affinity(
            node_affinity=k8s.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
                    node_selector_terms=[
                        k8s.V1NodeSelectorTerm(
                            match_expressions=[
                                k8s.V1NodeSelectorRequirement(
                                    key="hub.jupyter.org/node-purpose",
                                    operator="In",
                                    values=["user"],
                                )
                            ],
                        ),
                    ],
                ),
            ),
        )

        # Resources
        if resources is None:
            resources = k8s.V1ResourceRequirements(
                limits={"memory": "2000M"}, requests={"memory": "100M"}
            )

        # pulling image can be long, default to 120s -> too much pending timeout
        if startup_timeout_seconds is None:
            startup_timeout_seconds = 600

        super().__init__(
            arguments=arguments,
            env_vars=env_vars,
            secrets=secrets,
            namespace=os.environ.get("K8S_NAMESPACE"),
            image_pull_policy="Always",
            is_delete_operator_pod=True,
            resources=resources,
            tolerations=tolerations,
            affinity=affinity,
            in_cluster=True,
            get_logs=True,
            startup_timeout_seconds=startup_timeout_seconds,
            **kwargs,
        )

        self.skip_condition = skip_condition
        if skip_condition:
            # add to the list of variable to pass to jinja for rendering
            self.template_fields = self.template_fields + ("skip_condition",)

    def execute(self, *args, **kwargs):
        if self.skip_condition is not None and self.skip_condition.strip() == "False":
            raise AirflowSkipException
        super().execute(*args, **kwargs)


class HexaReportOperator(HexaKubernetesPodOperator):
    def __init__(
        self,
        *,
        success: bool,
        headline: str,
        info: str,
        output_dir: str,
        email_address: typing.Optional[str],
        token: str = None,
        credentials_url: str = None,
    ):
        env_vars = [
            k8s.V1EnvVar(name="EMAIL_HOST", value=Variable.get("EMAIL_HOST", "")),
            k8s.V1EnvVar(name="EMAIL_PORT", value=Variable.get("EMAIL_PORT", "")),
            k8s.V1EnvVar(
                name="EMAIL_HOST_USER", value=Variable.get("EMAIL_HOST_USER", "")
            ),
            k8s.V1EnvVar(
                name="EMAIL_HOST_PASSWORD",
                value=Variable.get("EMAIL_HOST_PASSWORD", ""),
            ),
            k8s.V1EnvVar(
                name="DEFAULT_FROM_EMAIL", value=Variable.get("DEFAULT_FROM_EMAIL", "")
            ),
        ]

        arguments = [
            "report",
            "success" if success else "failure",
            "-o",
            output_dir,
            "-t",
            "Airflow",
            "-r",
            "{{ dag_run.run_id }}",
            "-d",
            "{{ dag_run.start_date.isoformat() }}",
            "--logical_date={{ dag_run.execution_date.isoformat() }}",
            "-h",
            headline,
            "-i",
            info,
            "-c",
            "{{ dag_run.conf|tojson }}",
            "-e",
            "{{ dag_run.conf.get('_report_email', '%s') }}"
            % (email_address if email_address is not None else "no-reply@openhexa.org"),
        ]

        super().__init__(
            image="blsq/openhexa-pipelines-report",
            name="pipelines-report",
            task_id="success" if success else "failure",
            trigger_rule="all_success" if success else "one_failed",
            credentials_url=credentials_url,
            arguments=arguments,
            env_vars=env_vars,
            token=token,
        )


def create_report_operators(
    parent_tasks: typing.Sequence[BaseOperator],
    headline: str,
    info: str,
    output_dir: str,
    email_address: typing.Optional[str] = None,
    token: str = None,
    credentials_url: str = None,
):
    success = HexaReportOperator(
        success=True,
        headline=headline,
        info=info,
        output_dir=output_dir,
        email_address=email_address,
        credentials_url=credentials_url,
        token=token,
    )
    failure = HexaReportOperator(
        success=False,
        headline=headline,
        info=info,
        output_dir=output_dir,
        email_address=email_address,
        credentials_url=credentials_url,
        token=token,
    )

    for task in parent_tasks:
        task >> success
        task >> failure
