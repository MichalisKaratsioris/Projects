# Method A: Using only BashOperator and run the command I would normally run in the terminal. (Best)
# Method B: Using dbt-operators for a locally located project.
# Method C: Using dbt-operators for a cloud located project.

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow_dbt.operators.dbt_operator import (
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator,
    DbtDocsGenerateOperator
)

default_args = {
  'dir': 'C:\Users\mkara\Desktop\GitHub\dbt_google_develop_branch\dbt_mk_local'
}

with DAG(dag_id='dbt', default_args=default_args, schedule_interval='@weekly') as dag:

    begin_dag = EmptyOperator(
        task_id="begin"
    )

    end_dag = EmptyOperator(
        task_id="end"
    )

    task_dbt_snapshot = DbtSnapshotOperator(
        task_id='dbt_snapshot'
    )

    task_dbt_run = DbtRunOperator(
        task_id='dbt_run'
    )

    task_dbt_test = DbtTestOperator(
        task_id='dbt_test',
        retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
    )

    task_dbt_docs = DbtDocsGenerateOperator(
        task_id='dbt_docs_generate'
    )

    begin_dag >> task_dbt_snapshot >> task_dbt_run >> task_dbt_test >> task_dbt_docs >> end_dag



# NOTES:
# There are five operators currently implemented:
# - DbtDocsGenerateOperator: Calls dbt docs generate
# - DbtDepsOperator: Calls dbt deps
# - DbtSeedOperator: Calls dbt seed
# - DbtSnapshotOperator: Calls dbt snapshot
# - DbtRunOperator: Calls dbt run
# - DbtTestOperator: Calls dbt test


# Each of the above operators accept the following arguments:
# - profiles_dir:   If set, passed as the --profiles-dir argument to the dbt command
# - target:         If set, passed as the --target argument to the dbt command
# - dir:            The directory to run the dbt command in
# - full_refresh:   If set to True, passes --full-refresh
# - vars:           If set, passed as the --vars argument to the dbt command. Should
#                   be set as a Python dictionary, as will be passed to the dbt command as YAML
# - models:         If set, passed as the --models argument to the dbt command
# - exclude:        If set, passed as the --exclude argument to the dbt command
# - select:         If set, passed as the --select argument to the dbt command
# - dbt_bin:        The dbt CLI. Defaults to dbt, so assumes it's on your PATH
# - verbose:        The operator will log verbosely to the Airflow logs
# - warn_error:     If set to True, passes --warn-error argument to dbt command and will
#                   treat warnings as errors


# Typically you will want to use the DbtRunOperator, followed by the DbtTestOperator, as shown
# earlier. You can also use the hook directly. Typically, this can be used for when you need to
# combine the dbt command with another task in the same operators, for example running dbt docs
# and uploading the docs to somewhere they can be served from.
