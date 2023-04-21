import argparse
import os
import requests
import json
from pathlib import Path


class DbtConn:

    def __init__(self) -> None:
        tenant = "cloud"
        self.account_id = int(os.environ.get("dbt_account_id"))
        self.project_id = int(os.environ.get("dbt_project_id"))
        self.environment_id = int(os.environ.get("dbt_environment_id"))
        self.threads = int(os.environ.get("dbt_threads") or 4)
        self.base_url = f"https://{tenant}.getdbt.com/api/v2/accounts/{self.account_id}"
        self.headers = {
            "User-Agent": "VaultSpeed",
            "Content-Type": "application/json",
            "Authorization": f"Token {os.environ.get('dbt_token')}"
        }

    def remove_job(self, job_name):
        print(f"remove job {job_name} if it exists")
        res = requests.get(url=f"{self.base_url}/jobs", headers=self.headers)
        job_ids = [job["id"] for job in res.json()["data"] if job["name"] == job_name]
        if job_ids:
            res = requests.delete(url=f"{self.base_url}/jobs/{job_ids[0]}", headers=self.headers)
            print(res.json())

    def create_job(self, job_name, command, schedule=None):
        print(f"create job {job_name}")
        payload = {
            "id": None,
            "account_id": self.account_id,
            "project_id": self.project_id,
            "name": job_name,
            "environment_id": self.environment_id,
            "execute_steps": command,
            "state": 1,  # create job with active schedule
            "generate_docs": True,
            "dbt_version": None,
            "triggers": {
                "github_webhook": False,
                "schedule": bool(schedule),
                "custom_branch_only": False,  # not optional (leaving it empty prevents editing in the UI)
                "git_provider_webhook": False  # not optional (leaving it empty prevents editing in the UI)
            },
            "schedule": schedule or {
                "date": {
                    "type": "every_day"
                },
                "time": {
                    "type": "at_exact_hours",
                    "hours": [0]
                }
            },
            "settings": {
                "threads": self.threads,
                "target_name": "VaultSpeed"
            }
        }
        res = requests.post(url=f"{self.base_url}/jobs", headers=self.headers, data=json.dumps(payload))
        print(res.json())


def generate_status_update_macro(proc_schema, sources) -> str:
    source_macros = (f"""
        {{% if var("source") == '{src_name}' %}}
            {{%- set proc_name = '{info["proc_name"]}' -%}}
            {{%- set object_based_window = {info["object_based_window"]} -%}}

            {{% if var("load_type") == 'INCR' %}}
                {{%- set dag_name = '{info["INCR"]}' -%}}
            {{% endif %}}
            {{% if var("load_type") == 'INIT' %}}
                {{%- set dag_name = '{info["INIT"]}' -%}}
            {{% endif %}}
        {{% endif %}}
    """ for src_name, info in sources.items())
    return f"""
{{% macro fmc_set_status(results) -%}}

    {{% if execute %}}

        {{%- set proc_schema = '{proc_schema}' -%}}

        {"".join(source_macros)}

        {{% set vars = {{'success_flag': 1}} %}}

        {{% for res in results %}}

            {{% if res.status == 'error' %}}
                {{# -- workaround since we cant just do variable updates inside a loop with Jinja, their values are cleared when the loop ends #}}
                {{% if vars.update({{'success_flag': 0}}) %}} {{% endif %}}
            {{% endif %}}

        {{% endfor %}}

        {{% if object_based_window %}}
            {{{{ fmc_upd_run_status_fl_object_based(dag_name, proc_schema, proc_name, vars.success_flag) }}}}
        {{% else %}}
            {{{{ fmc_upd_run_status_fl(dag_name, proc_schema, proc_name, vars.success_flag) }}}}
        {{% endif %}}

    {{% endif %}}

{{% endmacro %}}
    """


def deploy_dbt_cloud_fmc(code_dir: str):
    dbt = DbtConn()
    proc_schema = ""
    sources = {}
    fmc_dir = Path(code_dir) / "FMC"

    for fmc_file in fmc_dir.glob("FMC_info_*"):
        fmc_info = json.loads(fmc_file.read_text(encoding="utf-8"))
        print(f"Start processing FMC flow: {fmc_info['dag_name']}")

        if (fmc_dir / fmc_info["map_mtd_file_name"]).exists():
            with open(fmc_dir / fmc_info["map_mtd_file_name"]) as file:
                fmc_flow = json.load(file)
        else:
            with open(fmc_dir / fmc_info["map_mtd_base_file_name"]) as file:
                fmc_flow = json.load(file)

        set_mtd_proc = fmc_flow[0]["mappings"][0]
        proc_schema = set_mtd_proc["map_schema"]
        proc_name = set_mtd_proc["map"]

        if fmc_info["load_type"] == "INCR":
            schedule = json.loads(fmc_info["schedule_interval"])
            set_mtd_cmd = f"dbt run-operation set_fmc_mtd_{fmc_info['flow_type'].lower()}_incr --args '{{dv_name: {fmc_info['dv_code']}, dag_name: {fmc_info['dag_name']}, " \
                          f"proc_schema: {proc_schema}, proc_name: {proc_name}}}'"
        else:
            schedule = {}
            set_mtd_cmd = f"""dbt run-operation set_fmc_mtd_{fmc_info['flow_type'].lower()}_init --args '{{dv_name: {fmc_info['dv_code']}, dag_name: {fmc_info['dag_name']}, """ \
                          f"""proc_schema: {proc_schema}, proc_name: {proc_name}, start_date: "{fmc_info['start_date'].replace('T', ' ')}"}}'"""

        if fmc_info["flow_type"] == "FL":
            flow_cmd = f"dbt run --select tag:{fmc_info['src_name']} --vars '{{load_type: {fmc_info['load_type']}, source: {fmc_info['src_name']}}}'"

            if fmc_info["src_name"] not in sources:
                sources[fmc_info["src_name"]] = {}
            sources[fmc_info["src_name"]]["proc_name"] = fmc_flow[-1]["mappings"][0]["map"]
            sources[fmc_info["src_name"]][fmc_info["load_type"]] = fmc_info["dag_name"]
            sources[fmc_info["src_name"]]["object_based_window"] = fmc_info["object_based_window"]

        else:
            flow_cmd = f"dbt run --select tag:BV --vars '{{load_type: {fmc_info['load_type']}, source: BV}}'"

            if "BV" not in sources:
                sources["BV"] = {}
            sources["BV"]["proc_name"] = fmc_flow[-1]["mappings"][0]["map"]
            sources["BV"][fmc_info["load_type"]] = fmc_info["dag_name"]
            sources["BV"]["object_based_window"] = False

        dbt.remove_job(fmc_info['dag_name'])
        dbt.create_job(fmc_info['dag_name'], [set_mtd_cmd, flow_cmd], schedule)

    print(sources)
    with (Path(code_dir) / "macros" / "fmc_set_status.sql").open(mode="w", newline="") as f:
        f.write(generate_status_update_macro(proc_schema, sources))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="dbt FMC",
        description="Deploy the generic FMC to dbt cloud",
        epilog=""
    )
    parser.add_argument(
        "code_dir",
        help="Directory containing the generated code",
    )
    args = parser.parse_args()
    deploy_dbt_cloud_fmc(args.code_dir)
