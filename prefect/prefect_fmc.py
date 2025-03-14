from dataclasses import dataclass
from typing import Tuple, List, Set, Dict
import json
from collections import deque
from copy import deepcopy
import os
from functools import cache
from datetime import datetime
import logging
import uuid

from prefect import task, flow
from jinja2 import Environment, FileSystemLoader, Template

_logger = logging.getLogger(__name__)


@cache
def get_proc_template() -> Template:
    loader = FileSystemLoader(os.getcwd())
    env = Environment(loader=loader)
    proc_template = env.get_template("templates/sf_proc_template.sql")
    return proc_template


@cache
def get_env() -> str:
    env = os.getenv("FMC_ENVIRONMENT", "TEST")  # expect dev or prod
    return env

@cache
def get_flow_uuid() -> int:
    return uuid.uuid4().int

@cache
def get_dag_name() -> str:
    # basically we just need dag_name and thats it?
    with open("example_data/fl_info.json", "r") as f:
        data = f.read()
        fl_info = json.loads(data)
        dag_name = fl_info['dag_name']
    return dag_name



def get_flow() -> dict:
    with open("example_data/ungrouped_flow.json", "r") as f:
        data = f.read()
        flow_dict = json.loads(data)
    return flow_dict



def sql_executor(sql: str) -> int:
    # ---- Edit this section based on your needs! -------
    from snowflake.connector import connect

    success = 1

    try:
        conn_params = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }
        
        conn = connect(**conn_params)

        with conn.cursor() as cur:
            cur.execute(sql).fetchall()
    except Exception as e:
        _logger.error(f"Error Running SQL: {sql}\n ERROR: {e}")
        success = 0
    
    return success


@dataclass(frozen=True)
class Task:
    name: str  # map name
    schema: str  # map schema
    proc_input_parameters: Dict

    def run(self, *prev_task_success):
        @task(name=self.name)
        def execute(*prev_task_success) -> tuple:
            # prev_task_results is needed for prefect to link tasks automatically

            input_params = deepcopy(self.proc_input_parameters)
            success_statuses = [ status[0] for status in list(prev_task_success)]
            if "success_flag" in input_params.keys():
                if 0 in list(success_statuses):
                    input_params.update({"success_flag": 0})
                else: 
                    input_params.update({"success_flag": 1})

            _logger.debug(f"Running Task: {self.name}")
            sql = get_proc_template().render(
                {
                    "map_name": self.name,
                    "map_schema": self.schema,
                    "input_parameters": input_params,
                }
            )
            _logger.debug(sql)

            # we'll only actually execute against Snowflake if we are not in a test environment
            if get_env() == "TEST":
                # prefect wont track an int 0 or 1
                # maybe this is because they all use the same memory address in python
                # maybe some hashing going on too
                return (1,sql)

            success = sql_executor(sql)

            return (success,sql)

        return execute(*prev_task_success)

@cache
def get_input_variable_map() -> dict:
    # Some proc_input_parameters need to come after the tasks are executed,
    # these should be passed through prev_task_results instead
    # https://docs.vaultspeed.com/space/VPD/3364388956/Generic+FMC#Input-Variables

    input_variable_map = {
        "dag_name": get_dag_name(),
        "load_cycle_id": get_flow_uuid(),
        "load_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "FMC_BEGIN_LW_TIMESTAMP": ValueError("FMC_BEGIN_LW_TIMESTAMP Not Supported"),
        "success_flag": None, # need to get from prev task results
        "upd_all_objects": "N",
    }

    return input_variable_map

def resolve_input_variables(inputs: List[str]) -> Dict:

    value_map: Dict = get_input_variable_map()
    values = {}
    try:
        for input in inputs:
            value = value_map[input]
            if isinstance(value, ValueError):
                raise value
            values.update({input: value})
    except Exception as e:
        _logger.error(f"Error Resolving Map For Input: {input}")
        raise e

    return values



def create_tasks(flow_dict: dict) -> Tuple[Dict[str, Task], Dict[str, Set[str]]]:
    # we get:
    # name: task object
    # name: task depenency names

    tasks = {}
    task_dependencies = {}
    for key, value in flow_dict.items():
        input_params: Dict = resolve_input_variables(value['input'])
        task = Task(
            name=key,
            schema=value["map_schema"],
            proc_input_parameters=input_params,
        )
        tasks.update({key: task})
        task_dependencies[key] = set(value.get("dependencies", []))

    return tasks, task_dependencies


def get_degree_zero_tasks(
    tasks: Dict[str, Task], task_dependencies: Dict[str, Set[str]]
) -> List[Task]:
    # (0 edges)
    no_deps_tasks = []
    for key, value in task_dependencies.items():
        if len(value) == 0:
            no_deps_tasks.append(tasks[key])

    return no_deps_tasks


def run_flow():
    flow_dict = get_flow()
    tasks, task_dependencies = create_tasks(flow_dict)
    task_degree_tracker = deepcopy(task_dependencies)  # for tracking degree
    task_results = {}  # task: result -> to pull for next task

    degree_zero_tasks = get_degree_zero_tasks(tasks, task_dependencies)
    queue = deque(degree_zero_tasks)

    # remove zero degree tasks from task_degree_tracker
    for node in degree_zero_tasks:
        task_degree_tracker.pop(node.name)

    while queue:
        node = queue.popleft()
        parent_results = []
        for parent in task_dependencies[node.name]:
            if parent in list(task_results.keys()):
                parent_results.append(task_results[parent])

        _logger.info(f"Running Node {node.name} With Parents: \n   {parent_results}")
        task_results[node.name] = node.run(*parent_results)

        # update node dependency degrees
        for key in list(task_degree_tracker.keys()):
            values = task_degree_tracker[key]
            values.discard(node.name)

        # update queue with nodes that no longer have dependencies
        for key in list(task_degree_tracker.keys()):
            values = task_degree_tracker[key]
            if len(values) == 0:
                # update queue
                queue.append(tasks[key])
                # remove from task_degree_tracker
                task_degree_tracker.pop(key)

    return task_results


@flow
def execute_task_tree():
    return run_flow()


if __name__ == "__main__":

    _logger.setLevel(logging.DEBUG)

    result = execute_task_tree()

    # Note: visualization may "fail" eventhough it executes correctly in Prefect
    # execute_task_tree.visualize()

    # TODO - add CLI
