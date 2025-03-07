from dataclasses import dataclass
from typing import Tuple, List, Set, Dict
import json
from collections import deque
from copy import deepcopy
import os
from functools import cache
import logging

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


def get_flow() -> dict:
    with open("example_data/ungrouped_flow.json", "r") as f:
        data = f.read()
        flow_dict = json.loads(data)
    return flow_dict


def sql_executor(sql: str):
    # ---- Edit this section based on your needs! -------
    from snowflake.connector import connect

    conn_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    }
    conn = connect(**conn_params)

    with conn.cursor() as cur:
        print(cur.execute(sql).fetchall())

    return


@dataclass(frozen=True)
class Task:
    name: str  # map name
    schema: str  # map schema
    proc_input_parameters: Tuple[str]

    def run(self, *prev_task_results):
        @task(name=self.name)
        def execute(*prev_task_results):
            # prev_task_results is needed for prefect to link tasks automatically
            _logger.debug(f"Running Task: {self.name}")
            sql = get_proc_template().render(
                {
                    "map_name": self.name,
                    "map_schema": self.schema,
                    "input_parameters": self.proc_input_parameters,
                }
            )
            _logger.debug(sql)

            # we'll only actually execute against Snowflake if we are not in a test environment
            if get_env() == "TEST":
                return sql

            sql_executor(sql)

            return sql

        return execute(*prev_task_results)


def create_tasks(flow_dict: dict) -> Tuple[Dict[str, Task], Dict[str, Set[str]]]:
    # we get:
    # name: task object
    # name: task depenency names

    tasks = {}
    task_dependencies = {}
    for key, value in flow_dict.items():
        task = Task(
            name=key,
            schema=value["map_schema"],
            proc_input_parameters=tuple(value["input"]),
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
