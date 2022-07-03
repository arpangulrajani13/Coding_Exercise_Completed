import os
import sys
import uuid
import multiprocessing
from itertools import product
from pathlib import Path

from src.utils import *
from task_status import TaskStatus
from task_type import TaskType
import src.stub.dist_mr_pb2 as dist_mr_pb2


class Driver(object):
    def __init__(self, N, M):
        self.N = N
        self.M = M
        self.map_task_files = []
        self.reduce_task_files = []
        self.task_status_manager = multiprocessing.Manager()
        self.task_status = self.task_status_manager.dict()

    def mr_file_storage(self):
        Path(INTERMEDIATE_DIR).mkdir(parents=True, exist_ok=True)
        for n, m in list(product(range(self.N), range(self.M))):
            with open(os.path.join(INTERMEDIATE_DIR, f"mr-{n}-{m}"), "w") as f:
                f.close()

        Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
        for m in range(self.M):
            with open(os.path.join(OUTPUT_DIR, f"out-{m}"), "w") as f:
                f.close()

    def collect_tasks(self, task_type=TaskType.MAP.value):
        if task_type == TaskType.MAP.value:
            self.map_task_files = collect_map_tasks()
            self.build_task_status(task_type)

        elif task_type == TaskType.REDUCE.value:
            self.reduce_task_files = collect_reduce_tasks()
            self.build_task_status(task_type)

        else:
            raise AttributeError(f"Invalid task type -> {task_type}")

    def build_task_status(self, task_type):
        # while building the task all are status 0
        new_tasks = self.task_status_manager.list()

        if task_type == TaskType.MAP.value:
            tasks = self.map_task_files
            for i, task in enumerate(tasks):
                new_tasks.append({"task_id": str(uuid.uuid4()),
                                  "file_name": task,
                                  "worked_id": None,
                                  "map_id": i % self.N})

        elif task_type == TaskType.REDUCE.value:
            for m in range(self.M):
                new_tasks.append({"task_id": str(uuid.uuid4()),
                                  "worked_id": None,
                                  "reduce_id": m})

        else:
            raise AttributeError(f"Invalid task type -> {task_type}")

        if self.task_status.get(TaskStatus.NOT_STARTED.value, None) is None:
            self.task_status[TaskStatus.NOT_STARTED.value] = self.task_status_manager.dict()
        self.task_status[TaskStatus.NOT_STARTED.value][task_type] = new_tasks

        if self.task_status.get(TaskStatus.COMPLETED.value, None) is None:
            self.task_status[TaskStatus.COMPLETED.value] = self.task_status_manager.dict()
        self.task_status[TaskStatus.COMPLETED.value][task_type] = self.task_status_manager.list()

        if self.task_status.get(TaskStatus.IN_PROGRESS.value, None) is None:
            self.task_status[TaskStatus.IN_PROGRESS.value] = self.task_status_manager.dict()
        self.task_status[TaskStatus.IN_PROGRESS.value][task_type] = self.task_status_manager.list()

        if self.task_status.get(TaskStatus.FAILED.value, None) is None:
            self.task_status[TaskStatus.FAILED.value] = self.task_status_manager.dict()
        self.task_status[TaskStatus.FAILED.value][task_type] = self.task_status_manager.list()

        if self.task_status.get(TaskStatus.TIMEOUT.value, None) is None:
            self.task_status[TaskStatus.TIMEOUT.value] = self.task_status_manager.dict()
        self.task_status[TaskStatus.TIMEOUT.value][task_type] = self.task_status_manager.list()

        if self.task_status.get(TaskStatus.KILLED.value, None) is None:
            self.task_status[TaskStatus.KILLED.value] = self.task_status_manager.dict()
        self.task_status[TaskStatus.KILLED.value][task_type] = self.task_status_manager.list()

        if self.task_status.get(TaskStatus.UNKNOWN.value, None) is None:
            self.task_status[TaskStatus.UNKNOWN.value] = self.task_status_manager.dict()
        self.task_status[TaskStatus.UNKNOWN.value][task_type] = self.task_status_manager.list()

    def is_map_task_completed(self):
        return len(self.task_status[TaskStatus.COMPLETED.value][TaskType.MAP.value]) == len(self.map_task_files)

    def is_reduce_task_completed(self):
        return len(self.task_status[TaskStatus.COMPLETED.value][TaskType.REDUCE.value]) == self.M

    def get_next_task(self):
        if not self.is_map_task_completed():
            task_type = TaskType.MAP.value
        else:
            task_type = TaskType.REDUCE.value

        try:
            next_task = self.task_status[TaskStatus.NOT_STARTED.value][task_type].pop(0)
        except IndexError:
            next_task = {}

        return task_type, next_task

    def update_task_status(self, worker_id, filename, old_status, task_type, task_id):
        if task_id is not None:
            if old_status == TaskStatus.NOT_STARTED.value:
                new_status = TaskStatus.IN_PROGRESS.value

            elif old_status == TaskStatus.IN_PROGRESS.value:
                new_status = TaskStatus.COMPLETED.value

            tasks = self.task_status[new_status][task_type]
            tasks.append({"task_id": task_id,
                            "file_name": filename,
                            "worked_id": worker_id})

            if self.task_status.get(new_status, None) is None:
                self.task_status[new_status] = {task_type: tasks}

            else:
                self.task_status[new_status][task_type] = tasks

    def get_prev_task_for_worker(self, worker_id, task_status):
        # if the status is undefined, the worker is just starting execution,
        # and so attempts a map operation
        if task_status == TaskStatus.UNDEFINED.value:
            return TaskType.MAP.value, None

        prev_task = None
        for task_type in TaskType:
            in_progress_tasks = self.task_status[TaskStatus.IN_PROGRESS.value].get(task_type.value,
                                                                                   self.task_status_manager.list())
            for i, in_progress_task in enumerate(in_progress_tasks):
                if in_progress_task["worked_id"] == worker_id:
                    try:
                        prev_task = self.task_status[TaskStatus.IN_PROGRESS.value][task_type.value].pop(i)
                        return task_type.value, prev_task
                    except IndexError:
                        break

        return task_type.value, prev_task

    def all_tasks_completed(self):
        return self.is_map_task_completed() and self.is_reduce_task_completed()

    def print_complete_tasks_report(self):
        print(self.task_status[TaskStatus.COMPLETED.value][0])
        print("********************************************************")
        print(self.task_status[TaskStatus.COMPLETED.value][1])
