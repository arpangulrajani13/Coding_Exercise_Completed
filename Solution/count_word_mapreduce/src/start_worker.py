from mapreduce.task_type import TaskType
from uuid import uuid4
import grpc
import stub.dist_mr_pb2_grpc as dist_mr_pb2_grpc
import stub.dist_mr_pb2 as dist_mr_pb2
from mapreduce.task_status import TaskStatus
from src.utils import do_map, do_reduce

def main():
    worker_id = str(uuid4())
    print("Worker id: " + worker_id)
    channel = grpc.insecure_channel('localhost:50051')
    stub = dist_mr_pb2_grpc.MapReduceDriverStub(channel)
    retry = 5
    task_status = TaskStatus.UNDEFINED.value

    while True:
        assigned_task = stub.GetTask(dist_mr_pb2.WorkerStatus(worker_id=worker_id,
                                                              worker_status=0,
                                                              task_status=task_status),
                                     wait_for_ready=True)
        if not assigned_task.task_id:
            print("No task assigned yet. Retrying.....")
            retry -= 1
            task_status = TaskStatus.UNDEFINED
            if retry == 0:
                print("No task assigned for too long. Exiting...")
                exit(0)
        do_task(assigned_task)

        # No task is completed.
        task_status = TaskStatus.COMPLETED.value


def do_task(assigned_task):
    task_type = assigned_task.task_type
    if task_type == TaskType.MAP.value:
        print("Map task assigned: {}\nWith ID: {}\nFor: {}".format(assigned_task.task_id,
                                                                   assigned_task.map_id,
                                                                   assigned_task.input_filename))
        do_map(assigned_task.input_filename, assigned_task.M, assigned_task.map_id)
    elif task_type == TaskType.REDUCE.value:
        print("Reduce task assigned: {}\nWith ID: {}".format(assigned_task.task_id,
                                                                      assigned_task.reduce_id))
        do_reduce(assigned_task.reduce_id, assigned_task.N)
    return


if __name__ == "__main__":
    main()
