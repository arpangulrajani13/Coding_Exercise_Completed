"""
This program is used to start or stop the server.
"""

from __future__ import print_function
from mapreduce.driver import Driver
from mapreduce.driver_service import DriverService
import grpc
import time
import src.stub.dist_mr_pb2_grpc
from concurrent import futures


def start_server(port, driver):
    """
    Start the server
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    src.stub.dist_mr_pb2_grpc.add_MapReduceDriverServicer_to_server(DriverService(driver=driver),
                                                                    server)
    server.add_insecure_port('[::]:' + str(port))
    server.start()
    print("...Server started.....")
    return server


def stop_server(server, driver):
    print("...Starting server in 20s...")
    time.sleep(20)
    print(".....Completed tasks........")
    driver.print_complete_tasks_report()
    print(".....Completed tasks........")
    print(".....Shutting down server.....")
    server.stop(0)


def main(args):
    driver = Driver(args.n, args.m)
    server = start_server(50051, driver)
    while True:
        if driver.all_tasks_completed():
            print("...All tasks completed.....")
            stop_server(server, driver)
            exit(0)
