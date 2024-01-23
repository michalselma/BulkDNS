# Package: BulkDNS
# Module: core/multi_proc
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-01-23

import multiprocessing
import signal
import time
import logging

from core import domain_ops


log = logging.getLogger('main')


# Below is for keyboard interrupt Signal catch
def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def worker(task):
    db = task[0]
    table = task[1]
    param = task[2]
    process = multiprocessing.current_process()
    worker_id = process.name[16:]  # Remove 'SpawnPoolWorker-' from thread.name()
    log.debug(f'{process.name} PID {process.pid} | Task: {param} / {table} | START')
    domain_ops.run_domain_check_param(db, table, param, worker_id)
    # Below will only kill active tasks but will keep pool open - something to follow up
    # try:
    #     domain_ops.run_domain_check_param(db, table, param, worker_id)
    # except KeyboardInterrupt:
    #     log.error(f"{process.name} PID {process.pid} interrupted. Task {param}/{table} terminated.")
    #     process.terminate()
    log.debug(f'{process.name} PID {process.pid} | Task: {param} / {table} | END')


def multiprocess_run(db, tbl_names, tld):
    cpu = multiprocessing.cpu_count()
    processes_limit = cpu  # Set the maximum number of parallel processes
    process_clean = 50  # Restart process every x tasks executed, to free up reserved and abandon OS resources.
    log.info(f'Available CPU: {cpu} | Parallel process to be executed: {processes_limit}')

    log.info(f'Preparing params data...')
    tasks = domain_ops.params_preparation(db, tbl_names, tld)

    # Start processing
    pool = multiprocessing.Pool(processes_limit, init_worker, maxtasksperchild=process_clean)
    result = pool.map_async(worker, tasks)
    try:
        # This loop is to monitor and identify Keyboard interrupt exception
        while not result.ready():
            time.sleep(1)
    except KeyboardInterrupt:
        log.error("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
        pool.join()
    else:
        log.info("Multiprocessing finished")
        pool.close()
        pool.join()

    # # Old code that is not able to handle Keyboard Interrupt. Just for reference.
    # with multiprocessing.Pool(processes=max_processes) as pool:
    #     try:
    #         # Map the worker function to the tasks
    #         pool.map(worker, tasks)
    #     except KeyboardInterrupt:
    #         print("Ctrl+C pressed. Terminating processes...")
    #         pool.terminate()
    #         pool.join()
    #         print("Processes terminated.")
    #     except Exception as e:
    #         print(f"An unexpected error occurred: {e}")
    #         pool.terminate()
    #         pool.join()
    #         print("Processes terminated.")
    #     pool.close()
    #     pool.join()
