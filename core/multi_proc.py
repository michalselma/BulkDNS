# Package: BulkDNS
# Module: core/multi_proc
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-02-16

import multiprocessing
import signal
import time
import logging
import gc

from core import domain


log = logging.getLogger('main')


# Below is for keyboard interrupt Signal catch
def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def worker(task):
    db = task[0]
    table = task[1]
    param = task[2]
    exp_date = task[3]
    updated_date = task[4]
    check_type = task[5]
    protocol = task[6]

    process = multiprocessing.current_process()
    worker_id = process.name[16:]  # Remove 'SpawnPoolWorker-' from thread.name()
    
    log.debug(f'{process.name} PID {process.pid} | Task: {param} / {table} | START')

    if protocol == 'rdap':
        domain.run_domain_check_param_rdap(db, table, param, exp_date, updated_date, check_type, worker_id)
    elif protocol == 'whois':
        domain.run_domain_check_param_whois(db, table, param, exp_date, updated_date, check_type, worker_id)
    else:
        log.error(f'Unidentified protocol: {protocol}')
        return

    # Below will only kill active tasks but will keep pool open - something to follow up
    # try:
    #     domain_ops.run_domain_check_param(db, table, param, worker_id)
    # except KeyboardInterrupt:
    #     log.error(f"{process.name} PID {process.pid} interrupted. Task {param}/{table} terminated.")
    #     process.terminate()
    log.debug(f'{process.name} PID {process.pid} | Task: {param} / {table} | END')


def multiprocess_run(db, tbl_names, tld, check_type, protocol):
    gc.enable()  # Enable automatic garbage collection.

    cpu = multiprocessing.cpu_count()
    processes_limit = 2 * cpu  # Set the maximum number of parallel processes
    process_clean = 50  # Restart process every x tasks executed, to free up reserved and abandon OS resources.

    log.info(f'Available CPU: {cpu} | Parallel process to be executed: {processes_limit}')
    log.info(f'Preparing params data...')
    tasks = domain.params_preparation(db, tbl_names, tld, check_type, protocol)

    # Start processing
    pool = multiprocessing.Pool(processes_limit, init_worker, maxtasksperchild=process_clean)
    result = pool.map_async(worker, tasks, chunksize=1)  # chunksize - batch of params for each worker (group tasks and pass each group to worker)
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
