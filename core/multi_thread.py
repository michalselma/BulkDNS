# Package: BulkDNS
# Module: core/multi_thread
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-01-11


import concurrent.futures
import threading
import multiprocessing
import logging
import gc

from core import domain_ops

log = logging.getLogger('main')


def worker(task):
    db = task[0]
    table = task[1]
    param = task[2]
    thread = threading.current_thread()
    worker_id = thread.name[21:]  # Remove 'ThreadPoolExecutor-0_' from thread.name()
    thread_tid = thread.native_id
    log.debug(f'Worker {worker_id} | TID {thread_tid} | Task {param} / {table} | START')
    domain_ops.run_domain_check_param(db, table, param, worker_id)
    log.debug(f'Worker {worker_id} | TID {thread_tid} | Task {param} / {table} | END')
    log.debug(f'Garbage Stats: {gc.get_stats()}')
    # collections is the number of times this generation was collected;
    # collected is the total number of objects collected inside this generation;
    # uncollectable is the total number of objects which were found to be uncollectable (and were therefore moved
    # to the garbage list) inside this generation.


def create_threads(thread_limit, tasks):
    # Pooling all tasks per param (task[2] value) and assigning to separate thread_objects
    with concurrent.futures.ThreadPoolExecutor(max_workers=thread_limit) as executor:
        thread_objects = {executor.submit(worker, task): task[2] for task in tasks}
        # Below will be executed when thread (future) is completed (state change from pending to finished)
        for future in concurrent.futures.as_completed(thread_objects):
            worker_object = thread_objects[future]  # This simply returns task[2] associated to future
            try:
                res = future.result()  # This is return value as result of job done by worker for task[2]
                # pass
            except Exception as ex:
                log.error(f'Error. {worker_object} generated an exception: {ex}')
            else:
                log.debug(f'Task {worker_object} finished and returned: {res}')
                # pass


def multithreading_run(db, tbl_names, tld):
    gc.enable()  # Enable automatic garbage collection.
    cpu = int(multiprocessing.cpu_count())
    thread_limit = 12 * cpu  # Set your desired thread limit
    log.info(f'Available CPU: {cpu} | Parallel threads to be executed: {thread_limit}')
    log.info(f'Preparing params data...')
    tasks = domain_ops.params_preparation(db, tbl_names, tld)
    log.info(f'Creating threads...')
    create_threads(thread_limit, tasks)
