# Package: BulkDNS
# Module: core/multi_thread
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-02-05


import concurrent.futures
import threading
import multiprocessing
import logging
import gc

from core import domain

log = logging.getLogger('main')


def worker(task):
    db = task[0]
    table = task[1]
    param = task[2]
    exp_date = task[3]
    updated_date = task[4]
    protocol = task[5]

    thread = threading.current_thread()
    worker_id = thread.name[21:]  # Remove 'ThreadPoolExecutor-0_' from thread.name()
    thread_tid = thread.native_id

    log.debug(f'Worker {worker_id} | TID {thread_tid} | Task {param} / {table} | START')

    if protocol == 'rdap':
        domain.run_domain_check_param_rdap(db, table, param, exp_date, updated_date, worker_id)
    elif protocol == 'whois':
        domain.run_domain_check_param_whois(db, table, param, exp_date, updated_date, worker_id)
    else:
        log.error(f'Unidentified protocol: {protocol}')
        return

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
                log.error(f'{worker_object} | Exception: {ex}')
            else:
                log.debug(f'Task {worker_object} finished and returned: {res}')
                # pass


def multithreading_run(db, tbl_names, tld, protocol):
    gc.enable()  # Enable automatic garbage collection.
    
    cpu = int(multiprocessing.cpu_count())

    if protocol == 'rdap':
        thread_limit = 2 * cpu  # Set your desired thread limit
    elif protocol == 'whois':
        thread_limit = 12 * cpu  # Set your desired thread limit
    else:
        log.error (f'Unidentified domain check protocol: {protocol}')        
        return

    log.info(f'Available CPU: {cpu} | Parallel threads to be executed: {thread_limit}')
    log.info(f'Preparing params data...')
    tasks = domain.params_preparation(db, tbl_names, tld, protocol)
    log.info(f'Creating threads...')
    create_threads(thread_limit, tasks)
