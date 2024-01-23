# Package: BulkDNS
# Module: core/single_proc
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-01-23

import logging

from core import domain_ops

log = logging.getLogger('main')


# This is using single query for select all domains in single table that should be checked
# and single param query to update checked domains in that table one by one
def single_process_run(db, tbl_names, tld):

    log.info(f'Preparing params data...')
    tasks = domain_ops.params_preparation(db, tbl_names, tld)
    worker_id = '0'

    for task in tasks:
        db = task[0]
        table = task[1]
        param = task[2]
        log.info(f'Checking {table} | {param}')
        domain_ops.run_domain_check_param(db, table, param, worker_id)
