# Package: BulkDNS
# Module: core/single_proc
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-02-16

import logging

from core import domain

log = logging.getLogger('main')


# This is using single query for select all domains in single table that should be checked
# and single param query to update checked domains in that table one by one
def single_process_run(db, tbl_names, tld, check_type, protocol):

    log.info(f'Preparing params data...')
    tasks = domain.params_preparation(db, tbl_names, tld, check_type, protocol)
    
    worker_id = '0'

    for task in tasks:
        db = task[0]
        table = task[1]
        param = task[2]
        exp_date = task[3]
        updated_date = task[4]
        check_type = task[5]
        protocol = task[6]
        
        log.info(f'Checking {table} | {param}')
        if protocol == 'rdap':
            domain.run_domain_check_param_rdap(db, table, param, exp_date, updated_date, check_type, worker_id)
            continue
        elif protocol == 'whois':
            domain.run_domain_check_param_whois(db, table, param, exp_date, updated_date, check_type, worker_id)
            continue
        else:
            log.error(f'Unidentified protocol: {protocol}')
            return
