# Package: BulkDNS
# Module: core/single_proc
# Author: Michal Selma <michal@selma.cc>
# Rev: 2023-12-15

import logging

from core import domain

log = logging.getLogger('main')


# This is using single query for select all domains in single table that should be checked
# and single param query to update checked domains in that table one by one
def run_domain_check_by_table(db, tbl_names, tld):
    for tbl_name in tbl_names:
        table = f'{tbl_name}_{tld}'
        log.debug(f'Checking domains to be processed from {table}')
        query = f'SELECT name, tld FROM {table} WHERE updated is null'
        call_id = f'{table} | SELECT'
        output = db.execute_single(query, call_id)
        if output is None:
            break  # to quickly handle NoneType error (e.g. no such table)
        for item in output:
            name = item[0]
            tld = item[1]
            log.info(f'Checking: {name}.{tld}')

            dns = domain.DOMAIN(name, tld, 10, 1)  # Create domain object of dns class
            domain_dta = dns.get_domain_data()  # Execute whois check:

            if domain_dta[6] == 0:  # Validating return code (exec_code) from get_domain_dta() function - '0' means OK
                log.info(f'{domain_dta[0]} | Domain available: {domain_dta[3]} | Retried: {domain_dta[8]}')
                sql_params = (domain_dta[3], domain_dta[4], domain_dta[5], domain_dta[0])
                call_id = f'{domain_dta[0]} | UPDATE '
                if db.db_type == 'sqlite':
                    param_query = f'UPDATE {table} SET avail = ?, expiry = ?, updated = ? WHERE domain = ?'
                    db.execute_single_param(param_query, sql_params, call_id)
                elif db.db_type == 'postgresql':
                    param_query = f'UPDATE {table} SET avail = %s, expiry = %s, updated = %s WHERE domain = %s'
                    db.execute_single_param(param_query, sql_params, call_id)
                else:
                    log.info(f'Error: Incorrect database type')
                    return
            else:
                log.info(f'{domain_dta[0]} | Error getting whois data after {domain_dta[8]} retries: {domain_dta[7]}')
