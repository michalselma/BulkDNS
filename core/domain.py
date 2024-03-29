# Package: BulkDNS
# Module: core/domain
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-02-22

import datetime
import logging

from core import rdap
from core import whois

log = logging.getLogger('main')

tbl_names_none_param = ['two_digit', 'two_letter', 'two_digit_letter']
tbl_names_short_param = ['three_digit', 'three_letter', 'three_special','generic']
tbl_names_medium_param = ['three_digit_letter', 'four_digit', 'four_letter', 'four_digit_letter', 'four_special', 'english']


def params_preparation(db, tbl_names, tld, check_type, protocol):
    params_to_process = []
    for tbl_name in tbl_names:
        table = f'{tbl_name}_{tld}'
        call_id = f'{table} | SELECT'

        # Get all that has never been checked OR expiring in next 30 days but not checked within last 7 days
        exp_date = datetime.datetime.now() + datetime.timedelta(days=30)
        exp_date = datetime.datetime(exp_date.year, exp_date.month, exp_date.day, 0, 0, 0)
        updated_date = datetime.datetime.now() - datetime.timedelta(days=7)
        updated_date = datetime.datetime(updated_date.year, updated_date.month, updated_date.day, 0, 0, 0)

        if tbl_name in tbl_names_none_param:
            substr = 'substr(name, 1, 0)'
        elif tbl_name in tbl_names_short_param:
            substr = 'substr(name, 1, 1)'
        elif tbl_name in tbl_names_medium_param:
            substr = 'substr(name, 1, 2)'
        else:
            substr = 'substr(name, 1, 3)'

        # Expiring domains check or available domains re-check will be executed only for domains with last check earlier than 7 days ago
        if check_type == 'expiring':
            sql_select = (f"SELECT DISTINCT {substr}, count(*) FROM {table} WHERE updated is null OR (expiry<='{exp_date}' AND updated<='{updated_date}') GROUP BY {substr} ORDER BY count(*), {substr}")
        else:  # This covers check_type == 'recheck'
            sql_select = (f"SELECT DISTINCT {substr}, count(*) FROM {table} WHERE avail='Y' AND updated<='{updated_date}' GROUP BY {substr} ORDER BY count(*), {substr}")

        result = db.execute_single(sql_select, call_id)

        if result is []:
            continue
        else:
            for param in result:
                params_to_process.append([db, table, param[0], exp_date, updated_date, check_type, protocol])

    log.info(f'Tasks (params) to process: {len(params_to_process)}')
    return params_to_process


# This use single query to select all domains from single table that should be checked and starts with 'param' value
# Is using multi param query to execute update of checked domains in the groups of 40 or less if items left < 40
def run_domain_check_param_whois(db, table, param, exp_date, updated_date, check_type, worker_id):

    if check_type == 'expiring':
        sql_select = f"SELECT name, tld FROM {table} WHERE (updated is null AND domain LIKE '{param}%') OR (expiry<='{exp_date}' AND updated<='{updated_date}' AND domain LIKE '{param}%')"
    else:  # This covers check_type == 'recheck'
        sql_select = f"SELECT name, tld FROM {table} WHERE avail='Y' AND updated<='{updated_date}' AND domain LIKE '{param}%'"

    call_id = f'{table} {param} | SELECT'
    sql_result = db.execute_single(sql_select, call_id)

    items_amount = len(sql_result)  # Items to be processed
    if items_amount == 0:
        log.info(f'No {param} items to process in {table}.')
        return  # Stop processing
    log.debug(f'Worker {worker_id} | {param} {table} | Items to process {items_amount}')
    print(f'Worker \033[95m{worker_id:>3}\033[00m \033[94m|\033[00m \033[93m{param}\033[00m {table} \033[94m|\033[00m Items to process \033[91m{items_amount}\033[00m')

    # Define variables
    items_left = items_amount  # For logic of when to execute DB update
    db_execute_trigger = 0  # Counter for triggering DB update
    processed_current_round = 0  # Counter for processed items in current round before update execute
    processed_counter = 0  # For on-screen information of processing progress
    failed = 0
    sql_params_array = []  # Array of checked domains data for executemany() DB update

    # Check each domain from the list one by one
    for item in sql_result:
        name = item[0]
        tld = item[1]

        domain_dta = whois.get_domain_data(name, tld, 10)  # Execute whois check

        items_left -= 1
        processed_counter += 1
        processed_current_round += 1

        # If domain check successful add to sql_params_array for future DB execution
        if domain_dta[6] == 0:  # Validating return code (exec_code) from get_domain_dta() function - '0' means OK
            log.debug(f'{domain_dta[0]} | Domain available: {domain_dta[3]} | Retries: {domain_dta[8]}')
            sql_params = (domain_dta[3], domain_dta[4], domain_dta[5], domain_dta[0])
            # Collect params into array and execute mass update later on
            sql_params_array.append(sql_params)
            db_execute_trigger += 1

        else:
            log.error(f'{domain_dta[0]} | Error getting whois data after {domain_dta[8]} retries: {domain_dta[7]}')
            print(f'{domain_dta[0]} | Error getting whois data after {domain_dta[8]} retries: {domain_dta[7]}')
            failed += 1

        # DB update logic:
        # items_left > 0 :
        #       trigger >= 40 -> execute update
        #       trigger < 40  -> continue
        # items_left = 0 :
        #       trigger > 0 -> execute update
        #       trigger = 0 -> continue

        # if items left to process are more than 0 and items successfully checked in current round (execute trigger)
        # are less than 40 continue to next item
        if items_left > 0 and db_execute_trigger < 40:
            continue  # Do nothing, just move to check next item
            
        # if items left to process are 0 and items successfully checked in current round (execute trigger) are 0
        # this is last item, so show summary of this last item and 'continue' to finish loop.
        elif items_left == 0 and db_execute_trigger == 0:
            # Percent value:
            percent = processed_counter / items_amount * 100
            # Percent formatted value:
            percent = "{0:.2f}".format(percent)
            log.debug(f'Worker {worker_id} | {param} {table} | Processed {percent}% ({processed_counter}/{items_amount}) | Failed {failed} Left {items_left}')
            print(f'Worker \033[95m{worker_id:>3}\033[00m \033[94m|\033[00m \033[93m{param}\033[00m {table} \033[94m|\033[00m '
                  f'Processed \033[92m{percent:>6}%\033[00m ({processed_counter}/{items_amount}) \033[94m|\033[00m '
                  f'Failed \033[91m{failed}\033[00m Left \033[92m{items_left}\033[00m')
            continue  # Do nothing, just close the loop

        # if items left to process are more than 0 and items successfully checked in current round (execute trigger)
        # are >= 40 or if items left to process are 0, but items successfully checked more than 0
        # execute update, clean db execute trigger and clean params array
        elif (items_left > 0 and db_execute_trigger >= 40) or (items_left == 0 and db_execute_trigger > 0):
            call_id = f'{param} | UPDATE at {domain_dta[0]}'
            log.debug(f'Worker {worker_id} | {param} {table} | DB update at {domain_dta[0]} | {db_execute_trigger} of {processed_current_round} items successfully verified')

            # Percent value:
            percent = processed_counter / items_amount * 100
            # Percent formatted value:
            percent = "{0:.2f}".format(percent)
            log.debug(f'Worker {worker_id} | {param} {table} | Processed {percent}% ({processed_counter}/{items_amount}) | Failed {failed} Left {items_left}')
            print(f'Worker \033[95m{worker_id:>3}\033[00m \033[94m|\033[00m \033[93m{param}\033[00m {table} \033[94m|\033[00m '
                  f'Processed \033[92m{percent:>6}%\033[00m ({processed_counter}/{items_amount}) \033[94m|\033[00m '
                  f'Failed \033[91m{failed}\033[00m Left \033[92m{items_left}\033[00m')

            if db.db_type == 'sqlite':
                sql_update_param = f'UPDATE {table} SET avail = ?, expiry = ?, updated = ? WHERE domain = ?'
                db.execute_many_param(sql_update_param, sql_params_array, call_id)
            elif db.db_type == 'postgresql':
                sql_update_param = f'UPDATE {table} SET avail = %s, expiry = %s, updated = %s WHERE domain = %s'
                db.execute_many_param(sql_update_param, sql_params_array, call_id)
            else:
                log.error(f'Error: Incorrect database type')
                return  # Stop further processing

            db_execute_trigger = 0
            processed_current_round = 0
            sql_params_array = []

        else:
            log.critical(f'{name}.{tld} | Should not see me! POSSIBLE LOGIC FAILURE!')
            continue


def run_domain_check_param_rdap(db, table, param, exp_date, updated_date, check_type, worker_id):

    if check_type == 'expiring':
        sql_select = f"SELECT name, tld FROM {table} WHERE (updated is null AND domain LIKE '{param}%') OR (expiry<='{exp_date}' AND updated<='{updated_date}' AND domain LIKE '{param}%')"
    else:  # This covers check_type == 'recheck'
        sql_select = f"SELECT name, tld FROM {table} WHERE avail='Y' AND updated<='{updated_date}' AND domain LIKE '{param}%'"

    call_id = f'{table} {param} | SELECT'
    sql_result = db.execute_single(sql_select, call_id)

    items_amount = len(sql_result)  # Items to be processed

    if items_amount == 0:
        log.info(f'No {param} items to process in {table}.')
        return  # Stop processing
    
    log.debug(f'Worker {worker_id} | {param} {table} | Items to process {items_amount}')
    
    # Define variables
    items_left = items_amount  # For logic of when to execute DB update
    db_execute_trigger = 0  # Counter for triggering DB update
    processed_current_round = 0  # Counter for processed items in current round before update execute
    failed = 0
    sql_params_array = []  # Array of checked domains data for executemany() DB update

    # Check each domain from the list one by one
    for item in sql_result:
        name = item[0]
        tld = item[1]

        domain_dta = rdap.query(name, tld, 10)

        items_left -= 1
        processed_current_round += 1

        # If domain check successful add to sql_params_array for future DB execution
        if domain_dta[6] == 0:  # Validating return code (exec_code) from get_domain_dta() function - '0' means OK
            log.debug(f'{domain_dta[0]} | Domain available: {domain_dta[3]} | Retries: {domain_dta[8]}')
            sql_params = (domain_dta[3], domain_dta[4], domain_dta[5], domain_dta[0])
            # Collect params into array and execute mass update later on
            sql_params_array.append(sql_params)
            db_execute_trigger += 1

        else:
            log.error(f'{domain_dta[0]} | Error getting whois data after {domain_dta[8]} retries: {domain_dta[7]}')
            print(f'{domain_dta[0]} | Error getting whois data after {domain_dta[8]} retries: {domain_dta[7]}')
            failed += 1

        # DB update logic:
        # items_left > 0 :
        #       trigger >= 40 -> execute update
        #       trigger < 40  -> continue
        # items_left = 0 :
        #       trigger > 0 -> execute update
        #       trigger = 0 -> continue

        # if items left to process are more than 0 and items successfully checked in current round (execute trigger)
        # are less than 40 continue to next item
        if items_left > 0 and db_execute_trigger < 40:
            continue  # Do nothing, just move to check next item
            
        # if items left to process are 0 and items successfully checked in current round (execute trigger) are 0
        # this is last item, so show summary of this last item and 'continue' to finish loop.
        elif items_left == 0 and db_execute_trigger == 0:
            log.debug(f'Worker {worker_id} | {param} {table} | Items {items_amount} Failed {failed} Left {items_left}')
            print(f'Worker \033[95m{worker_id:>3}\033[00m \033[94m|\033[00m \033[93m{param}\033[00m {table} \033[94m|\033[00m '
                  f'Items \033[93m{items_amount}\033[00m Failed \033[91m{failed}\033[00m Left \033[92m{items_left}\033[00m')
            continue  # Do nothing, just close the loop

        # if items left to process are more than 0 and items successfully checked in current round (execute trigger)
        # are >= 40 or if items left to process are 0, but items successfully checked more than 0
        # execute update, clean db execute trigger and clean params array
        elif (items_left > 0 and db_execute_trigger >= 40) or (items_left == 0 and db_execute_trigger > 0):
            call_id = f'{param} | UPDATE at {domain_dta[0]}'
            log.debug(f'Worker {worker_id} | {param} {table} | DB update at {domain_dta[0]} | {db_execute_trigger} of {processed_current_round} items successfully verified')

            log.debug(f'Worker {worker_id} | {param} {table} | Items {items_amount} Failed {failed} Left {items_left}')
            print(f'Worker \033[95m{worker_id:>3}\033[00m \033[94m|\033[00m \033[93m{param}\033[00m {table} \033[94m|\033[00m '
                  f'Items \033[93m{items_amount}\033[00m Failed \033[91m{failed}\033[00m Left \033[92m{items_left}\033[00m')

            if db.db_type == 'sqlite':
                sql_update_param = f'UPDATE {table} SET avail = ?, expiry = ?, updated = ? WHERE domain = ?'
                db.execute_many_param(sql_update_param, sql_params_array, call_id)
            elif db.db_type == 'postgresql':
                sql_update_param = f'UPDATE {table} SET avail = %s, expiry = %s, updated = %s WHERE domain = %s'
                db.execute_many_param(sql_update_param, sql_params_array, call_id)
            else:
                log.error(f'Error: Incorrect database type')
                return  # Stop further processing

            db_execute_trigger = 0
            processed_current_round = 0
            sql_params_array = []

        else:
            log.critical(f'{name}.{tld} | Should not see me! POSSIBLE LOGIC FAILURE!')
            continue
