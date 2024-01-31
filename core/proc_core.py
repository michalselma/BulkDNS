# Package: BulkDNS
# Module: core/proc_core
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-01-31

import logging

from common import sqlite
from common import postgresql
from core import single_proc
from core import multi_proc
from core import multi_thread

log = logging.getLogger('main')


def run(config_dta):
    tld = config_dta['DEFAULT']['tld']
    # remove spaces and split key/value(string) to list using comma as separator of items
    tbl_names = config_dta['DEFAULT']['tbl_names'].replace(" ", "").split(",")

    db_type = config_dta['DEFAULT']['db_type']
    db_name = config_dta['DEFAULT']['db_name']
    db_retry_limit = int(config_dta['DEFAULT']['db_retry_limit'])
    db_retry_sleep_time = int(config_dta['DEFAULT']['db_retry_sleep_time'])

    # Prepare default db object
    if db_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        db = sqlite.DB(db_type, db_path, db_name, db_retry_limit)
    elif db_type == 'postgresql':
        cfg_db = config_dta['DB.postgres']
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db = postgresql.DB(db_type, db_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
    else:
        log.critical(f'Error: Incorrect database type')
        return

    log.info('1 - Check domains RDAP [single processing]')
    log.info('2 - Check domains WHOIS [single processing]')
    log.info('3 - Check domains RDAP [multiprocessing]')
    log.info('4 - Check domains WHOIS [multiprocessing]')
    log.info('5 - Check domains RDAP [multithreading]')
    log.info('6 - Check domains WHOIS [multithreading]')
    log.info('Choose option and press Enter: ')
    user_option = input()

    if user_option == '1':
        protocol='rdap'
        single_proc.single_process_run(db, tbl_names, tld, protocol)

    if user_option == '2':
        protocol='whois'
        single_proc.single_process_run(db, tbl_names, tld, protocol)

    elif user_option == '3':
        protocol='rdap'
        multi_proc.multiprocess_run(db, tbl_names, tld, protocol)
    
    elif user_option == '4':
        protocol='whois'
        multi_proc.multiprocess_run(db, tbl_names, tld, protocol)

    elif user_option == '5':
        protocol='rdap'
        multi_thread.multithreading_run(db, tbl_names, tld, protocol)

    elif user_option == '6':
        protocol='whois'
        multi_thread.multithreading_run(db, tbl_names, tld, protocol)

    else:
        log.info('Incorrect option picked')
        return
