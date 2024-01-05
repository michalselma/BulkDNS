# Package: BulkDNS
# Module: core/proc_core
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-01-05

import logging

from common import sqlite
from common import postgresql
from core import single_proc
from core import multi_proc

log = logging.getLogger('main')


def run(config_dta):
    tld = config_dta['DEFAULT']['tld']
    # remove spaces and split key/value(string) to list using comma as separator of items
    tbl_names = config_dta['DEFAULT']['tbl_names'].replace(" ", "").split(",")

    db_type = config_dta['DEFAULT']['db_type']
    db_name = config_dta['DEFAULT']['db_name']
    db_retry_limit = config_dta['DEFAULT']['db_retry_limit']

    # Prepare default db object
    if db_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        db = sqlite.DB(db_type, db_path, db_name, db_retry_limit)
    elif db_type == 'postgres':
        cfg_db = config_dta['DB.postgres']
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db = postgresql.DB(db_type, db_name, db_host, db_port, user_name, user_password, db_retry_limit)
    else:
        log.critical(f'Error: Incorrect database type')
        return

    log.info('1 - Check domains [single processing]')
    log.info('2 - Check domains [multiprocessing]')
    log.info('3 - Check domains [multithreading]')
    log.info('Choose option and press Enter: ')
    user_option = input()

    if user_option == '1':
        single_proc.single_process_run(db, tbl_names, tld)

    elif user_option == '2':
        multi_proc.multiprocess_run(db, tbl_names, tld)

    elif user_option == '3':
        log.info('Not implemented yet...')

    else:
        log.info('Incorrect option picked')
        return
