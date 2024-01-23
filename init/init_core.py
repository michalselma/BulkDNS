# Package: BulkDNS
# Module: init/init_core
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-01-23

import logging

from init import init_db
from init import init_dict
from common import sqlite
from common import postgresql

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

    log.info('1 - Initialize DB and create tables structure')
    log.info('2 - Generate dictionaries')
    log.info('Choose option and press Enter: ')
    user_option = input()

    if user_option == '1':
        # DB initialization and top level domain tables create
        sys_db = cfg_db['sys_db']  # Taken from config based on specific DB.* config section driven by db_type above
        db.dbname = sys_db  # Override default dbname with system db name, as default may not yet exist in DB
        init_db.initialize(db, tbl_names, tld)

    elif user_option == '2':
        # Generate char-based domain dictionaries (one, two, three, four and five char combinations)
        init_dict.create_domain_dta(db, tbl_names, tld)

    else:
        log.info('Incorrect option picked')
        return
