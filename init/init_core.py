# Package: BulkDNS
# Module: init/init_core
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-02-16

import logging

from init import init_db
from init import init_domain
from common import sqlite
from common import postgresql

log = logging.getLogger('main')


def run(config_dta):
    db_domain_type = config_dta['DOMAIN']['db_type']
    db_domain_name = config_dta['DOMAIN']['db_name']
    tld_domain = config_dta['DOMAIN']['tld']
    # remove spaces and split key/value(string) to list using comma as separator of items
    tbl_domain_names = config_dta['DOMAIN']['tbl_names'].replace(" ", "").split(",")

    db_dict_type = config_dta['DICTIONARY']['db_type']
    db_dict_name = config_dta['DICTIONARY']['db_name']
    tld_dict = config_dta['DICTIONARY']['tld']
    # remove spaces and split key/value(string) to list using comma as separator of items
    tbl_dict_names = config_dta['DICTIONARY']['tbl_names'].replace(" ", "").split(",")

    db_retry_limit = int(config_dta['DEFAULT']['db_retry_limit'])
    db_retry_sleep_time = int(config_dta['DEFAULT']['db_retry_sleep_time'])

    db_backup_type = config_dta['DEFAULT']['db_backup_type']

    # Prepare default domain db object
    if db_domain_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        sys_db = cfg_db['sys_db']  # Taken from config based on specific DB.* config section driven by db_type
        db_domain = sqlite.DB(db_domain_type, db_path, sys_db, db_retry_limit)
    elif db_domain_type == 'postgresql':
        cfg_db = config_dta['DB.postgres']
        sys_db = cfg_db['sys_db']  # Taken from config based on specific DB.* config section driven by db_type
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db_domain = postgresql.DB(db_domain_type, sys_db, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
    else:
        log.critical(f'Error: Incorrect database type')
        return

    # Prepare default dictionary db object
    if db_dict_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        sys_db = cfg_db['sys_db']  # Taken from config based on specific DB.* config section driven by db_type
        db_dict = sqlite.DB(db_dict_type, db_path, sys_db, db_retry_limit)
    elif db_dict_type == 'postgresql':
        cfg_db = config_dta['DB.postgres']
        sys_db = cfg_db['sys_db']  # Taken from config based on specific DB.* config section driven by db_type
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db_dict = postgresql.DB(db_dict_type, sys_db, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
    else:
        log.critical(f'Error: Incorrect database type')
        return

    # Prepare default backup db object
    if db_backup_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        sys_db = cfg_db['sys_db']  # Taken from config based on specific DB.* config section driven by db_type
        db_backup = sqlite.DB(db_backup_type, db_path, sys_db, db_retry_limit)
    elif db_backup_type == 'postgresql':
        cfg_db = config_dta['DB.postgres']
        sys_db = cfg_db['sys_db']  # Taken from config based on specific DB.* config section driven by db_type
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db_backup = postgresql.DB(db_backup_type, sys_db, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
    else:
        log.critical(f'Error: Incorrect database type')
        return

    log.info('1 - Initialize DB and create tables structure')
    log.info('2 - Generate domains')
    log.info('3 - Upload dictionaries')
    log.info('Choose option and press Enter: ')
    user_option = input()

    if user_option == '1':
        # Main operations database for char combinations, taken domains to be moved then to '_taken' db in separate process
        new_db_name = db_domain_name
        init_db.initialize(db_domain, new_db_name, tbl_domain_names, tld_domain)

        # Below is to store domains that are taken. Archive and bring back if needed based on domain status change to
        # be managed by archiver module.
        new_db_name = f'{db_domain_name}_taken'
        init_db.initialize(db_domain, new_db_name, tbl_domain_names, tld_domain)

        # Below is backup DB common for live and taken DBs. Data backup is managed by archiver module.
        db_backup_name = f'{db_domain_name}_backup'
        init_db.initialize(db_backup, db_backup_name, tbl_domain_names, tld_domain)

        # The same for dictionary DB structure
        new_db_name = db_dict_name
        init_db.initialize(db_dict, new_db_name, tbl_dict_names, tld_dict)

        new_db_name = f'{db_dict_name}_taken'
        init_db.initialize(db_dict, new_db_name, tbl_dict_names, tld_dict)

        db_backup_name = f'{db_dict_name}_backup'
        init_db.initialize(db_backup, db_backup_name, tbl_dict_names, tld_dict)

    elif user_option == '2':
        # Generate char-based domain dictionaries (one, two, three and four char combinations)
        db_domain.db_name = db_domain_name  # Modify default db_name to use domain database
        init_domain.create_domain_dta(db_domain, tbl_domain_names, tld_domain)

    elif user_option == '3':
        # Upload words list
        print('Not implemented yet...')

    else:
        log.info('Incorrect option picked')
        return
