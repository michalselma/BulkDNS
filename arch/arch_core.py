# Package: BulkDNS
# Module: arch/arch_core
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-01-23

import logging

from common import sqlite
from common import postgresql
from arch import data_ops

log = logging.getLogger('main')


def run(config_dta):
    tld = config_dta['DEFAULT']['tld']
    # remove spaces and split key/value(string) to list using comma as separator of items
    tbl_names = config_dta['DEFAULT']['tbl_names'].replace(" ", "").split(",")

    db_type = config_dta['DEFAULT']['db_type']
    db_name = config_dta['DEFAULT']['db_name']
    db_arch_name = config_dta['DEFAULT']['db_arch_name']
    db_backup_type = config_dta['DEFAULT']['db_backup_type']
    db_backup_name = config_dta['DEFAULT']['db_backup_name']
    db_retry_limit = int(config_dta['DEFAULT']['db_retry_limit'])
    db_retry_sleep_time = int(config_dta['DEFAULT']['db_retry_sleep_time'])

    # Prepare default db objects
    if db_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        db = sqlite.DB(db_type, db_path, db_name, db_retry_limit)
        db_arch = sqlite.DB(db_type, db_path, db_arch_name, db_retry_limit)
    elif db_type == 'postgresql':
        cfg_db = config_dta['DB.postgres']
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db = postgresql.DB(db_type, db_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
        db_arch = postgresql.DB(db_type, db_arch_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
    else:
        log.critical(f'Error: Incorrect database type')
        return

    log.info('1 - Archive taken domains expiring in more than 30 days')
    log.info('2 - Restore domains expiring in next 30 days')
    log.info('3 - Migrate / Backup all data to the separate database')
    log.info('Choose option and press Enter: ')
    user_option = input()

    if user_option == '1':
        arch_type = 'archive'
        data_ops.archiver(db, db_arch, tbl_names, tld, arch_type)
    elif user_option == '2':
        arch_type = 'restore'
        data_ops.archiver(db, db_arch, tbl_names, tld, arch_type)

    elif user_option == '3':
        if db_backup_type == 'sqlite':
            cfg_db = config_dta['DB.sqlite']
            db_path = cfg_db['db_location']
            db_backup = sqlite.DB(db_type, db_path, db_backup_name, db_retry_limit)
        elif db_backup_type == 'postgres':
            cfg_db = config_dta['DB.postgres']
            db_host = cfg_db['db_host']
            db_port = cfg_db['db_port']
            user_name = cfg_db['user_name']
            user_password = cfg_db['user_password']
            db_backup = postgresql.DB(db_type, db_backup_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
        else:
            log.critical(f'Error: Incorrect backup database type')
            return
        data_ops.backup_data(db, db_arch, db_backup, tbl_names, tld)

    else:
        log.info('Incorrect option picked')
        return
