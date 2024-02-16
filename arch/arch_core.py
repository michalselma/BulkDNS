# Package: BulkDNS
# Module: arch/arch_core
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-02-16

import logging

from common import sqlite
from common import postgresql
from arch import data_ops

log = logging.getLogger('main')


def run(config_dta):
    db_domain_type = config_dta['DOMAIN']['db_type']
    db_domain_name = config_dta['DOMAIN']['db_name']
    db_domain_archive_name = f'{db_domain_name}_taken'
    tld_domain = config_dta['DOMAIN']['tld']
    # remove spaces and split key/value(string) to list using comma as separator of items
    tbl_domain_names = config_dta['DOMAIN']['tbl_names'].replace(" ", "").split(",")

    db_dict_type = config_dta['DICTIONARY']['db_type']
    db_dict_name = config_dta['DICTIONARY']['db_name']
    db_dict_archive_name = f'{db_dict_name}_taken'
    tld_dict = config_dta['DICTIONARY']['tld']
    # remove spaces and split key/value(string) to list using comma as separator of items
    tbl_dict_names = config_dta['DICTIONARY']['tbl_names'].replace(" ", "").split(",")

    db_retry_limit = int(config_dta['DEFAULT']['db_retry_limit'])
    db_retry_sleep_time = int(config_dta['DEFAULT']['db_retry_sleep_time'])

    db_backup_type = config_dta['DEFAULT']['db_backup_type']

    # Prepare default domain db and domain archive db objects
    if db_domain_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        db_domain = sqlite.DB(db_domain_type, db_path, db_domain_name, db_retry_limit)
        db_domain_arch = sqlite.DB(db_domain_type, db_path, db_domain_archive_name, db_retry_limit)
    elif db_domain_type == 'postgresql':
        cfg_db = config_dta['DB.postgres']
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db_domain = postgresql.DB(db_domain_type, db_domain_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
        db_domain_arch = postgresql.DB(db_domain_type, db_domain_archive_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
    else:
        log.critical(f'Error: Incorrect database type')
        return

    # Prepare default dictionary db and dictionary archive db objects
    if db_dict_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        db_dict = sqlite.DB(db_dict_type, db_path, db_dict_name, db_retry_limit)
        db_dict_arch = sqlite.DB(db_dict_type, db_path, db_dict_archive_name, db_retry_limit)
    elif db_dict_type == 'postgresql':
        cfg_db = config_dta['DB.postgres']
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db_dict = postgresql.DB(db_dict_type, db_dict_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
        db_dict_arch = postgresql.DB(db_dict_type, db_dict_archive_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
    else:
        log.critical(f'Error: Incorrect database type')
        return

    # Prepare default backup db objects for domain and dictionary
    if db_backup_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        db_backup_domain_name = f'{db_domain_name}_backup'
        db_domain_backup = sqlite.DB(db_backup_type, db_path, db_backup_domain_name, db_retry_limit)
        db_backup_dict_name = f'{db_dict_name}_backup'
        db_dict_backup = sqlite.DB(db_backup_type, db_path, db_backup_dict_name, db_retry_limit)
    elif db_backup_type == 'postgresql':
        cfg_db = config_dta['DB.postgres']
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db_backup_domain_name = f'{db_domain_name}_backup'
        db_domain_backup = postgresql.DB(db_backup_type, db_backup_domain_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
        db_backup_dict_name = f'{db_dict_name}_backup'
        db_dict_backup = postgresql.DB(db_backup_type, db_backup_dict_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
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
        # domains
        data_ops.archiver(db_domain, db_domain_arch, tbl_domain_names, tld_domain, arch_type)
        # dictionary
        data_ops.archiver(db_dict, db_dict_arch, tbl_dict_names, tld_dict, arch_type)

    elif user_option == '2':
        arch_type = 'restore'
        # domains
        data_ops.archiver(db_domain, db_domain_arch, tbl_domain_names, tld_domain, arch_type)
        # dictionary
        data_ops.archiver(db_dict, db_dict_arch, tbl_dict_names, tld_dict, arch_type)

    elif user_option == '3':
        # domains
        data_ops.backup_data(db_domain, db_domain_arch, db_domain_backup, tbl_domain_names, tld_domain)
        # dictionary
        data_ops.backup_data(db_dict, db_dict_arch, db_dict_backup, tbl_dict_names, tld_dict)

    else:
        log.info('Incorrect option picked')
        return
