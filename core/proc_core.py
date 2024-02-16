# Package: BulkDNS
# Module: core/proc_core
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-02-16

import logging

from common import sqlite
from common import postgresql
from core import single_proc
from core import multi_proc
from core import multi_thread

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

    # Prepare default domain db object
    if db_domain_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        db_domain = sqlite.DB(db_domain_type, db_path, db_domain_name, db_retry_limit)
    elif db_domain_type == 'postgresql':
        cfg_db = config_dta['DB.postgres']
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db_domain = postgresql.DB(db_domain_type, db_domain_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
    else:
        log.critical(f'Error: Incorrect database type')
        return

    # Prepare default dictionary db object
    if db_dict_type == 'sqlite':
        cfg_db = config_dta['DB.sqlite']
        db_path = cfg_db['db_location']
        db_dict = sqlite.DB(db_dict_type, db_path, db_dict_name, db_retry_limit)
    elif db_dict_type == 'postgresql':
        cfg_db = config_dta['DB.postgres']
        db_host = cfg_db['db_host']
        db_port = cfg_db['db_port']
        user_name = cfg_db['user_name']
        user_password = cfg_db['user_password']
        db_dict = postgresql.DB(db_dict_type, db_dict_name, db_host, db_port, user_name, user_password, db_retry_limit, db_retry_sleep_time)
    else:
        log.critical(f'Error: Incorrect database type')
        return

    log.info('1 - New and expiring domains RDAP [single processing]')
    log.info('2 - New and expiring domains RDAP [multiprocessing]')
    log.info('3 - New and expiring domains WHOIS [multithreading]')
    log.info('4 - Available domains re-check RDAP [single processing]')
    log.info('5 - Available domains re-check RDAP [multiprocessing]')
    log.info('6 - Available domains re-check WHOIS [multithreading]')

    log.info('Choose option and press Enter: ')
    user_option = input()

    if user_option == '1':
        protocol = 'rdap'
        check_type = 'expiring'
        single_proc.single_process_run(db_domain, tbl_domain_names, tld_domain, check_type, protocol)

    elif user_option == '2':
        protocol = 'rdap'
        check_type = 'expiring'
        multi_proc.multiprocess_run(db_domain, tbl_domain_names, tld_domain, check_type, protocol)

    elif user_option == '3':
        protocol = 'whois'
        check_type = 'expiring'
        multi_thread.multithreading_run(db_domain, tbl_domain_names, tld_domain, check_type, protocol)
    
    elif user_option == '4':
        protocol = 'rdap'
        check_type = 'recheck'
        single_proc.single_process_run(db_domain, tbl_domain_names, tld_domain, check_type, protocol)

    elif user_option == '5':
        protocol = 'rdap'
        check_type = 'recheck'
        multi_proc.multiprocess_run(db_domain, tbl_domain_names, tld_domain, check_type, protocol)

    elif user_option == '6':
        protocol = 'whois'
        check_type = 'recheck'
        multi_thread.multithreading_run(db_domain, tbl_domain_names, tld_domain, check_type, protocol)

    else:
        log.info('Incorrect option picked')
        return
