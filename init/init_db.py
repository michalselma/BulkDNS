# Package: BulkDNS
# Module: init/init_db
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-02-22

import logging

log = logging.getLogger('main')


def create_db(db, new_db_name):
    call_id = f'{new_db_name} | CREATE DB'
    if db.db_type == 'sqlite':
        # When connecting to non-existing db in sqlite it will be created automatically, so what create_new_db() do
        # for sqlite is just to establish connection to new_db_name. We replace db.dbname param for DB connection object
        db.db_name = new_db_name
        db.create_new_db(call_id)
    elif db.db_type == 'postgresql':
        # For postgresql connect to system table (defined in db object) and call create_new_db with new_db_name param.
        db.create_new_db(new_db_name, call_id)
    else:
        log.critical(f'Error: Incorrect database type. (Should not see me!)')


def create_tbl(db, tbl_names, tld):
    for tbl_name in tbl_names:
        table = f'{tbl_name}_{tld}'
        query = (f'CREATE TABLE {table} (domain VARCHAR(300) PRIMARY KEY NOT NULL, name VARCHAR(255), '
                 f'tld VARCHAR(40), avail CHARACTER(1), expiry TIMESTAMP, updated TIMESTAMP)')
        call_id = f'{table} | CREATE TABLE'
        db.execute_single(query, call_id)


def create_dict_tbl(db, tbl_names):
    for tbl_name in tbl_names:
        query = (f'CREATE TABLE {tbl_name} (domain_name VARCHAR(100) PRIMARY KEY NOT NULL, dictionary_term VARCHAR(100), '
                 f'term_type VARCHAR(50), category VARCHAR(50), comb_use CHARACTER(1))')
        call_id = f'{tbl_name} | CREATE TABLE'
        db.execute_single(query, call_id)


def initialize(db, new_db_name, tbl_names, tld):
    log.info(f'Creating database: {new_db_name}')
    create_db(db, new_db_name)
    db.db_name = new_db_name  # When new db has been created, for tables initialization modify db object to use new db
    log.info(f'Creating tables structure in {new_db_name}...')
    create_tbl(db, tbl_names, tld)


def initialize_dict(db, tbl_names):
    log.info(f'Creating tables structure in {db.db_name}...')
    create_dict_tbl(db, tbl_names)
