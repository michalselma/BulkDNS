# Package: BulkDNS
# Module: init/init_db
# Author: Michal Selma <michal@selma.cc>
# Rev: 2023-11-25

from common import logger

log = logger.log_run()


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


def create_domain_tbl(db, tbl_names, tld):
    for tbl_name in tbl_names:
        table = f'{tbl_name}_{tld}'
        query = (f'CREATE TABLE {table} (domain VARCHAR(300) PRIMARY KEY NOT NULL, name VARCHAR(255), '
                 f'tld VARCHAR(40), avail CHARACTER(1), expiry TIMESTAMP, updated TIMESTAMP)')
        call_id = f'{table} | CREATE TABLE'
        db.execute_single(query, call_id)


def initialize(db, tbl_names, tld):
    # Main operations database for char combinations, taken domains to be moved then to '_taken' db in separate process
    new_db_name = 'domain'
    log.info(f'Creating database: {new_db_name}')
    create_db(db, new_db_name)
    db.db_name = new_db_name  # When new db has been created, for tables initialization modify db object to use new db
    log.info(f'Creating tables structure...')
    create_domain_tbl(db, tbl_names, tld)

    # Below is to store domains that are taken. Archive and bring back if needed based on domain status change to
    # be managed by archiver module.
    new_db_name = 'domain_taken'
    log.info(f'Creating database: {new_db_name}')
    create_db(db, new_db_name)
    db.db_name = new_db_name  # When new db is created, for tables initialization modify db object to use new db
    log.info(f'Creating tables structure...')
    create_domain_tbl(db, tbl_names, tld)

    log.info(f'Finished')
