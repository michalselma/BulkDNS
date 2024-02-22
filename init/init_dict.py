# Package: BulkDNS
# Module: init/init_dict
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-02-22

import logging
import itertools

log = logging.getLogger('main')


def upload_dta(db, tbl_name, source):
    log.info(f'Opening file {source}')
    try:
        with open(source, 'r') as file:
            file_data = file.read().splitlines()
            file.close()
    except Exception as ex:
        log.error(f'Exception: {ex}')
        return

    log.info(f'Uploading data into {tbl_name}')
    param_array = []
    count = 0

    # Get data already in the dictionary tables
    log.debug(f'Querying destination table: {tbl_name}')
    query = f'SELECT domain_name FROM {tbl_name}'
    call_id = f'{tbl_name} | SELECT'
    res = db.execute_single(query, call_id)
    db_data = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items

    for line in file_data:
        # split each line comma separated values and store as list
        line_items = line.split(",")
        count += 1
        # print(line_items)  #
        if len(line_items) != 5:
            log.error(f'Data validation failed. Something wrong with content of the file at line {count} : {line}')
            return
        if line_items[0] in db_data:
            log.warning(f'Data already exists. Skipping input line {count} : {line}')
            continue
        else:
            param_array.append(line_items)

    # Execute insert
    call_id = f'{db.db_type} | {tbl_name} | INSERT'
    if db.db_type == 'sqlite':
        param_query = f'INSERT INTO {tbl_name}(domain_name, dictionary_term, term_type, category, comb_use) VALUES(?,?,?,?,?)'
        db.execute_many_param(param_query, param_array, call_id)
    elif db.db_type == 'postgresql':
        param_query = f'INSERT INTO {tbl_name}(domain_name, dictionary_term, term_type, category, comb_use) VALUES(%s,%s,%s,%s,%s)'
        db.execute_many_param(param_query, param_array, call_id)
    else:
        log.error(f'Incorrect database type')
        return


def create_dict_domains(db, db_arch, tbl_names, tld):
    for tbl_name in tbl_names:
        log.debug(f'Querying source (dictionary) table: {tbl_name}')
        query = f'SELECT domain_name FROM {tbl_name}'
        call_id = f'{tbl_name} | SELECT'
        res = db.execute_single(query, call_id)
        db_data_source = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
        log.debug(f'Source DB data: {db_data_source}')

        table = f'{tbl_name[5:]}_{tld}'

        # Get data already in the dictionary domain tables (destination)
        log.debug(f'Querying destination (dictionary domain) table: {db.db_name} | {table}')
        query = f'SELECT name FROM {table}'
        call_id = f'{table} | SELECT'
        res = db.execute_single(query, call_id)
        db_data_dest = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
        log.debug(f'Destination DB data: {db_data_dest}')

        log.debug(f'Querying destination archive (dictionary domain archive) table: {db_arch.db_name} | {table}')
        query = f'SELECT name FROM {table}'
        call_id = f'{table} | SELECT'
        res = db_arch.execute_single(query, call_id)
        db_data_dest_arch = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
        log.debug(f'Destination DB data: {db_data_dest_arch}')

        sql_params_array = []
        for name in db_data_source:
            if name in db_data_dest:
                log.warning(f'Data already exists. Skipping source item: {name}')
                continue
            elif name in db_data_dest_arch:
                log.debug(f'Data already exists in archive {table}. Skipping source item: {name}')
                continue
            else:
                domain = f'{name}.{tld}'
                sql_params_array.append([domain, name, tld, None, None, None])

        # When params array prepared for exact table, execute
        log.debug(f'Data preparation finished. Executing insert into {table}')

        call_id = f'{table} | INSERT'
        if db.db_type == 'sqlite':
            sql_param_query = f'INSERT INTO {table} VALUES(?,?,?,?,?,?)'
            db.execute_many_param(sql_param_query, sql_params_array, call_id)
        elif db.db_type == 'postgresql':
            sql_param_query = f'INSERT INTO {table} VALUES(%s,%s,%s,%s,%s,%s)'
            db.execute_many_param(sql_param_query, sql_params_array, call_id)
        else:
            log.critical(f'Error: Incorrect database type. (Should not see me!)')
            return


def create_comb_domains(db, db_arch, tbl_names, tld):
    for tbl_name in tbl_names:
        # Get dictionary items
        log.info(f'Querying source (dictionary) table: {tbl_name}')
        query = f"SELECT domain_name FROM {tbl_name} WHERE comb_use = 'Y'"
        call_id = f'{tbl_name} | SELECT'
        res = db.execute_single(query, call_id)
        db_data_source = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
        log.debug(f'Source DB data: {db_data_source}')

        # Generate combinations
        log.info(f'Generating combinations based on {tbl_name}')
        # itertools.product is ~10-30% faster
        # ret = [(a+b) for a in db_data_source for b in db_data_source]
        ret = itertools.product(db_data_source, repeat=2)
        combinations = [''.join(item) for item in ret]
        combinations = list(set(combinations))  # Remove duplicates
        log.debug(f'Generated combinations: {combinations}')

        table = f'{tbl_name[5:]}_comb_{tld}'

        # Get data already in the dictionary domain tables (destination)
        log.info(f'Querying destination (dictionary domain) table: {table}')
        query = f'SELECT name FROM {table}'
        call_id = f'{table} | SELECT'
        res = db.execute_single(query, call_id)
        db_data_dest = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
        log.debug(f'Destination DB data: {db_data_dest}')

        log.info(f'Querying destination archive (dictionary domain archive) table: {table}')
        query = f'SELECT name FROM {table}'
        call_id = f'{table} | SELECT'
        res = db_arch.execute_single(query, call_id)
        db_data_dest_arch = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
        log.debug(f'Destination DB data: {db_data_dest_arch}')

        sql_params_array = []
        for name in combinations:
            if name in db_data_dest:
                log.debug(f'Data already exists in {table}. Skipping source item: {name}')
                continue
            elif name in db_data_dest_arch:
                log.debug(f'Data already exists in archive {table}. Skipping source item: {name}')
                continue
            else:
                domain = f'{name}.{tld}'
                sql_params_array.append([domain, name, tld, None, None, None])

        # When params array prepared for exact table, execute
        log.info(f'Data preparation finished. Executing insert into {table}')

        call_id = f'{table} | INSERT'
        if db.db_type == 'sqlite':
            sql_param_query = f'INSERT INTO {table} VALUES(?,?,?,?,?,?)'
            db.execute_many_param(sql_param_query, sql_params_array, call_id)
        elif db.db_type == 'postgresql':
            sql_param_query = f'INSERT INTO {table} VALUES(%s,%s,%s,%s,%s,%s)'
            db.execute_many_param(sql_param_query, sql_params_array, call_id)
        else:
            log.critical(f'Error: Incorrect database type. (Should not see me!)')
            return


def create_comb_domains_two_tables(db, db_arch, tbl_name_dict, tbl_generic_dict, tld):
    # Get table1 dictionary items
    log.info(f'Querying first source (dictionary) table: {tbl_name_dict}')
    query = f"SELECT domain_name FROM {tbl_name_dict} WHERE comb_use = 'Y'"
    call_id = f'{tbl_name_dict} | SELECT'
    res = db.execute_single(query, call_id)
    db_data_source_1 = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
    log.debug(f'Source DB data: {db_data_source_1}')

    # Get table2 dictionary items
    log.info(f'Querying second source (dictionary) table: {tbl_generic_dict}')
    query = f"SELECT domain_name FROM {tbl_generic_dict} WHERE comb_use = 'Y'"
    call_id = f'{tbl_generic_dict} | SELECT'
    res = db.execute_single(query, call_id)
    db_data_source_2 = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
    log.debug(f'Source DB data: {db_data_source_2}')

    # Generate combinations
    log.info(f'Generating combinations based on {tbl_name_dict} & {tbl_generic_dict}')
    # itertools.product is ~10-30% faster
    # ret = [(a+b) for a in db_data_source for b in db_data_source]
    ret = itertools.chain(itertools.product(db_data_source_1, db_data_source_2), itertools.product(db_data_source_2, db_data_source_1))
    combinations = [''.join(item) for item in ret]
    combinations = list(set(combinations))  # Remove duplicates
    log.debug(f'Generated combinations: {combinations}')

    table = f'{tbl_name_dict[5:]}_comb_{tld}'

    # Get data already in the dictionary domain tables (destination)
    log.info(f'Querying destination (dictionary domain) table: {table}')
    query = f'SELECT name FROM {table}'
    call_id = f'{table} | SELECT'
    res = db.execute_single(query, call_id)
    db_data_dest = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
    log.debug(f'Destination DB data: {db_data_dest}')

    log.info(f'Querying destination archive (dictionary domain archive) table: {table}')
    query = f'SELECT name FROM {table}'
    call_id = f'{table} | SELECT'
    res = db_arch.execute_single(query, call_id)
    db_data_dest_arch = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
    log.debug(f'Destination DB data: {db_data_dest_arch}')

    sql_params_array = []
    for name in combinations:
        if name in db_data_dest:
            log.debug(f'Data already exists in {table}. Skipping source item: {name}')
            continue
        elif name in db_data_dest_arch:
            log.debug(f'Data already exists in archive {table}. Skipping source item: {name}')
            continue
        else:
            domain = f'{name}.{tld}'
            sql_params_array.append([domain, name, tld, None, None, None])

    # When params array prepared for exact table, execute
    log.info(f'Data preparation finished. Executing insert into {table}')

    call_id = f'{table} | INSERT'
    if db.db_type == 'sqlite':
        sql_param_query = f'INSERT INTO {table} VALUES(?,?,?,?,?,?)'
        db.execute_many_param(sql_param_query, sql_params_array, call_id)
    elif db.db_type == 'postgresql':
        sql_param_query = f'INSERT INTO {table} VALUES(%s,%s,%s,%s,%s,%s)'
        db.execute_many_param(sql_param_query, sql_params_array, call_id)
    else:
        log.critical(f'Error: Incorrect database type. (Should not see me!)')
        return


def upload_dict(db,tbl_names):
    for tbl_name in tbl_names:
        source = f'db/{tbl_name}.txt'
        log.info(f'Processing {source} into {tbl_name}')
        upload_dta(db, tbl_name, source)
