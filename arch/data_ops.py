# Package: BulkDNS
# Module: arch/data_ops
# Author: Michal Selma <michal@selma.cc>
# Rev: 2023-12-23

import datetime
import logging

log = logging.getLogger('main')


def backup_data(db, db_arch, db_backup, tbl_names, tld):

    for tbl_name in tbl_names:
        table = f'{tbl_name}_{tld}'

        # If only source and backup provided then it is restore, otherwise it is backup
        if db_arch is None:
            log.info(f'Restore {table} from {db.db_type}:{db.db_name} into {db_backup.db_type}:{db_backup.db_name}')
        else:
            log.info(f'Backup of {table} from {db.db_type}:{db.db_name} & {db_arch.db_name} into {db_backup.db_type}:{db_backup.db_name}')

        log.debug(f'Phase 1: Getting source DB data (live): {db.db_type}/{db.db_name}/{table}')
        src_sql_query = f'SELECT domain, name, tld, avail, expiry, updated FROM {table}'
        call_id = f'{db.db_type} | {table} | SELECT'
        src_db_dta_live = db.execute_single(src_sql_query, call_id)

        if db_arch is None:
            log.info(f'Arch DB not set. Restore mode. Skipping archive DB...')
            src_db_dta = src_db_dta_live
        else:
            log.debug(f'Phase 2: Getting source DB data (arch): {db_arch.db_type}/{db_arch.db_name}/{table}')
            src_sql_query = f'SELECT domain, name, tld, avail, expiry, updated FROM {table}'
            call_id = f'{db_arch.db_type} | {table} | SELECT'
            src_db_dta_arch = db_arch.execute_single(src_sql_query, call_id)
            src_db_dta = src_db_dta_live + src_db_dta_arch

        src_dta_count = len(src_db_dta)
        log.info(f'Items to be migrated: {src_dta_count}')

        log.debug(f'Phase 3: Cleaning backup table: {db_backup.db_type}/{db_backup.db_name}/{table}')

        if db_backup.db_type == 'sqlite':
            call_id = f'{db_backup.db_type} | {table} | DELETE FROM'
            dst_sql_query = f'DELETE FROM {table}'
            db_backup.execute_single(dst_sql_query, call_id)
            call_id = f'{db_backup.db_type} | {table} | VACUUM'
            dst_sql_query = f'VACUUM'
            db_backup.execute_single(dst_sql_query, call_id)
        elif db_backup.db_type == 'postgresql':
            call_id = f'{db_backup.db_type} | {table} | TRUNCATE'
            dst_sql_query = f'TRUNCATE {table}'
            db_backup.execute_single(dst_sql_query, call_id)
        else:
            log.error(f'Error: Incorrect backup database type')
            return

        # Below is to validate or manipulate source data before update to destination - e.g. replace "" with None
        log.debug(f'Phase 4: Preparing {tbl_name} data...')
        dst_dta = []
        to_process_len = src_dta_count
        counter = 0
        for src_item in src_db_dta:
            counter = counter + 1
            print(f'{counter} of {to_process_len}', end="\r", flush=True)
            dst_item = []
            for index in range(len(src_item)):
                #  Make None if empty
                if src_item[index] == "":
                    dst_item.append(None)
                elif src_item[index] is str:
                    # If object is string remove any leading and trailing whitespaces by using strip()
                    stripped = src_item[index].strip()
                    #  Make None if empty
                    if stripped == "":
                        dst_item.append(None)
                    else:
                        dst_item.append(stripped)
                else:
                    dst_item.append(src_item[index])
            dst_dta.append(dst_item)

        log.debug(f'Phase 5: Data preparation finished. Executing insert into "{table}"')
        # Execute insert
        call_id = f'{db_backup.db_type} | {table} | INSERT'
        if db_backup.db_type == 'sqlite':
            param_query = f'INSERT INTO {table}(domain, name, tld, avail, expiry, updated) VALUES(?,?,?,?,?,?)'
            db_backup.execute_many_param(param_query, dst_dta, call_id)
        elif db_backup.db_type == 'postgresql':
            param_query = f'INSERT INTO {table}(domain, name, tld, avail, expiry, updated) VALUES(%s,%s,%s,%s,%s,%s)'
            db_backup.execute_many_param(param_query, dst_dta, call_id)
        else:
            log.error(f'Error: Incorrect backup database type')
            return

        log.info(f'Backup of {table} finished successfully')


def archiver(db, db_arch, tbl_names, tld, arch_type):

    exp_date = datetime.datetime.now() + datetime.timedelta(days=30)
    exp_date = datetime.datetime(exp_date.year, exp_date.month, exp_date.day, 0, 0, 0)

    if arch_type == 'archive':
        src_db = db
        dst_db = db_arch
    elif arch_type == 'restore':
        src_db = db_arch
        dst_db = db
    else:
        log.error(f'Incorrect archiver type: {arch_type}')
        return

    for tbl_name in tbl_names:
        table = f'{tbl_name}_{tld}'

        log.debug(f'Archiver step 1: Getting domains amount in: {src_db.db_type}/{src_db.db_name}/{table}')
        count_sql_query = f"SELECT count(*) FROM {table}"
        call_id = f'{src_db.db_type} | {table} | SELECT'
        count_result = src_db.execute_single(count_sql_query, call_id)

        log.debug(f'Archiver step 2: Getting domains data to be archived/restored: {src_db.db_type}/{src_db.db_name}/{table}')
        call_id = f'{src_db.db_type} | {table} | SELECT'
        if arch_type == 'archive':
            select_sql_query = f"SELECT domain, name, tld, avail, expiry, updated FROM {table} WHERE avail = 'N' AND expiry > '{exp_date}'"
        elif arch_type == 'restore':
            select_sql_query = f"SELECT domain, name, tld, avail, expiry, updated FROM {table} WHERE avail = 'N' AND expiry <= '{exp_date}'"
        else:
            log.error(f'Incorrect archiver type: {arch_type}')
            return
        src_db_dta = src_db.execute_single(select_sql_query, call_id)

        # Prepare and show some stats
        src_dta_count = len(src_db_dta)
        domains_amount = int(count_result[0][0])
        if domains_amount == 0:
            log.error(f'No data in source table: {src_db.db_type}/{src_db.db_name}/{table}')
            continue

        # Percent value:
        percent = (src_dta_count / domains_amount) * 100
        # Percent formatted value:
        percent = "{0:.2f}".format(percent)
        log.info(f'{table} | {arch_type} | {percent} % | {src_dta_count} of {domains_amount}')

        log.debug(f'Archiver step 3: Validating insert data and skipping duplicates')
        call_id = f'{src_db.db_type} | {table} | SELECT'
        select_sql_query = f"SELECT domain FROM {table}"
        res = dst_db.execute_single(select_sql_query, call_id)
        src_dta = []
        dst_db_dta = [row[0] for row in res]  # Change tuple of single-item tuples to tuple of single-items
        for src_itm in src_db_dta:
            if src_itm[0] in dst_db_dta:
                log.error(f'Domain already exist in destination table: {src_itm[0]}. Will be removed from source table.')
            else:
                src_dta.append(src_itm)

        log.debug(f'Archiver step 4: Executing data insert into {dst_db.db_type}/{dst_db.db_name}/{table}')
        call_id = f'{dst_db.db_type} | {table} | INSERT'
        if dst_db.db_type == 'sqlite':
            param_query = f'INSERT INTO {table}(domain, name, tld, avail, expiry, updated) VALUES(?,?,?,?,?,?)'
            dst_db.execute_many_param(param_query, src_dta, call_id)
        elif dst_db.db_type == 'postgresql':
            param_query = f'INSERT INTO {table}(domain, name, tld, avail, expiry, updated) VALUES(%s,%s,%s,%s,%s,%s)'
            dst_db.execute_many_param(param_query, src_dta, call_id)
        else:
            log.error(f'Error: Incorrect database type')
            return

        log.debug(f'Archiver step 5: Preparing data to delete from {src_db.db_type}/{src_db.db_name}/{table}')
        src_dta_del = []
        for item in src_db_dta:
            src_dta_del.append([item[0]])

        log.debug(f'Archiver step 6: Delete {len(src_dta_del)} domains from {src_db.db_type}/{src_db.db_name}/{table}')
        call_id = f'{src_db.db_type}/{src_db.db_name}/{table} | DELETE'
        if src_db.db_type == 'sqlite':
            param_query = f'DELETE FROM {table} WHERE domain=?'
            src_db.execute_many_param(param_query, src_dta_del, call_id)
        elif src_db.db_type == 'postgresql':
            param_query = f'DELETE FROM {table} WHERE domain=%s'
            src_db.execute_many_param(param_query, src_dta_del, call_id)
        else:
            log.error(f'Error: Incorrect database type')
            return
