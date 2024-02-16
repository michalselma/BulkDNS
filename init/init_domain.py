# Package: BulkDNS
# Module: init/init_dict
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-02-16

import logging
import itertools
import time

log = logging.getLogger('main')


def gen_two_digit():
    log.debug(f'Execute: gen_two_digit()')
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    # itertools.product is ~10-30% faster
    # ret = [(a+b) for a in digits for b in digits]
    ret = itertools.product(digits, repeat=2)
    ret = [''.join(item) for item in ret]
    return ret


def gen_two_letter():
    log.debug(f'Execute: gen_two_letter()')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    # itertools.product is ~10-30% faster
    # ret = [(a+b) for a in letters for b in letters]
    ret = itertools.product(letters, repeat=2)
    ret = [''.join(item) for item in ret]
    return ret


def gen_two_digit_letter():
    log.debug('Execute: gen_two_digit_letter()')
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    ret = itertools.chain(itertools.product(digits, letters), itertools.product(letters, digits))
    ret = [''.join(item) for item in ret]
    return ret


# two_special will be always empty as "-" is not allowed as domain prefix or suffix and there are no more special chars

def gen_three_digit():
    log.debug(f'Execute: gen_three_digit()')
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    # itertools.product is ~10-30% faster
    # ret = [(a + b + c) for a in digits for b in digits for c in digits]
    ret = itertools.product(digits, repeat=3)
    ret = [''.join(item) for item in ret]
    return ret


def gen_three_letter():
    log.debug(f'Execute: gen_three_letter()')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    # itertools.product is ~10-30% faster
    # ret = [(a + b + c) for a in letters for b in letters for c in letters]
    ret = itertools.product(letters, repeat=3)
    ret = [''.join(item) for item in ret]
    return ret


def gen_three_digit_letter():
    log.debug('Execute: gen_three_digit_letter()')
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    all_comb = digits + letters
    all_combinations = (item for item in itertools.product(all_comb, repeat=3))
    ret = []
    for element in all_combinations:
        # skip if element items are only from 'letters' group (letters with letters)
        if not all(item in letters for item in element):
            # skip if element items are only from 'digits' group (digits with digits)
            if not all(item in digits for item in element):
                ret.append(''.join(element))
    return ret


def gen_three_special():
    log.debug('Execute: gen_three_special()')
    specials = ('-',)
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    all_comb = specials + digits + letters
    exclude = digits + letters
    all_combinations = (item for item in itertools.product(all_comb, repeat=3))
    exclude = (item for item in itertools.product(exclude, repeat=3))
    # exclude digits, letters and digits-letters combinations
    prerequisite_list = set(all_combinations) - set(exclude)
    ret = []
    for element in prerequisite_list:
        # name should not start or end with any special char
        if element[0] not in specials:
            if element[-1] not in specials:
                ret.append(''.join(element))
    return ret


def gen_four_digit():
    log.debug(f'Execute: gen_four_digit()')
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    # itertools.product is ~10-30% faster
    # ret = [(a + b + c + d) for a in digits for b in digits for c in digits for d in digits]
    ret = itertools.product(digits, repeat=4)
    ret = [''.join(item) for item in ret]
    return ret


def gen_four_letter():
    log.debug(f'Execute: gen_four_letter()')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    # itertools.product is ~10-30% faster
    # ret = [(a + b + c + d) for a in letters for b in letters for c in letters for d in letters]
    ret = itertools.product(letters, repeat=4)
    ret = [''.join(item) for item in ret]
    return ret


def gen_four_digit_letter():
    log.debug('Execute: gen_four_digit_letter()')
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    all_comb = digits + letters
    all_combinations = (item for item in itertools.product(all_comb, repeat=4))
    ret = []
    for element in all_combinations:
        # skip if element items are only from 'letters' group (letters with letters)
        if not all(item in letters for item in element):
            # skip if element items are only from 'digits' group (digits with digits)
            if not all(item in digits for item in element):
                ret.append(''.join(element))
    return ret


def gen_four_special():
    log.debug('Execute: gen_four_special()')
    specials = ('-',)
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    all_comb = specials + digits + letters
    exclude = digits + letters
    all_combinations = (item for item in itertools.product(all_comb, repeat=4))
    exclude = (item for item in itertools.product(exclude, repeat=4))
    # exclude digits, letters and digits-letters combinations
    prerequisite_list = set(all_combinations) - set(exclude)
    ret = []
    for element in prerequisite_list:
        # name should not start or end with any special char
        if element[0] not in specials:
            if element[-1] not in specials:
                ret.append(''.join(element))
    return ret


def gen_five_digit():
    log.debug(f'Execute: gen_five_digit()')
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    # itertools.product is ~10-30% faster
    # ret = [(a + b + c + d + e) for a in digits for b in digits for c in digits for d in digits for e in digits]
    ret = itertools.product(digits, repeat=5)
    ret = [''.join(item) for item in ret]
    return ret


def gen_five_letter():
    log.debug(f'Execute: gen_five_letter()')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    # itertools.product is ~10-30% faster
    # ret = [(a + b + c + d + e) for a in letters for b in letters for c in letters for d in letters for e in letters]
    ret = itertools.product(letters, repeat=5)
    ret = [''.join(item) for item in ret]
    return ret


def gen_five_digit_letter():
    log.debug('Execute: gen_five_digit_letter()')
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    all_comb = digits + letters
    all_combinations = (item for item in itertools.product(all_comb, repeat=5))
    ret = []
    for element in all_combinations:
        # skip if element items are only from 'letters' group (letters with letters)
        if not all(item in letters for item in element):
            # skip if element items are only from 'digits' group (digits with digits)
            if not all(item in digits for item in element):
                ret.append(''.join(element))
    return ret


def gen_five_special():
    log.debug('Execute: gen_five_special()')
    specials = ('-',)
    digits = ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
    letters = ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
    all_comb = specials + digits + letters
    exclude = digits + letters
    # generate and skip if each of generated element chars are only from 'digits-letters' group (no special in at least one place of element)
    all_combinations = (item for item in itertools.product(all_comb, repeat=5) if
                        not all(char_itm in exclude for char_itm in item))
    ret = []
    for element in all_combinations:
        # skip if element items starts or ends with special
        if element[0] not in specials:
            if element[-1] not in specials:
                ret.append(''.join(element))
    return ret


def create_domain_dta(db, tbl_names, tld):
    timer_start = time.perf_counter()
    names = None
    # for each table name from tables array defined above
    for tbl_name in tbl_names:
        log.info(f'Processing {tbl_name}')
        # Generate combinations(names) based on exact table name it should belong to.
        if tbl_name == 'two_digit':
            names = gen_two_digit()
        if tbl_name == 'two_letter':
            names = gen_two_letter()
        if tbl_name == 'two_digit_letter':
            names = gen_two_digit_letter()
        if tbl_name == 'three_digit':
            names = gen_three_digit()
        if tbl_name == 'three_letter':
            names = gen_three_letter()
        if tbl_name == 'three_digit_letter':
            names = gen_three_digit_letter()
        if tbl_name == 'three_special':
            names = gen_three_special()
        if tbl_name == 'four_digit':
            names = gen_four_digit()
        if tbl_name == 'four_letter':
            names = gen_four_letter()
        if tbl_name == 'four_digit_letter':
            names = gen_four_digit_letter()
        if tbl_name == 'four_special':
            names = gen_four_special()
        if tbl_name == 'five_digit':
            names = gen_five_digit()
        if tbl_name == 'five_letter':
            names = gen_five_letter()
        if tbl_name == 'five_digit_letter':
            names = gen_five_digit_letter()
        if tbl_name == 'five_special':
            names = gen_five_special()

        # If for any reason, generated names are None or empty, skip to next item
        if not names:
            log.warning(f'No combinations generated. Skipping...')
            continue

        # Collect names that already exist in db
        table = f'{tbl_name}_{tld}'
        log.debug(f'Checking DB items in {table}')
        query = f'SELECT name FROM {table}'
        call_id = f'{table} | SELECT'
        output = db.execute_single(query, call_id)

        # Compare if all items that exist in db equals all generated items
        if len(output) == len(names):
            log.info(f'All generated names for {tbl_name} already exist in {table} table')
            continue  # Skip further processing for this table
        else:
            # Prepare data for mass insert
            sql_params_array = []
            log.info(f'Preparing {tbl_name} data...')
            to_process_len = len(names)
            counter = 0
            result_set = set(output)  # Convert list to a set as search in list is much slower than search in set
            for name in names:
                counter = counter + 1

                # If last item print with /n
                if counter == to_process_len:
                    log.info(f'{counter} of {to_process_len}')
                else:
                    # It is not crucial that complete print count is displayed each time (on each counter),
                    # so we can flush print buffer immediately and gain some free resources or processing time
                    print(f'{counter} of {to_process_len}', end="\r", flush=True)

                # Check if domain already exists in table.
                if name in result_set:
                    # print(f'Domain {domain} already exists in {table} .')
                    continue  # If yes, skip further processing
                # If not, add to params list, to be used for SQL executemany params query
                else:
                    # print(f'Preparing [{domain}, {name}, {tld}, None, None, None] to be added into {table} .')
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

    timer_stop = time.perf_counter()
    log.debug(f'Execution time [seconds]: {timer_stop - timer_start}')
