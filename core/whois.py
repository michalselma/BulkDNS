# Package: BulkDNS
# Module: core/domain
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-01-31

import whoisdomain
import logging
import datetime
import random
import time

log = logging.getLogger('main')


def get_domain_raw_data(name, tld):
    domain = f'{name}.{tld}'
    try:
        res = whoisdomain.query(domain)
        if res is None:
            print(f'Domain information not found for: {domain}')
        else:
            print(res)
    except whoisdomain.exceptions.WhoisPrivateRegistry as e:
        print(f'{domain} | Exception WhoisPrivateRegistry: {e}')
    except whoisdomain.exceptions.WhoisCommandFailed as e:
        print(f'{domain} | Exception WhoisCommandFailed: {e}')
    except whoisdomain.exceptions.WhoisQuotaExceeded as e:
        print(f'{domain} | Exception WhoisQuotaExceeded: {e}')
    except whoisdomain.exceptions.FailedParsingWhoisOutput as e:
        print(f'{domain} | Exception FailedParsingWhoisOutput: {e}')
    except whoisdomain.exceptions.UnknownTld as e:
        print(f'{domain} | Exception UnknownTld: {e}')
    except whoisdomain.exceptions.UnknownDateFormat as e:
        print(f'{domain} | Exception UnknownDateFormat: {e}')
    except whoisdomain.exceptions.WhoisCommandTimeout as e:
        print(f'{domain} | Exception WhoisCommandTimeout: {e}')

def get_domain_data(name, tld, retry):
    domain = f'{name}.{tld}'

    exec_count = 0
    retry_count = 0
    available = None
    expiry_date = None
    updated = None
    exec_code = -1  # Assume error as default
    err_msg = None

    while exec_count <= retry_count < retry:
        try:
            log.debug(f'{domain} | Checking domain')
            res = whoisdomain.query(domain=domain, cmd='whois', slow_down=0, cache_age=0)
            if res is None:
                log.debug(f'{domain} | Domain information NOT FOUND')
                available = 'Y'
                expiry_date = None
                dt = datetime.datetime.now(datetime.UTC)
                updated = dt.strftime("%Y-%m-%d %H:%M:%S")
                exec_code = 0
                err_msg = None
            else:
                log.debug(f'{domain} | Domain information FOUND ')
                available = 'N'
                expiry_date = str(res.expiration_date)
                dt = datetime.datetime.now(datetime.UTC)
                updated = dt.strftime("%Y-%m-%d %H:%M:%S")
                exec_code = 0
                err_msg = None
            exec_count += 1
        except whoisdomain.WhoisPrivateRegistry as err:
            err_msg = 'WhoisPrivateRegistry'
            retry_count += 1
            exec_count += 1
            log.debug(f'{domain} | Exception WhoisPrivateRegistry | {err} | Retrying {retry_count}')
            time.sleep(random.random())  # random floating point number in the range 0.0 <= X < 1.0
            continue

        except whoisdomain.WhoisCommandFailed as err:
            err_msg = 'WhoisCommandFailed'
            retry_count += 1
            exec_count += 1
            log.debug(f'{domain} | Exception WhoisCommandFailed | {err} | Retrying {retry_count}')
            time.sleep(random.random())  # random floating point number in the range 0.0 <= X < 1.0
            continue

        except whoisdomain.WhoisQuotaExceeded as err:
            err_msg = 'WhoisQuotaExceeded'
            retry_count += 1
            exec_count += 1
            log.debug(f'{domain} | Exception WhoisQuotaExceeded | {err} | Retrying {retry_count}')
            time.sleep(random.random())  # random floating point number in the range 0.0 <= X < 1.0
            continue

        except whoisdomain.FailedParsingWhoisOutput as err:
            err_msg = 'FailedParsingWhoisOutput'
            retry_count += 1
            exec_count += 1
            log.debug(f'{domain} | Exception FailedParsingWhoisOutput | {err} | Retrying {retry_count}')
            time.sleep(random.random())  # random floating point number in the range 0.0 <= X < 1.0
            continue

        except whoisdomain.UnknownTld as err:
            err_msg = 'UnknownTld'
            retry_count += 1
            exec_count += 1
            log.debug(f'{domain} | Exception UnknownTld | {err} | Retrying {retry_count}')
            time.sleep(random.random())  # random floating point number in the range 0.0 <= X < 1.0
            continue

        except whoisdomain.UnknownDateFormat as err:
            err_msg = 'UnknownDateFormat'
            retry_count += 1
            exec_count += 1
            log.debug(f'{domain} | Exception UnknownDateFormat | {err} | Retrying {retry_count}')
            time.sleep(random.random())  # random floating point number in the range 0.0 <= X < 1.0
            continue

        except whoisdomain.WhoisCommandTimeout as err:
            err_msg = 'WhoisCommandTimeout'
            retry_count += 1
            exec_count += 1
            log.debug(f'{domain} | Exception WhoisCommandTimeout | {err} | Retrying {retry_count}')
            time.sleep(random.random())  # random floating point number in the range 0.0 <= X < 1.0
            continue

    if exec_count == retry_count > 0:
        log.debug(f'{domain} | Exception {err_msg} | Retried: {retry_count} | No more retries...')
    else:
        log.debug(f'{domain} | Domain information successfully gained | Retry count: {retry_count}')

    log.debug(f'{domain} | Returning domain data: {domain, name, tld, available, expiry_date, updated, exec_code, err_msg, retry_count}')
    return domain, name, tld, available, expiry_date, updated, exec_code, err_msg, retry_count
