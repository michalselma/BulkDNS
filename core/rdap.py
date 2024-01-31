# Package: BulkDNS
# Module: core/rdap
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-01-31

import subprocess
import json
import time
import datetime
import random
import logging


log = logging.getLogger('main')


def rdap(domain):
    # Running standard windows Command Line using powershell.exe (std cmd.exe had issues)
    cmd = 'rdap'
    cmd_line = '-r'
    try:
        command_call = subprocess.run([cmd, cmd_line, domain], capture_output=True, text=True, check=True, shell=True)
        # Get command output to variable
        log.debug(f'rdap executed properly: {cmd} {cmd_line} {domain}')
        # If stdout is empty return zero to manage information out of function
        cmd_code = command_call.returncode
        cmd_output = command_call.stdout
    except subprocess.CalledProcessError as err:
        log.debug(f'Command Line call: "{cmd} {cmd_line} {domain}" returned RuntimeError (code {err.returncode})')
        log.debug(f'Runtime Error output: {err.output}')
        log.debug(f'Command Line error (CalledProcessError): {err.stderr}')
        # Get command error to variable to return function output
        cmd_code = err.returncode
        cmd_output = err.stderr
        
    #print('Command Line output: ' + str(cmd_output))
    # If subprocess run() check=False, then subprocess run() will got return code 0.
    # In such case check_output() might fail with nonzero return code.
    # check_output() returns un-decoded data, where run() returns string.
    # cmd_output = subprocess.check_output([cmd, cmd_arg, ps_cmd], stderr=subprocess.STDOUT, shell=True)
    log.debug('Procedure finished: rdap()')
    return cmd_code, cmd_output


def rdap_run(domain):
    exec_code = None
    available = None
    expiry_date = None
    err_msg = None

    res = rdap(domain)

    if res[0] == 0: # Executed correctly
        json_data = json.loads(res[1])
        if json_data["events"][1]['eventAction'] == 'expiration':
            exec_code = 0
            available = 'N'
            expiry_date = json_data["events"][1]['eventDate']
            err_msg = None
        else:
            print(f'Data decoding error: {res[1]}')
            exec_code = -1
            available = None
            expiry_date = None
            err_msg = 'Data decoding error'

    elif res[0] == 1:  # Error
        if 'RDAP server returned 404, object does not exist' in res[1]:
            exec_code = 0
            available = 'Y'
            expiry_date = None
            err_msg = res[1]
        else:
            log.debug(f'Unknown Error: {res[1]}')
            exec_code = -1
            available = None
            expiry_date = None
            err_msg = res[1]

    return exec_code, available, expiry_date, err_msg


def query(name, tld, retry):
    available = None
    expiry_date = None
    updated = None
    exec_code = None
    err_msg = None
    retry_count = 0

    domain = f'{name}.{tld}'

    if retry < 0:
        retry = 0

    while retry_count <= retry:

        res = rdap_run(domain)

        available = res[1]
        expiry_date = res[2]
        exec_code = res[0]
        err_msg = res[3]

        if exec_code == 0:
            dt = datetime.datetime.now(datetime.UTC)
            updated = dt.strftime("%Y-%m-%d %H:%M:%S")
            break
        else:
            time.sleep(random.random())  # random floating point number in the range 0.0 <= X < 1.0
            retry_count += 1
            updated = None

    log.debug(f'{domain} | Returning domain data: {domain, name, tld, available, expiry_date, updated, exec_code, err_msg, retry_count}')
    return domain, name, tld, available, expiry_date, updated, exec_code, err_msg, retry_count
