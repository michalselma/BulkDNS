# Package: common
# Module: postgresql
# Author: Michal Selma <michal@selma.cc>
# Rev: 2024-01-23

# TO DO con.autocommit = True - to check efficiency as in sqlite it almost kills processing
# Probably should be .autocommit = False

# If the try statement reaches a break, continue or return statement, the "finally" clause will execute
# just prior to the break, continue or return statement’s execution.
# If a "finally" clause includes a return statement, the returned value will be the one from the "finally" clause’s
# return statement, not the value from the try/except clause’s return statement.

# *** Psycopg 3 Connection can be used as a context manager:
# with psycopg.connect() as conn:
#     ... # use the connection
#
# # the connection is now closed
# *** which is equivalent of:
# conn = psycopg.connect()
# try:
#     ... # use the connection
# except BaseException:
#     conn.rollback()
# else:
#     conn.commit()
# finally:
#     conn.close()
# *** You can use:
#
# with conn.cursor() as cur:
#     ...
#
# *** to close the cursor automatically when the block is exited.

import psycopg
import time
import logging

log = logging.getLogger('main')


class DB:
    def __init__(self, db_type, db_name, db_host, db_port, db_user, db_password, db_retry, db_retry_sleep_time):
        self.db_type = db_type
        self.db_name = db_name
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_retry = db_retry
        self.db_retry_sleep_time = db_retry_sleep_time

    def create_new_db(self, db_name, call_id):
        try:
            with psycopg.connect(user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port,
                                 dbname=self.db_name) as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    query = f'CREATE database {db_name}'
                    cur.execute(query)
            log.debug(f'{call_id} | Database "{db_name}" created successfully')
        except psycopg.Error as err:
            log.error(f'{call_id} | DB error: {err}')

    # TO DO - avid SQL Injection it is suggested to use execute_single_param (not full query as it is in execute_single)
    def execute_single(self, query, call_id):
        exec_count = 0
        retry_count = 0
        res = []
        error = None
        while exec_count <= retry_count < self.db_retry:
            try:
                with psycopg.connect(user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port, dbname=self.db_name) as conn:
                    with conn.cursor() as cur:
                        cur.execute(query)
                        # Validate result by checking existence of cursor description,
                        # otherwise there might be error exception in parent functions.
                        res = cur.fetchall() if cur.description else []
                exec_count += 1
                error = None

            except psycopg.ProgrammingError as err:
                exec_count += 1
                log.debug(f'{call_id} | DB error: {err} | No retries')
                error = err
                continue

            except psycopg.IntegrityError as err:
                exec_count += 1
                log.debug(f'{call_id} | DB error: {err} | No retries')
                error = err
                continue

            except psycopg.Error as err:
                retry_count += 1
                exec_count += 1
                log.debug(f'{call_id} | DB Error: {err} | Retrying {retry_count}')
                time.sleep(self.db_retry_sleep_time)
                error = err
                continue

        if exec_count == retry_count > 0:
            log.error(f'{call_id} | DB Error: {error} | Retried: {retry_count} | No more retries...')
        else:
            if error is None:
                log.debug(f'{call_id} | DB execute successful | Retry count: {retry_count}')
            else:
                log.error(f'{call_id} | DB Error: {error} | Retried: {retry_count} | No more retries...')

        log.debug(f'{call_id} | Returning DB data: {res}')
        return res

    def execute_many_param(self, param_query, params_array, call_id):
        exec_count = 0
        retry_count = 0
        res = []
        error = None
        while exec_count <= retry_count < self.db_retry:
            try:
                with psycopg.connect(user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port,
                                     dbname=self.db_name) as conn:
                    with conn.cursor() as cur:
                        cur.executemany(param_query, params_array)
                        # Validate result by checking existence of cursor description,
                        # otherwise there might be error exception in parent functions.
                        res = cur.fetchall() if cur.description else []
                exec_count += 1
                error = None

            except psycopg.ProgrammingError as err:
                exec_count += 1
                log.debug(f'{call_id} | DB error: {err} | No retries')
                error = err
                continue

            except psycopg.IntegrityError as err:
                exec_count += 1
                log.debug(f'{call_id} | DB error: {err} | No retries')
                error = err
                continue

            except psycopg.Error as err:
                retry_count += 1
                exec_count += 1
                log.debug(f'{call_id} | DB error: {err} | Retrying {retry_count}')
                time.sleep(self.db_retry_sleep_time)
                error = err
                continue

        if exec_count == retry_count > 0:
            log.error(f'{call_id} | DB Error: {error} | Retried: {retry_count} | No more retries...')
        else:
            if error is None:
                log.debug(f'{call_id} | DB execute successful | Retry count: {retry_count}')
            else:
                log.error(f'{call_id} | DB Error: {error} | Retried: {retry_count} | No more retries...')

        log.debug(f'{call_id} | Returning DB data: {res}')
        return res

    def execute_single_param(self, param_query, params, call_id):
        exec_count = 0
        retry_count = 0
        res = []
        error = None
        while exec_count <= retry_count < self.db_retry:
            try:
                with psycopg.connect(user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port,
                                     dbname=self.db_name) as conn:
                    with conn.cursor() as cur:
                        cur.execute(param_query, params)
                        # Validate result by checking existence of cursor description,
                        # otherwise there might be error exception in parent functions.
                        res = cur.fetchall() if cur.description else []
                exec_count += 1
                error = None

            except psycopg.ProgrammingError as err:
                exec_count += 1
                log.debug(f'{call_id} | DB error: {err} | No retries')
                error = err
                continue

            except psycopg.IntegrityError as err:
                exec_count += 1
                log.debug(f'{call_id} | DB error: {err} | No retries')
                error = err
                continue

            except psycopg.Error as err:
                retry_count += 1
                exec_count += 1
                log.debug(f'{call_id} | DB error: {err} | Retrying {retry_count}')
                time.sleep(self.db_retry_sleep_time)
                error = err
                continue

        if exec_count == retry_count > 0:
            log.error(f'{call_id} | DB Error: {error} | Retried: {retry_count} | No more retries...')
        else:
            if error is None:
                log.debug(f'{call_id} | DB execute successful | Retry count: {retry_count}')
            else:
                log.error(f'{call_id} | DB Error: {error} | Retried: {retry_count} | No more retries...')

        log.debug(f'{call_id} | Returning DB data: {res}')
        return res
