# Package: common
# Module: sqlite
# Author: Michal Selma <michal@selma.cc>
# Rev: 2023-11-19

# sqlite3.connect.autocommit = True is extremely inefficient for executemany() function
# using sqlite3.connect.commit() after executemany() instead

import os
import sqlite3


class DB:
    def __init__(self, db_type, db_path, db_name, db_retry):
        self.db_type = db_type
        self.db_path = db_path
        self.db_name = db_name
        self.db_retry = db_retry

    def create_new_db(self, call_id):
        # Initialize DB local file which is database for sqlite
        # In sqlite database is its physical file location (preferred dynamically calculated absolute path)
        db_file = os.path.abspath(f'{self.db_path}/{self.db_name}.sqlite3')
        try:
            # Establish connection (if db does not exist, con will automatically create it (file physical location))
            with sqlite3.connect(database=db_file):
                # print(f'{call_id} | DB connection successful')
                print(f'{call_id} | Database "{self.db_name}" auto-created successfully or exists in {db_file}')
                print(f'{call_id} | Database version: {sqlite3.sqlite_version}')
        except sqlite3.Error as err:
            print(f'{call_id} | DB error: {err}')
            raise err

    # For mass iteration and inserts -> cur.execute() is inefficient. Use cur.executemany()
    # Prepare param data sets first and then execute massive insert is much more efficient
    # To avid SQL Injection it is suggested to use execute_single_param (not predefined full query as in execute_single)
    def execute_single(self, query, call_id):
        # In sqlite database is its physical file location (preferred dynamically calculated absolute path)
        db_file = os.path.abspath(f'{self.db_path}/{self.db_name}.sqlite3')
        try:
            with sqlite3.connect(database=db_file) as con:
                cur = con.cursor()
                cur.execute("PRAGMA busy_timeout = 600000")  # set execution timeout in milliseconds
                cur.execute(query)
                # Return all rows
                res = cur.fetchall()
                cur.close()
                print(f'{call_id} | DB execute successful')
            return res
        except sqlite3.Error as err:
            print(f'{call_id} | DB error: {err}')

    def execute_many_param(self, param_query, params_array, call_id):
        # In sqlite database is its physical file location (preferred dynamically calculated absolute path)
        db_file = os.path.abspath(f'{self.db_path}/{self.db_name}.sqlite3')
        try:
            with sqlite3.connect(database=db_file) as con:
                cur = con.cursor()
                cur.execute("PRAGMA busy_timeout = 600000")  # set execution timeout in milliseconds
                cur.executemany(param_query, params_array)
                # Return all rows
                res = cur.fetchall()
                cur.close()
                # print(f'{call_id} | DB execute successful')
            return res
        except sqlite3.Error as err:
            print(f'{call_id} | DB error: {err}')

    def execute_single_param(self, param_query, params, call_id):
        # In sqlite database is its physical file location (preferred dynamically calculated absolute path)
        db_file = os.path.abspath(f'{self.db_path}/{self.db_name}.sqlite3')
        try:
            with sqlite3.connect(database=db_file) as con:
                cur = con.cursor()
                cur.execute("PRAGMA busy_timeout = 600000")  # set execution timeout in milliseconds
                cur.execute(param_query, params)
                # Return all rows
                res = cur.fetchall()
                cur.close()
                # print(f'{call_id} | DB execute successful')
            return res
        except sqlite3.Error as err:
            print(f'{call_id} | DB error: {err}')
