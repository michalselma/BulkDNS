# Package: common
# Module: logger
# Author: Michal Selma <michal@selma.cc>
# Rev: 2023-11-25

import sys
import logging

from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler


def log_configure(logger_name):

    # This will create second file default formatted overwritten on each run
    logging.basicConfig(level=logging.INFO,
                        filename='./log/default.log',
                        filemode='w')

    # Define console handler
    handler_console = logging.StreamHandler(sys.stdout)
    handler_console.setLevel(logging.INFO)

    # Define file handler
    handler_file = RotatingFileHandler(filename='./log/logging.log', mode='a', maxBytes=10000000, backupCount=100)
    # handler_file = TimedRotatingFileHandler(filename='./log/logger.log', when='S', interval=1, backupCount=100)
    # handler_file.suffix = '%Y%m%d%H%M%S'
    handler_file.setLevel(logging.DEBUG)

    # Define screen formatter and add to console handler
    formatter_screen = logging.Formatter('%(message)s')
    handler_console.setFormatter(formatter_screen)

    # Define timestamp formatter and add to file handler
    formatter_timestamp = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler_file.setFormatter(formatter_timestamp)

    # Add both handlers to main logger
    logger_name.addHandler(handler_console)
    logger_name.addHandler(handler_file)

    # Define global logging level
    logger_name.setLevel(logging.DEBUG)


# Call/Create custom logger
def log_run():
    logger_name = 'main'
    log = logging.getLogger(logger_name)
    return log
