# Package: common
# Module: logger
# Author: Michal Selma <michal@selma.cc>
# Rev: 2023-12-15

import sys
import logging

from logging.handlers import RotatingFileHandler
from logging.handlers import TimedRotatingFileHandler


def configure(logger_name, log_level):

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

    if log_level == 'DEBUG':
        logger_name.setLevel(logging.DEBUG)
    else:
        logger_name.setLevel(logging.INFO)

    logger_name.propagate = 0
