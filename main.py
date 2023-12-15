# Package: BulkDNS
# Module: main
# Author: Michal Selma <michal@selma.cc>
# Rev: 2023-12-15

import os
import sys
import configparser
import logging

from init import init_core
from core import proc_core

from common import logger

log = logging.getLogger('main')

if __name__ == '__main__':
    # Read configuration
    config = configparser.ConfigParser()
    if not config.read(os.path.realpath('./config.cfg')):
        print(f'Configuration file not found.')
        sys.exit(0)

    # When configuration is loaded modify logging configuration if needed
    log_level = config['DEFAULT']['log_level']
    logger.configure(log, log_level)

    log.info('1 - System initialization')
    log.info('2 - Domains check')
    log.info('3 - Archiving')
    log.info('Choose option and press Enter: ')
    user_option = input()
    if user_option == '1':
        init_core.run(config)
    elif user_option == '2':
        proc_core.run(config)
    elif user_option == '3':
        log.info('Not implemented yet...')
    else:
        log.info('Incorrect option picked')
