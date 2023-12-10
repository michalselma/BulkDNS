# Package: BulkDNS
# Module: main
# Author: Michal Selma <michal@selma.cc>
# Rev: 2023-12-10

import os
import sys
import configparser
import logging

from init import init_core

from common import logger

log = logging.getLogger('main')

if __name__ == '__main__':
    # init logging configuration for root logger
    logger.configure(log)

    # Read configuration
    config = configparser.ConfigParser()
    if not config.read(os.path.realpath('./config.cfg')):
        log.critical(f'Configuration file not found.')
        sys.exit(0)

    log.info('1 - System initialization')
    log.info('2 - Domains check')
    log.info('3 - Archiving')
    log.info('Choose option and press Enter: ')
    user_option = input()
    if user_option == '1':
        init_core.run(config)
    elif user_option == '2':
        log.info('Not implemented yet...')
    elif user_option == '3':
        log.info('Not implemented yet...')
    else:
        log.info('Incorrect option picked')
