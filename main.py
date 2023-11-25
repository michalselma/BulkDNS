# Package: BulkDNS
# Module: main
# Author: Michal Selma <michal@selma.cc>
# Rev: 2023-11-25

import os
import sys
import configparser

from init import init_core
from common import logger

log = logger.log_run()


if __name__ == '__main__':
    # init logger
    logger.log_configure(log)

    # Read configuration
    config = configparser.ConfigParser()
    if not config.read(os.path.realpath('./config.cfg')):
        log.critical(f'Configuration file not found.')
        sys.exit(0)

    init_core.run(config)
