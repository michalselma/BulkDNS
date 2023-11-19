# Package: BulkDNS
# Module: main
# Author: Michal Selma <michal@selma.cc>
# Rev: 2023-11-19

import os
import sys
import configparser

from init import init_core


if __name__ == '__main__':
    # Read configuration
    config = configparser.ConfigParser()
    if not config.read(os.path.realpath('./config.cfg')):
        print(f'Configuration file not found.')
        sys.exit(0)

    init_core.run(config)
