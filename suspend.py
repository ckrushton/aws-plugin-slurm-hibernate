#!/usr/bin/env python3

import sys
import common

logger, config = common.get_common('suspend')

# Retrieve the list of hosts to suspend
try:
    hostlist = sys.argv[1]
    logger.info('Hostlist: %s' %hostlist)
except:
    logger.critical('Missing hostlist argument')
    sys.exit(1)

logger.debug('Nodes to suspend: %s', hostlist)
