#!/usr/bin/env python3

import json
import filelock
import os
import sys
import time

import common

logger, config, partitions = common.get_common('suspend')

# Retrieve the list of hosts to suspend
try:
    hostlist = sys.argv[1]
    logger.info('Hostlist: %s' %hostlist)
except:
    logger.critical('Missing hostlist argument')
    sys.exit(1)

# Expand the hoslist and retrieve a list of node names
expanded_hostlist = common.expand_hostlist(hostlist)
logger.debug('Expanded hostlist: %s' %', '.join(expanded_hostlist))


# Parse the expanded hostlist
nodes_to_suspend = common.parse_node_names(expanded_hostlist)
logger.debug('Nodes to suspend: %s', json.dumps(nodes_to_suspend, indent=4))


for partition_name, nodegroups in nodes_to_suspend.items():
    for nodegroup_name, node_ids in nodegroups.items():

        nodegroup = common.get_partition_nodegroup(partition_name, nodegroup_name)

        # Which file contains the nodes currently running in this fleet?
        nodegroup_folder = config["NodePartitionFolder"]
        nodegroup_file = nodegroup_folder + os.path.sep + "_".join([partition_name, nodegroup_name, "paritions.txt"])

        # Lock this file to prevent other instances from updating it at the same time,
        # avoiding a race condition.
        lockfile = nodegroup_file + ".lock"
        lock = filelock.FileLock(lockfile)
        with lock:
            # Get a list of all nodes currently allocated to this partition.
            nodes = list()
            with open(nodegroup_file) as f:
                for line in f:
                    line = line.rstrip("\r\n")
                    nodes.append(line)

            nodes = set(nodes)
            logger.debug("Existing nodelist for partition %s: %s" % (partition_name, ",".join(nodes)))

            # Obtain the IDs of the nodes to suspend.
            nodes_to_remove = list(common.get_node_name(partition_name, nodegroup_name, x) for x in node_ids)
            logger.debug("Suspend nodelist for partition %s: %s" % (partition_name, ",".join(nodes_to_remove)))

            # Remove the nodes to suspended.
            # Sanity check to ensure these nodes are are actually allocated in this partition.
            for r_node in nodes_to_remove:
                if r_node not in nodes:
                    logger.warning("Node %s cannot be suspended because it is not currently running in partition %s" % (r_node, partition_name))
                else:
                    nodes.remove(r_node)

            # Generate a new output file with the updated nodegroup.
            with open(nodegroup_file, "w") as o:
                for node in nodes:
                    o.write(node)
                    o.write(os.linesep)

        logger.info("Updated nodeset for parition %s" % partition_name)

# Give the daemon a chance to stop nodes.
time.sleep(60)

