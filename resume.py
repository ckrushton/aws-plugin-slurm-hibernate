#!/usr/bin/env python3

import json
import filelock
import os
import sys
import time

import common

logger, config, partitions = common.get_common('resume')

# Retrieve the list of hosts to resume
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
nodes_to_resume = common.parse_node_names(expanded_hostlist)
logger.debug('Nodes to resume: %s', json.dumps(nodes_to_resume, indent=4))

for partition_name, nodegroups in nodes_to_resume.items():
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

            logger.debug("Existing nodelist for partition %s: %s" % (partition_name, ",".join(nodes)))

            # Expand with the new nodelist.
            new_nodes = list(common.get_node_name(partition_name, nodegroup_name, x) for x in node_ids)
            logger.debug("Additional nodelist for partition %s: %s" % (partition_name, ",".join(new_nodes)))
            nodes = set(nodes)

            # Add the new nodes to the existing node set.
            # Check to ensure there are no duplicates.
            for new_node in new_nodes:
                if new_node in nodes:
                    logger.warning("Node %s is already allocated to partition %s. Skipping..." % (new_node, partition_name))
                nodes.add(new_node)

            # Generate a new output file with the updated nodegroup.
            with open(nodegroup_file, "w") as o:
                for node in nodes:
                    o.write(node)
                    o.write(os.linesep)

        logger.info("Updated nodeset for parition %s" % partition_name)

# Give the daemon a chance to allocate nodes.
time.sleep(60)
