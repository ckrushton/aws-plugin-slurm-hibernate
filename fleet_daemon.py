#!/usr/bin/env python3

# Adapted from https://github.com/aws-samples/aws-plugin-for-slurm

import boto3
import filelock
import os
import time
import json

import common

logger, config = common.get_common("fleet-daemon")


# Update /etc/hosts with the IP of this new node
def update_hosts_file(node_name, ip, hostfile="/etc/hosts"):

    lockfile = hostfile + ".lock"
    lock = filelock.FileLock(lockfile, timeout=10)
    try:
        with lock:
            # Load existing hosts file.
            hostentries = []
            with open(hostfile) as f:
                for line in f:
                    line = line.rstrip("\r\n")
                    cols = line.split(" ")
                    # If this hostname already exists, drop it. We will update the IP later.
                    if len(cols) > 1 and cols[1] == node_name:
                        continue
                    hostentries.append(line)

            # Add new host entries for the new node.
            new_host = ip + " " + node_name
            hostentries.append(new_host)
            logger.debug("Updated %s with %s" % (hostfile, new_host))
            # Update and write the new hostfile.
            with open(hostfile, "w") as o:
                for host in hostentries:
                    o.write(host)
                    o.write(os.linesep)

    except TimeoutError:
        logger.info("Unable to update hostfile %s: File locked" % hostfile)
    except Exception as e:
        logger.warning("Unable to update hostfile %s - %s" % (hostfile, e))


def allocate_instance(instance, node_name, weight=1):
    
    instance_id = instance["InstanceId"]
    instance_ip = instance["PrivateIpAddress"]
    # Assign the appropriate name to this node.
    try:
        client.create_tags(Resources = [instance_id], Tags=[{"Key": "Name", "Value": node_name}])
    except Exception as e:
        logger.warning("Unable to assign instance %s name %s - %s" % (instance_id, node_name, e))
    # Allocate this new node to Slurm
    common.update_node(node_name, "nodeaddr=%s nodehostname=%s comment=InstanceId:%s weight=%s" % (instance_ip, node_name, instance_id, weight))
    update_hosts_file(node_name, instance_ip)


# Request new EC2 instances and assign to Slurm nodes.
def request_new_instances(client, new_nodes, config, nodegroup):

    num_new_nodes=len(new_nodes)
    num_allocated = 0
    launch_template = config["LaunchTemplate"]
    is_spot = config["PurchasingOption"] == "spot"
    interrupt_behavior = config["InteruptionBehavior"]

    tag_specifications = [
        {"ResourceType": "instance",
         "Tags": [
                {"Key": "nodegroup",
                 "Value": nodegroup
                 }
            ]
        }
    ]
    market_options = {}
    if is_spot:
        market_options = {"MarketType": "spot",
                          "SpotOptions": {
                              "SpotInstanceType": "persistent",
                              "InstanceInterruptionBehavior": interrupt_behavior
                          }
                          }

        # Try to allocate spot instances first.
        for instance_type in config["Instances"]:
            for subnet in config["SubnetIds"]:
                try:
                    num_outstanding_nodes = num_new_nodes - num_allocated
                    logger.debug("Requesting %s spot instances of type %s in subnet %s" % (num_outstanding_nodes, instance_type, subnet))
                    instance_response = client.run_instances(LaunchTemplate={"LaunchTemplateId" :launch_template}, InstanceType=instance_type, MinCount=1, MaxCount=num_outstanding_nodes, SubnetId=subnet,
                                    InstanceMarketOptions=market_options, TagSpecifications=tag_specifications)
                    logger.debug("Run Instance response - %s" % json.dumps(instance_response,indent=4,default=str))
                    # Did we manage to allocate nodes?
                    for instance in instance_response["Instances"]:
                        node_name = new_nodes[num_allocated]
                        allocate_instance(instance, node_name, weight=2)
                        num_allocated += 1

                    # To not flood AWS API with requests.
                    time.sleep(0.2)

                except Exception as e:
                    logger.info("Unable to fullfill spot request for %s %s instances in subnet %s - %s" % (num_outstanding_nodes, instance_type, subnet, e))

                if num_allocated == num_new_nodes:
                    break
            if num_allocated == num_new_nodes:
                break

    # If this is an on-demand fleet or we can't allocate spot instances, request on-demand instances.
    if num_allocated < num_new_nodes:
        for instance_type in config["Instances"]:
            for subnet in config["SubnetIds"]:
                try:
                    num_outstanding_nodes = num_new_nodes - num_allocated
                    logger.debug("Requesting %s On-Demand Instances of type %s in subnet %s" % (num_outstanding_nodes, instance_type, subnet))
                    instance_response = client.run_instances(LaunchTemplate={"LaunchTemplateId" :launch_template}, InstanceType=instance_type, MinCount=1, 
                                                             MaxCount=num_outstanding_nodes, SubnetId=subnet, TagSpecifications=tag_specifications)
                    logger.debug("Run Instance response - %s" % json.dumps(instance_response,indent=4,default=str))
                    # Did we manage to allocate nodes?
                    for instance in instance_response["Instances"]:
                        node_name = new_nodes[num_allocated]
                        allocate_instance(instance, node_name, weight=1)
                        num_allocated += 1

                    # To not flood AWS API with requests.
                    time.sleep(0.2)

                except Exception as e:
                    logger.info("Unable to fullfill On-Demand request for %s %s instances in subnet %s - %s" % (num_outstanding_nodes, instance_type, subnet, e))

                if num_allocated == num_new_nodes:
                    break
            if num_allocated == num_new_nodes:
                break

    if num_allocated < num_new_nodes:
        # If we can't allocate enough instances right now, that is okay, we will try again the next time the daemon is run.
        logger.warning("Unable launch %s instances" % num_outstanding_nodes)


# Compare the nodes that are currently running to those present in the fleet, and determine what changes are required.
def process_fleet_nodes(client, nodes, instances):

    nodes_to_create = []
    seen_instances = []

    # Process all nodes in this partition and nodegroup.
    for node_name, node_attributes in nodes.items():
        # Has this node been associated with an EC2 instance?
        logger.info("Processing node %s" % node_name)
        
        instance_id = None
        instance_attributes = {}

        if "Comment" in node_attributes:
            instance_id = node_attributes["Comment"]["InstanceId"]
            if instance_id == "":
                instance_id = None
            else:
                logger.info("Node %s is linked to instance %s" % (node_name, instance_id))
                try:
                    instance_attributes = instances[instance_id]
                    seen_instances.append(instance_id)
                    logger.debug("Instance detail for %s: %s" % (instance_id, instance_attributes))
                except KeyError:  # This instance was likely terminated a long time ago.
                    instance_id = None

        node_states = set(node_attributes["State"])

        # If this node has been powered down because it went down, reset it and set it to idle.
        # Let the node fully power down first so we don't overwhelm Slurm.
        if "DOWN" in node_states and "POWERED_DOWN" in node_states:
            logger.info("Node %s is POWERED_DOWN because it went DOWN. Resetting..." % node_name)
            common.update_node(node_name, "state=IDLE")

        elif instance_id is None:
            # No instance is allocated to this node.
            if "POWERING_UP" in node_states:
                # We need to allocate an instance to this node.
                nodes_to_create.append(node_name)
            elif "POWERED_DOWN" in node_states or "POWERING_DOWN" in node_states:
                # Node is powered down, and no instance is linked. This is the appropriate senario.
                pass
            else:
                # Node is up, but there is no associated instance (was it terminated outside of Slurm's control?)
                # Set this node to DOWN.
                common.update_node(node_name, "state=POWER_DOWN_FORCE reason=instance_terminated")
        else:
            # An instance is allocated to this node.
            # Check to ensure the IP address for the node is correct.
            instance_ip = instance_attributes["PrivateIpAddress"]
            # If this node is powered down, terminate the associated instance.
            if "POWERED_DOWN" in node_states or "POWERING_DOWN" in node_states:
                # If there is still an EC2 instance linked with this node, terminate it.
                if instance_attributes["State"]["Name"] not in ["terminated", "stopping"]:
                    logger.info("Node %s is set to POWER_DOWN. Terminating linked instance %s" % (node_name, instance_id))
                    terminate_instance(client, instance_id, instance_attributes)
                    # Remove this linked node, as it is terminated.
                    common.update_node(node_name, "Comment=InstanceId:")
            # Instance is UP
            elif instance_ip != node_attributes["NodeAddr"]:
                logger.info("Node %s is assigned an IP address of %s, but linked instance %s has an IP address of %s. Updating..." % (node_name, node_attributes["NodeAddr"], instance_id, instance_ip))
                common.update_node(node_name, "nodeaddr=%s" % instance_ip)
            # If the underlying instance is hibernated, set it to DRAIN to prevent additional jobs from being allocated to this node.
            if instance_attributes["State"]["Name"] == "stopped" and not "DRAIN" in node_states:
                logger.info("Node %s is linked with a hibernated instance %s. Setting to DRAIN" % (node_name, instance_id))
                common.update_node(node_name, "state=DRAIN reason=instance_hibernated")
            elif "DRAIN" in node_states and instance_attributes["State"]["Name"] != "stopped":
                logger.info("Node %s is linked with a non-hibernated instance %s. Setting to UNDRAIN" % (node_name, instance_id))
                common.update_node(node_name, "state=UNDRAIN")
            # Node is stuck.
            elif "DOWN" in node_states:
                logger.error("Node %s is stuck and has been set to DOWN. Powering down and resetting..." % (node_name))
                common.update_node(node_name, "state=POWER_DOWN reason=node_stuck")
            # In some situations, a node may be placed in COMPLETING+DRAIN state by Slurm 
            # and remains stuck. In that case, force the node to become DOWN
            if "COMPLETING" in node_states and ("DRAIN" in node_states or "NOT_RESPONDING" in node_states):
                common.update_node(node_name, "state=POWER_DOWN_FORCE reason=node_stuck")

    # Are there any instances which are not associated with a node?
    orphan_instances = {x: y for x, y in instances.items() if x not in seen_instances}

    return nodes_to_create, orphan_instances


def terminate_instance(client, instance_id, instance_attributes):
    """
    Terminate an EC2 instance and the associated Spot request ID
    """

    try:
        client.terminate_instances(InstanceIds=[instance_id])
        logger.info("Terminated Instance %s" % (instance_id))
    except Exception as e:
        logger.error("Failed to terminate Instance %s - %s" %(instance_id, e))
    
    # To prevent Spot requests from being re-fulfilled even after their instance is terminated, 
    # cancel the associated Spot requests.
    if "SpotInstanceRequestId" in instance_attributes:
        spot_id = instance_attributes["SpotInstanceRequestId"]
        try:
            client.cancel_spot_instance_requests(SpotInstanceRequestIds=[spot_id])
            logger.info("Cancelled Spot Instance request %s for instance %s" % (spot_id, instance_id))
        except Exception as e:
            logger.error("Failed to cancel Spot Instance request %s - %s" %(spot_id, e))
    time.sleep(0.1)


def scontrol_nodeinfo():

    scontrol_output = common.run_scommand("scontrol", ["show", "nodes"])
    nodeinfo = {}
    current_node = None
    for line in scontrol_output:

        if line.startswith("NodeName"):
            # Node information line
            # ex. NodeName=xlarge-node-3 CoresPerSocket=64
            nodename = line.split(" ")[0].split("=")[1]
            # Check and ensure there are no duplicate nodenames.
            if nodename in nodeinfo:
                raise AttributeError("Duplicate nodename detected: %s" % nodename)
            current_node = nodename
            nodeinfo[current_node] = {}
        else:
            # Information about the current node.
            line_split = list(i for i in line.split(" ") if "=" in i)
            attributes = {i.split("=")[0]: i.split("=")[1] for i in line_split}
            # If the node states are provided (ex. IDLE+CLOUD+POWERED_DOWN), divide those into an
            # iterable (ex. ["IDLE", "CLOUD", "POWERED_DOWN"])
            if "State" in attributes:
                attributes["State"] = attributes["State"].split("+")

            # If a comment is provided (ex. InstanceId: i-21421adf), unpack those arguments into a dictionary.
            if "Comment" in attributes:
                comment_split = list(x for x in attributes["Comment"].split(","))
                attributes["Comment"] = {x.split(":")[0]: x.split(":")[1] for x in comment_split}

            nodeinfo[current_node].update(attributes)

    return nodeinfo


def get_all_instances_for_nodegroup(client, launch_template, nodegroup):

    filter_string = [
        {"Name": "tag:aws:ec2launchtemplate:id", 
         "Values": [launch_template],
        },
        {"Name": "tag:nodegroup",
         "Values": [nodegroup]
        },
        {"Name": "instance-state-name",
         "Values": ["pending", "running", "stopped", "stopping"]}
    ]

    instance_ids = {}
    try:
        response_describe = client.describe_instances(Filters=filter_string)
        if len(response_describe["Reservations"]) > 0:
            for reservation in response_describe["Reservations"]:
                for instance in reservation["Instances"]:
                    instance_id = instance["InstanceId"]
                    instance_ids[instance_id] = instance
    except Exception as e:
        logger.error("Unable to describe instances for launch template %s - %s" % (launch_template, e))

    return instance_ids


# Get a list of all nodes and associated information from Slurm.
all_node_info = scontrol_nodeinfo()

for partition_name, nodegroups in config["Partitions"].items():
 
    for nodegroup_name, nodegroup_atts in nodegroups.items():

        region = config["Region"]

        # Start AWS client.
        client = boto3.client('ec2', region_name=region)

        # Get the list of nodes associated with this partition.
        logger.info("Examining partition: '%s' nodegroup: '%s'" % (partition_name, nodegroup_name))
        logger.debug("Obtaining Slurm node information for nodegroup")
        nodegroup_prefix = partition_name + "-" + nodegroup_name
        nodes = {x: y for x, y in all_node_info.items() if x.startswith(nodegroup_prefix) and y["Partitions"] == partition_name}  # Filter for nodes associated with this partition.

        # Get a list of all instances created with the compute node launch template.
        launch_template = nodegroup_atts["LaunchTemplate"]
        instances = get_all_instances_for_nodegroup(client, launch_template, nodegroup_prefix)
        interrupt_behavior = nodegroup_atts["InteruptionBehavior"]
        logger.debug("Instances currently associated with this nodegroup: %s" % (list(instances.keys())))

        nodes_to_create, orphan_instances = process_fleet_nodes(client, nodes, instances)

        # Do we need to assign instances to nodes which are now powering up?
        if len(nodes_to_create) > 0:
            logger.info("Allocating instances for POWERING_UP nodes %s" % ",".join(nodes_to_create))
            request_new_instances(client, nodes_to_create, nodegroup_atts, nodegroup_prefix)

        # Are there any extraneous instances we should clean up?
        if len(orphan_instances) > 0:
            logger.warning("Terminating orphan instances %s " % ",".join(orphan_instances))
            for instance_id, instance_attr in orphan_instances.items():
                terminate_instance(client, instance_id, instance_attr)

logger.info("Fleet daemon complete")
