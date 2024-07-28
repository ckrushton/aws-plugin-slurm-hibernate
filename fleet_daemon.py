#!/usr/bin/env python3

# Adapted from https://github.com/aws-samples/aws-plugin-for-slurm

import boto3
import filelock
import json
import os
import time

import common

logger, config, partitions = common.get_common('resume')

# Obtain a list of all instances assigned to an EC2 fleet.
def get_fleet_instances(fleet_id):

    # Get a list of instances allocated with this fleet.
    try:
        instance_response = client.describe_fleet_instances(FleetId=fleet_id)
        instances = list(x["InstanceId"] for x in instance_response["ActiveInstances"])
        logger.debug("DescribeFleetInstances Response:" + json.dumps(instance_response,indent=4))
    except Exception as e:
        logger.error('Failed to describe fleet instances for %s: %s' % (fleet_id, e))
        instances = []

    if len(instances) > 0:
        # Filter for instances which are running
        try:
            response_describe = client.describe_instances(InstanceIds=instances,
                Filters=[{'Name': 'instance-state-name', 'Values': ['pending', 'running']}]
                )
            instances = []
            if len(response_describe["Reservations"]) > 0:
                for reservation in response_describe["Reservations"]:
                    for instance_info in reservation["Instances"]:
                        instances.append(instance_info["InstanceId"])
        except Exception as e:
            logger.critical('Failed to describe instances in fleet %s - %s' % (fleet_id, e))
            instances = []

    return instances


def get_fleet_region_type(partitions):

    partition_regions = {}
    partition_type = {}
    for partition in partitions:
        partition_name = partition["PartitionName"]
        partition_regions[partition_name] = {}
        partition_type[partition_name] = {}

        for nodegroup in partition["NodeGroups"]:
            nodegroup_name = nodegroup["NodeGroupName"]
            nodegroup_region = nodegroup["Region"]
            nodegroup_type = nodegroup["PurchasingOption"]
            partition_regions[partition_name][nodegroup_name] = nodegroup_region
            partition_type[partition_name][nodegroup_name] = nodegroup_type

    return partition_regions, partition_type


def allocate_new_instances(fleet_id, new_instances, nodes_to_create):

    node_index = 0
    # Alloocate and name the new instances
    if len(new_instances) == 0:
        # No new instances in fleet.
        pass
    else:
        try:
            response_describe = client.describe_instances(InstanceIds=new_instances,
                Filters=[
                    {'Name': 'instance-state-name', 'Values': ['pending', 'running']}
                ]
            )
        except Exception as e:
            logger.critical('Failed to describe instances in fleet %s - %s' % (fleet_id, e))

        if len(response_describe["Reservations"]) > 0:
            # At least one node was allocated.

            for reservation in response_describe["Reservations"]:
                for instance in reservation["Instances"]:

                    # Already allocated sufficient instances.
                    if node_index == len(nodes_to_create):
                        break

                    instance_id = instance["InstanceId"]
                    ip_address = instance['PrivateIpAddress']
                    instance_name = nodes_to_create[node_index]
                    node_index += 1
                    hostname = 'ip-%s' %'-'.join(ip_address.split('.'))

                    logger.info('Launched node %s %s %s' %(instance_name, instance_id, ip_address))

                    # Tag the instance
                    new_tags =  [
                        {
                            'Key': "Name",
                            'Value': instance_name
                        }
                    ]

                    try:
                        client.create_tags(Resources=[instance_id], Tags=new_tags)
                        logger.debug('Tagged node %s: %s' %(instance_id, json.dumps(new_tags, indent=4)))
                    except Exception as e:
                        logger.error('Failed to tag node %s - %s' %(instance_id, e))
                        continue

                    # Update node information in Slurm
                    try:
                        slurm_param = 'nodeaddr=%s nodehostname=%s' %(ip_address, hostname)
                        common.update_node(instance_name, slurm_param)
                        logger.debug('Updated node information in Slurm %s' % instance_name)
                    except Exception as e:
                        logger.error('Failed to update node information in Slurm %s - %s' %(instance_name, e))

                    # Update hostsfile with new node IP
                    update_hosts_file(instance_name, ip_address)

                    try:
                        new_instances.remove(instance_id)
                    except ValueError:
                        pass

    return node_index, new_instances


# Adjust the size of an EC2 Fleet Request.
def adjust_fleet_size(client, fleet_id, new_fleet_size):

    # Obtain the current size of the fleet
    try:
        describe_fleet_response = client.describe_fleets(FleetIds=[fleet_id])
        fleet_size = describe_fleet_response["Fleets"][0]["TargetCapacitySpecification"]
    except Exception as e:
        logger.error("Failed to describe fleet %s - %s" % (fleet_id, e))
        return False

    # Compare the current and requested fleet sizes.
    needs_to_resize = False
    for instance_type, capacity in new_fleet_size.items():
        if capacity != fleet_size[instance_type]:
            needs_to_resize = True

    if needs_to_resize:
        logger.debug("Modifying size of fleet %s to %s" % (fleet_id, new_fleet_size))
        try:
            change_fleet_response = client.modify_fleet(FleetId=fleet_id, TargetCapacitySpecification=new_fleet_size, ExcessCapacityTerminationPolicy="no-termination")
        except Exception as e:
            logger.error('Failed to update fleet size for %s: %s' % (fleet_id, e))

    return True


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
        logger.warn("Unable to updat hostfile %s - %s" % (hostfile, e))


# Compare the nodes that are currently running to those present in the fleet, and determine what chages are required.
def link_nodes_to_fleet(client, fleet_instances, nodes):

    spot_instances = {}
    demand_instances = {}
    new_instances = []

    if len(fleet_instances) > 0:
        # Instances are already allocated to these fleets
        # Parse information from instances.
        try:
            response_describe = client.describe_instances(InstanceIds=list(fleet_instances),
                Filters=[
                    {'Name': 'instance-state-name', 'Values': ['pending', 'running']}
                ]
            )
        except Exception as e:
            logger.critical('Failed to describe instances in partition %s - %s' % (partition_name, e))
            return {}, {}, {}, []

        # Get detailed instance information for this fleet.
        if len(response_describe["Reservations"]) == 0:
            # No instances in fleet.
            pass
        else:
            for reservation in response_describe["Reservations"]:
                for instance in reservation["Instances"]:
                    instance_id = instance["InstanceId"]
                    instance_ip = instance['PrivateIpAddress']
                    instance_name = None
                    for tag in instance["Tags"]:
                        if tag["Key"] == "Name":
                            instance_name = tag["Value"]

                    # If the instance name or type tag is not set, these nodes has not yet been configured
                    if instance_name is None:
                        new_instances.append(instance_id)
                        continue

                    # Is this a Spot instance or On-Demand instance?
                    if "InstanceLifecycle" in instance and instance["InstanceLifecycle"] == "spot":
                        spot_instances[instance_name] = instance_id
                    else:
                        demand_instances[instance_name] = instance_id

                    # Store the instance ID associated with this node.
                    if instance_name in nodes:
                        nodes[instance_name] = instance_id

                        # Check the IP of this, and update if necessary.
                        node_info = common.run_scommand("scontrol", ["show", "nodes", instance_name])
                        for line in node_info:
                            if line.startswith("   NodeAddr="):
                                node_ip = line.split(" ")[3]
                                node_ip = node_ip.replace("NodeAddr=", "")
                                if node_ip != instance_ip:
                                    logger.info("Node %s has changed IP address from %s to %s. Updating" % (instance_name, node_ip, instance_ip))
                                    slurm_param = 'nodeaddr=%s nodehostname=%s' %(instance_ip, instance_name)
                                    common.update_node(instance_name, slurm_param)

    return spot_instances, demand_instances, nodes, new_instances


# Determine which fleet is associated with which partition.
partition_fleet_ids = common.parse_fleet_ids()
partition_regions, partition_type = get_fleet_region_type(partitions)

for partition_name, nodegroups in partition_fleet_ids.items():
    for nodegroup_name, fleet_id in nodegroups.items():

        # Start AWS client.
        region = partition_regions[partition_name][nodegroup_name]
        fleet_type = partition_type[partition_name][nodegroup_name]
        client = boto3.client('ec2', region_name=region)

        # Get the file listing the nodes to run in this partition.
        nodegroup_folder = config["NodePartitionFolder"]
        nodegroup_file = nodegroup_folder + os.path.sep + "_".join([partition_name, nodegroup_name, "paritions.txt"])

        # Lock this file to prevent other daemon instances from modifying this fleet at the same time.
        lockfile = nodegroup_file + ".lock"
        lock = filelock.FileLock(lockfile, timeout = 10)

        try:
            with lock:

                # Load the list of nodes associated with this partition.
                # This is the "ideal" state of the partition.
                nodes = {}
                with open(nodegroup_file) as f:
                    for line in f:
                        line = line.rstrip("\r\n")
                        nodes[line] = None

                num_nodes = len(nodes)

                # Sanity check that the fleet is healthy.
                # And determine how many nodes have been allocated to this fleet.
                logger.info("Examining partition %s" % (partition_name))
                logger.debug('Found fleet ID %s for partition %s' % (fleet_id, partition_name))
                try:
                    fleet_status_response = client.describe_fleets(FleetIds=[fleet_id])
                    fleet_status = fleet_status_response["Fleets"][0]["FleetState"]
                    if fleet_status != "active":
                        logger.error("Fleet %s is not in a healthy state %s" % (fleet_id, fleet_status))
                        continue
                except Exception as e:
                    logger.error('Failed to obtain fleet status for %s: %s' % (fleet_id, e))
                    continue

                # How many instances are currently running with this fleet?
                fleet_instances = set(get_fleet_instances(fleet_id))
                spot_instances, demand_instances, nodes, new_instances = link_nodes_to_fleet(client, fleet_instances, nodes)

                num_instances = len(spot_instances) + len(demand_instances)

                # Find which instances to delete from the current fleet.
                od_to_remove = list(y for x, y in demand_instances.items() if x not in nodes)
                spot_to_remove = list(y for x, y in spot_instances.items() if x not in nodes)
                # Find which new instances need to be allocated.
                nodes_to_create = list(x for x, y in nodes.items() if y is None)
                if fleet_type == "spot":
                    new_demand_target = len(demand_instances) - len(od_to_remove)
                    new_spot_target = len(spot_instances) - len(spot_to_remove) + len(nodes_to_create)
                else:
                    new_demand_target = len(demand_instances) - len(od_to_remove) + len(nodes_to_create)
                    new_spot_target = len(spot_instances) - len(spot_to_remove)

                # Status messages.
                logger.info("Partition %s is requesting %s active nodes" % (partition_name, num_nodes))
                logger.debug("Partition %s active nodes: %s" % (partition_name, ",".join(nodes.keys())))
                logger.info("Partition %s currently has %s Spot nodes and %s On-Demand nodes in fleet" % (partition_name, len(spot_instances), len(demand_instances)))
                logger.debug("Partition %s Spot Instances: %s" % (partition_name, ",".join(spot_instances.keys())))
                logger.debug("Partition %s On-Demand Instances: %s" % (partition_name, ",".join(demand_instances.keys())))
                logger.debug("Partition %s Instances awaiting configuration: %s" % (partition_name, ",".join(new_instances)))
                logger.info("Partition %s needs to remove %s instances from fleet, and start %s instances" % (partition_name, len(od_to_remove) + len(spot_to_remove), len(nodes_to_create)))

                # Is this fleet already configured?
                if len(od_to_remove) == 0 and len(spot_to_remove) == 0 and len(nodes_to_create) == 0:
                    logger.info("Partition %s already fully provisioned" % partition_name)
                    continue

                # Expand or shrink fleet size, if necessary
                fleet_command = {"TotalTargetCapacity": num_nodes, "OnDemandTargetCapacity": new_demand_target, "SpotTargetCapacity": new_spot_target}
                adjust_fleet_size(client, fleet_id, fleet_command)

                # Delete extraneous instances.
                # On-demand instances.
                if len(od_to_remove) != 0:
                    try:
                        client.terminate_instances(InstanceIds=od_to_remove)
                        logger.info('Terminated on-demand instances %s from fleet %s' % (",".join(od_to_remove), fleet_id))
                    except Exception as e:
                        logger.error('Failed to terminate On-Demand instances in fleet %s - %s' %(fleet_id, e))
                    time.sleep(1)
                # Spot instances.
                if len(spot_to_remove):
                    try:
                        client.terminate_instances(InstanceIds=spot_to_remove)
                        logger.info('Terminated Spot Instances %s from fleet %s' % (",".join(spot_to_remove), fleet_id))
                    except Exception as e:
                        logger.error('Failed to terminate Spot Instances in fleet %s - %s' %(fleet_id, e))
                    time.sleep(1)

                # Allocate new instances.
                # NOTE: As this daemon only runs once every minute, these new instances were likely started during previous daemon runs.
                if len(new_instances) > 0:
                    num_new_instances, orphan_instances = allocate_new_instances(fleet_id, new_instances, nodes_to_create)
                    
                    # Clean up any remaining (overprovisioned) instances
                    for orphan in orphan_instances:
                        logger.debug("Terminating orphan instance %s" % orphan)
                        try:
                            client.terminate_instances(InstanceIds=[orphan])
                        except Exception as e:
                            logger.error('Failed to terminate orphan instance in fleet %s - %s' %(fleet_id, e))
                else:
                    logger.debug("No new instances currently allocated to fleet %s. Will check again later." % (fleet_id))

        except TimeoutError:
            logger.warning("Failed to process partition %s: Partition is locked" % partition_name)

logger.info("Daemon completed successfully.")