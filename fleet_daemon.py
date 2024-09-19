#!/usr/bin/env python3

# Adapted from https://github.com/aws-samples/aws-plugin-for-slurm

import boto3
import datetime
import filelock
import json
import os
import time

import common

logger, config, partitions = common.get_common('fleet-daemon')
daemon_start_time = datetime.datetime.now()

# Obtain a list of all instances assigned to an EC2 fleet.
def get_fleet_instances(fleet_id):

    # Get a list of instances allocated with this fleet.
    try:
        # We include stopping and stopped instances, as those could be in the process of hibernation.
        fleet_filters = [{"Name": "tag:aws:ec2:fleet-id", "Values": [fleet_id]}, {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}]
        instance_response = client.describe_instances(Filters=fleet_filters)
        logger.debug("DescribeFleetInstances Response:" + json.dumps(instance_response,indent=4,default=str))
        instances = []
        if len(instance_response["Reservations"]) > 0:  # There are instances allocated to this fleet.
            for reservation in instance_response["Reservations"]:
                instances.extend(x["InstanceId"] for x in reservation["Instances"])
    except Exception as e:
        logger.error('Failed to describe fleet instances for %s: %s' % (fleet_id, e))
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
                    {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping', 'stopped']}
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


# Check a fleet history to determine if we have been waiting on new instances for a prolonged period of time
def spot_allocation_timeout(client, fleet_id, timeout_seconds=300):

    # Check the fleet history from a minute ago, to ensure all the events have been updated
    start_time = daemon_start_time - datetime.timedelta(seconds=timeout_seconds)

    start_time = '{:%Y-%m-%dT%H:%M:00Z}'.format(start_time)

    # Sanity check that this fleet is not stuck modifying.
    # This can occur if the fleet is blacklisted, and we don't want to make the situation worse.
    try:
        describe_fleet_response = client.describe_fleets(FleetIds=[fleet_id])
        fleet_info = describe_fleet_response["Fleets"][0]
    except Exception as e:
        logger.error("Failed to describe fleet %s - %s" % (fleet_id, e))
        return False

    # Is the fleet is currently being edited?
    if fleet_info["FleetState"] == "modifying":
        return False

    try:
        fleet_history = client.describe_fleet_history(FleetId=fleet_id, StartTime=start_time)
    except Exception as e:
        logger.error("Failed to describe fleet history %s - %s" % (fleet_id, e))
        return False  # Assume the fleet has not timed out, as we cannot check it.

    # Remove periodic stop bid updates.
    fleet_events = list(x for x in fleet_history["HistoryRecords"] if x["EventType"] != "spotInstanceRequestChange")

    # Check to see if there have been any updates to the fleet in the prescribed period of time.
    if len(fleet_events) == 0:
       logger.warning("Fleet %s has not been updated for more than %s seconds, despite requesting additional capacity" % (fleet_id, timeout_seconds))
       # No events.
       return True

    # Is the most recent event warning about spot capacity?
    last_record = fleet_events[-1]
    if last_record["EventType"] == "error":
        last_record_event = last_record["EventInformation"]["EventSubType"]
        if last_record_event == "spotInstanceCountLimitExceeded" or last_record_event == "spotFleetRequestConfigurationInvalid":
            # We are out of spot capacity.
            logger.warning("Fleet %s is out of spot capacity - %s" % (fleet_id, last_record_event))
            return True
        if last_record_event == "allLaunchSpecsTemporarilyBlacklisted":
            # We have not been very elegant, and there have been repeted problems launching instances.
            # We have been blacklisted for a few minutes.
            logger.warning("Fleet %s is currently blacklisted and will not be further modified - %s" % (fleet_id, last_record))

    return False


# Convert any unallocated spot capacity to on-demand capacity.
def spot_fallback_to_demand(client, fleet_id, target_capacity):

    # Get the instances currently assigned to this fleet
    fleet_instances = get_fleet_instances(fleet_id)

    # How many spot instances are allocated?
    try:
        describe_response = client.describe_instances(InstanceIds=fleet_instances, Filters=[
                    {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping', 'stopped']},
                    {"Name": "instance-lifecycle", "Values": ["spot"]}
                ])
    except Exception as e:
        logger.warning("Unable to describe spot instances in fleet %s - %s" % (fleet_id, e))

    num_spot_allocated = sum(len(x["Instances"]) for x in describe_response["Reservations"])

    # How many additional On-Demand instances should we request?
    target_od = target_capacity - num_spot_allocated

    # Adjust the size of the requested fleet
    fleet_command = {"TotalTargetCapacity": target_capacity, "OnDemandTargetCapacity": target_od, "SpotTargetCapacity": num_spot_allocated}
    logger.debug("Modifying size of fleet %s to %s" % (fleet_id, fleet_command))
    try:
        change_fleet_response = client.modify_fleet(FleetId=fleet_id, TargetCapacitySpecification=fleet_command, ExcessCapacityTerminationPolicy="no-termination")
    except Exception as e:
        logger.error('Failed to update fleet size for %s: %s' % (fleet_id, e))


# Adjust the size of an EC2 Fleet Request.
def adjust_fleet_size(client, fleet_id, target_size, num_od_remove, fleet_type):

    # Obtain the current size of the fleet
    try:
        describe_fleet_response = client.describe_fleets(FleetIds=[fleet_id])
        fleet_size = describe_fleet_response["Fleets"][0]["TargetCapacitySpecification"]
    except Exception as e:
        logger.error("Failed to describe fleet %s - %s" % (fleet_id, e))
        return False

    # Do we need to adjust the size of the fleet?
    if target_size == fleet_size["TotalTargetCapacity"] and num_od_remove == 0:
        return True
    
    # Need to resize the fleet.
    # Is this a spot fleet on on-demand?
    od_capacity = fleet_size["OnDemandTargetCapacity"]
    if fleet_type == "spot":
        # As this is a spot fleet, if there are On-Demand instances allocated (as a fallback) which need to be shut down
        # reduce the allocation of On-Demand instances accordingly
        on_demand_target = od_capacity - num_od_remove
        fleet_command = {"TotalTargetCapacity": num_nodes, "OnDemandTargetCapacity": on_demand_target, "SpotTargetCapacity": num_nodes - on_demand_target}
    else:  # On-demand.
        # NOTE: We shouldn't have spot instances in an On-Demand fleet (as we won't fall back to spot instances if On-Demand can't be allocated!),
        # so this is a lot simplier than handling spot fleets.
        # Just set the capacity to the target capacity.
        fleet_command = {"TotalTargetCapacity": num_nodes, "OnDemandTargetCapacity": num_nodes, "SpotTargetCapacity": 0}

    logger.debug("Modifying size of fleet %s to %s" % (fleet_id, fleet_command))
    try:
        change_fleet_response = client.modify_fleet(FleetId=fleet_id, TargetCapacitySpecification=fleet_command, ExcessCapacityTerminationPolicy="no-termination")
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
        logger.warning("Unable to update hostfile %s - %s" % (hostfile, e))


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
                    {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping', 'stopped']}
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
                            # Check to make sure this node is not flagged as POWERED_DOWN. If so, it ran into a race condition where it was re-requested by Slurm at the same time
                            # it was marked as POWERED_DOWN.
                            # Mark it as UP to give Slurm the chance to use it again, or power it down.
                            if line.startswith("   State="):
                                if "IDLE" in line and "POWERED_DOWN" in line:
                                    # Mark node as UP to prevent it from becoming an orphan.
                                    try:
                                        common.update_node(instance_name, "state=RESUME")
                                        logger.warning("Node %s was set as POWERED_DOWN but is active in the daemon. Setting node to UP" % instance_name)
                                    except Exception as e:
                                        logger.error("Failed to set node %s to RESUME - %s" %(instance_name, e))

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

                # Status messages.
                logger.info("Partition %s is requesting %s active nodes" % (partition_name, num_nodes))
                logger.debug("Partition %s active nodes: %s" % (partition_name, ",".join(nodes.keys())))
                logger.info("Partition %s currently has %s Spot nodes and %s On-Demand nodes in fleet" % (partition_name, len(spot_instances), len(demand_instances)))
                logger.debug("Partition %s Spot Instances: %s" % (partition_name, ",".join(spot_instances.keys())))
                logger.debug("Partition %s On-Demand Instances: %s" % (partition_name, ",".join(demand_instances.keys())))
                logger.debug("Partition %s Instances awaiting configuration: %s" % (partition_name, ",".join(new_instances)))
                logger.info("Partition %s needs to remove %s instances from fleet, and start %s instances" % (partition_name, len(od_to_remove) + len(spot_to_remove), len(nodes_to_create)))

                # Is this fleet already configured?
                if len(od_to_remove) == 0 and len(spot_to_remove) == 0 and len(nodes_to_create) == 0 and len(new_instances) == 0:
                    logger.info("Partition %s already fully provisioned" % partition_name)
                    continue

                # Expand or shrink fleet size, if necessary
                adjust_fleet_size(client, fleet_id, num_nodes, len(od_to_remove), fleet_type)

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

                # If this is a spot fleet, check to see if we have reached spot capacity, or if it has taken an excessivly long amount
                # of time to obtain the necessary spot instances
                # If this is the case, we will fall back to requesting on-demand instances.
                if fleet_type == "spot":
                    if len(nodes_to_create) > 0 and spot_allocation_timeout(client, fleet_id, timeout_seconds=210):
                        logger.info("Attempting to fill oustanding capacity of fleet %s with On-Demand instances" % (fleet_id))
                        spot_fallback_to_demand(client, fleet_id, num_nodes)

        except TimeoutError:
            logger.warning("Failed to process partition %s: Partition is locked" % partition_name)

logger.info("Daemon completed successfully.")