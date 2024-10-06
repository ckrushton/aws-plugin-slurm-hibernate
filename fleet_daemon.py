#!/usr/bin/env python3

# Adapted from https://github.com/aws-samples/aws-plugin-for-slurm

import boto3
import botocore.config
import filelock
import os
import time
import json
import urllib.request

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


# Request new EC2 instances and assign to Slurm nodes.
def request_new_instances(client, node_name, config, instance_rank, nodegroup):

    launch_template = config["LaunchTemplate"]
    is_spot = config["PurchasingOption"] == "spot"
    interrupt_behavior = config["InteruptionBehavior"]
    overrides = config["Overrides"]
    node_allocated = False

    tag_specifications = [
        {"ResourceType": "instance",
         "Tags": [{"Key": "nodegroup","Value": nodegroup},
                  {"Key": "launchtemplate", "Value": launch_template},
                  {"Key": "Name", "Value": node_name}]
        },
        {"ResourceType": "spot-instances-request",
         "Tags": [{"Key": "nodegroup","Value": nodegroup},
                  {"Key": "launchtemplate", "Value": launch_template},
                  {"Key": "Name", "Value": node_name}]
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
        for instance_type in instance_rank:
            for subnet in config["SubnetIds"]:
                try:
                    logger.debug("Requesting spot instance of type %s in subnet %s" % (instance_type, subnet))
                    instance_response = client.run_instances(LaunchTemplate={"LaunchTemplateId" :launch_template}, InstanceType=instance_type, MinCount=1, MaxCount=1, SubnetId=subnet,
                                    InstanceMarketOptions=market_options, TagSpecifications=tag_specifications, **overrides)
                    logger.debug("Run Instance response - %s" % json.dumps(instance_response,indent=4,default=str))
                    # Did we manage to allocate an instance?
                    for instance in instance_response["Instances"]:
                        instance_id = instance["InstanceId"]
                        instance_ip = instance["PrivateIpAddress"]
                        spot_id = instance["SpotInstanceRequestId"]
                        common.update_node(node_name, "nodeaddr=%s nodehostname=%s comment=InstanceId:%s,SpotId:%s weight=%s" % (instance_ip, node_name, instance_id, spot_id, 2))
                        update_hosts_file(node_name, instance_ip)
                        node_allocated = True
                        break

                    # To not flood AWS API with requests.
                    time.sleep(0.1)

                except Exception as e:
                    logger.info("Unable to fullfill spot request for %s instances in subnet %s - %s" % (instance_type, subnet, e))

                if node_allocated:
                    break
            if node_allocated:
                break

    # If this is an on-demand fleet or we can't allocate spot instances, request on-demand instances.
    if not node_allocated:
        tag_specifications.pop(-1)  # Remove the spot instance tagging.
        for instance_type in config["Instances"]:
            for subnet in config["SubnetIds"]:
                try:
                    logger.debug("Requesting On-Demand Instance of type %s in subnet %s" % (instance_type, subnet))
                    instance_response = client.run_instances(LaunchTemplate={"LaunchTemplateId" :launch_template}, InstanceType=instance_type, MinCount=1, 
                                                             MaxCount=1, SubnetId=subnet, TagSpecifications=tag_specifications, **overrides)
                    logger.debug("Run Instance response - %s" % json.dumps(instance_response,indent=4,default=str))
                    # Did we manage to allocate nodes?
                    for instance in instance_response["Instances"]:
                        instance_id = instance["InstanceId"]
                        instance_ip = instance["PrivateIpAddress"]
                        common.update_node(node_name, "nodeaddr=%s nodehostname=%s comment=InstanceId:%s,SpotId:%s weight=%s" % (instance_ip, node_name, instance_id, "", 1))
                        update_hosts_file(node_name, instance_ip)
                        node_allocated = True
                        break

                    # To not flood AWS API with requests.
                    time.sleep(0.1)

                except Exception as e:
                    logger.info("Unable to fullfill On-Demand request for %s instance in subnet %s - %s" % (instance_type, subnet, e))

                if node_allocated:
                    break
            if node_allocated:
                break

    if not node_allocated:
        # If we can't allocate enough instances right now, that is okay, we will try again the next time the daemon is run.
        logger.warning("Unable launch instance for node %s. Will try again later" % node_name)
    else:
        return instance_response

# Compare the nodes that are currently running to those present in the fleet, and determine what changes are required.
def process_fleet_nodes(client, nodes, instances, instance_rank, spot_requests, config, nodegroup):

    seen_instances = []
    seen_spot = []

    # Process all nodes in this partition and nodegroup.
    for node_name, node_attributes in nodes.items():
        # Has this node been associated with an EC2 instance?
        logger.info("Processing node %s" % node_name)
        
        instance_id = None
        instance_id_raw = ""
        instance_attributes = {}

        if "Comment" in node_attributes:
            instance_id_raw = node_attributes["Comment"]["InstanceId"]
            if "SpotId" in node_attributes["Comment"]:
                spot_id = node_attributes["Comment"]["SpotId"]
            else:
                spot_id = ""
            seen_spot.append(spot_id)
            if instance_id_raw != "":
                instance_id = instance_id_raw
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
        elif "DRAIN" in node_states and "POWERED_DOWN" in node_states:
            logger.info("Node %s is DRAINing but POWERED_DOWN. Resetting..." % node_name)
            common.update_node(node_name, "state=IDLE")

        elif instance_id is None:
            # No instance is allocated to this node.
            if "POWERING_UP" in node_states:
                # We need to allocate an instance to this node.
                logger.info("Node %s is POWERING_UP. Allocating instance" % node_name)
                request_new_instances(client, node_name, config, instance_rank, nodegroup)
            elif "POWERED_DOWN" in node_states or "POWERING_DOWN" in node_states:
                # Node is powered down, and no instance is linked. This is the appropriate senario.
                # Ensure the node is not tagged with an instance.
                if instance_id_raw != "" and instance_id is None:
                    common.update_node(node_name, "Comment=InstanceId:,SpotId:")
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
                    common.update_node(node_name, "Comment=InstanceId:,SpotId:")
            # Instance is UP
            # If the underlying instance is hibernated, set it to DRAIN to prevent additional jobs from being allocated to this node.
            elif instance_attributes["State"]["Name"] == "stopped" and not "DRAIN" in node_states:
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
            if instance_ip != node_attributes["NodeAddr"]:
                logger.info("Node %s is assigned an IP address of %s, but linked instance %s has an IP address of %s. Updating..." % (node_name, node_attributes["NodeAddr"], instance_id, instance_ip))
                common.update_node(node_name, "nodeaddr=%s" % instance_ip)

    # Are there any instances which are not associated with a node?
    orphan_instances = {x: y for x, y in instances.items() if x not in seen_instances}
    orphan_spot = list(x for x in spot_requests if x not in seen_spot)

    return orphan_instances, orphan_spot


def terminate_instance(client, instance_id, instance_attributes):
    """
    Terminate an EC2 instance and the associated Spot request ID
    """
    if instance_id is not None:
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


def cancel_spot(client, spot_id):
    """
    Cancels a spot instance request and terminates the associated EC2 instance(if present)
    """

    instance_id = None

    try:
        response_describe = client.describe_spot_instance_requests(SpotInstanceRequestIds=[spot_id])
        for request in response_describe["SpotInstanceRequests"]:  # Should have a length of 1
            if "InstanceId" in request:
                instance_id = request["InstanceId"]
    except Exception as e:
        logger.error("Unable to describe spot request  %s - %s" % (spot_id, e))

    terminate_instance(client, instance_id, {"SpotInstanceRequestId": spot_id})


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
        {"Name": "tag:launchtemplate", 
         "Values": [launch_template],
        },
        {"Name": "tag:nodegroup",
         "Values": [nodegroup]
        },
        {"Name": "instance-state-name",
         "Values": ["pending", "running", "stopped", "stopping"]}
    ]

    # Get a list of EC2 instances using this launch template, and tagged with this nodegroup
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

    # Get a list of spot requests tagged with this nodegroup.
    filter_string.pop(-1)
    filter_string.append({"Name": "state", "Values": ["open", "active"]})
    spot_ids = []
    try:
        response_describe = client.describe_spot_instance_requests(Filters=filter_string)
        for request in response_describe["SpotInstanceRequests"]:
            spot_id = request["SpotInstanceRequestId"]
            spot_ids.append(spot_id)
    except Exception as e:
        logger.error("Unable to describe spot requests for launch template %s - %s" % (launch_template, e))

    return instance_ids, spot_ids


def get_spot_interruption_rate(region, instance_type, interrupt_url = "https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json"):
    """
    Determine the liklihood of a Spot interruption for a given instance.
    """

    # Load spot interruption frequency file from AWS.
    with urllib.request.urlopen(interrupt_url) as f:
        interrupt_json = json.load(f)

    if region not in interrupt_json["spot_advisor"]:
        logger.warning("Unable to obtain Spot Instance interruption likelihood for region %s" % region)
        return 10  # Return a super high ranking, so this is prioritized last.
    
    region_instances = interrupt_json["spot_advisor"][region]["Linux"]
    if instance_type not in region_instances:
        logger.warning("Unable to obtain Spot Instance interruption likelihood for instance type %s in region %s" % (instance_type, region))
        return 10  # Return a super high ranking, so this is prioritized last.
    
    interrupt_ranking = region_instances[instance_type]["r"]  # Lower is better (i.e. less likely to be interrupted)
    return interrupt_ranking

def get_spot_pricing(region, instance_type, pricing_url = "http://spot-price.s3.amazonaws.com/spot.js"):
    """
    Get the spot pricing for a given instance type.
    """
    # Load spot interruption frequency file from AWS.
    with urllib.request.urlopen(pricing_url) as f:
        pricing_dump = f.read().decode("utf-8")
        json_data = pricing_dump[pricing_dump.find('{'): pricing_dump.rfind('}')+1]
        pricing_json = json.loads(json_data)

    for region_data in pricing_json["config"]["regions"]:
        if region_data["region"] == region:  # We have found the correct region.
            # Find matching instance type.
            for instance_group_data in region_data["instanceTypes"]:
                for instance_data in instance_group_data["sizes"]:
                    if instance_data["size"] == instance_type:  # We have found the correct price.
                        value_data = instance_data["valueColumns"][0]  # Info for Linux is stored in column 1
                        price = value_data["prices"]["USD"]
                        if price == "N/A*":  # No pricing data availible.
                            return None
                        else:
                            return float(price)
            else:  # We did not find pricing data for this instance type.
                logger.warning("Unable to obtain Stop Instance pricing for instance type %s in region %s" % (instance_type, region))
                return None
    else:  # We did not find pricing data for this region
        logger.warning("Unable to obtain Stop Instance pricing for region %s" % region)
        return None


def get_instance_priority(instances, allocation_strategy, purchasing_option):

    if allocation_strategy == "rank" or purchasing_option == "on-demand":
        # Use the provided user order.
        logger.info("Using user-provided instance order to assign instance priority")
        instance_rank = instances
    elif allocation_strategy == "lowest-price":
        # Prioritize lowest cost instances.
        logger.info("Prioritizing lowest cost Spot Instances")
        instance_rank = sorted(instances, key=lambda x: get_spot_pricing(region, x))
    elif allocation_strategy == "capacity-optimized":
        logger.info("Prioritizing Spot Instances with the lowest likelihood of interruption")
        instance_rank = sorted(instances, key=lambda x: get_spot_interruption_rate(region, x))
    elif allocation_strategy == "price-capacity-optimized":
        logger.info("Prioritizing Spot Instances using a mix of instance pricing and capacity")
        instance_rank = sorted(instances, key=lambda x: (get_spot_interruption_rate(region, x) + 3) * get_spot_pricing(region, x))

    return instance_rank


def transplate_spot_to_od(client, node_name, instance_id, config, nodegroup_prefix):
    """
    Resume a hibernated Spot Instance using an On-Demand instance.

    1) Unmount the boot drive (and any additional drives) from the Spot Instance
    2) Disconnect the Elastic Network Interface from the Spot Instance
    3) Terminate the Spot Instance
    4) Create an On-Demand Instance using the salvaged Elastic Network Interface.
    5) Hibernate the On-Demand Instance
    6) Remove the original Volumes from the On-Demand instance.
    7) Attatch the Volumes salvaged from the Spot Instance
    8) Resume the Hibernated Spot Instance.

    """

    logger.info("Transplating Spot Instance %s to an On-Demand Instance" % (instance_id))

    # FAILSAFE: Sanity check that this instance is not running.
    instance_info = {}
    try:
        response_describe = client.describe_instances(InstanceIds=[instance_id])
        if len(response_describe["Reservations"]) > 0:
            for reservation in response_describe["Reservations"]:
                for instance in reservation["Instances"]:
                    # Is this instance running?
                    if instance["State"]["Name"] != "stopped":
                        # This instance was resumed (or terminated?) since the daemon last got information for this instance.
                        # Don't do anything.
                        logger.warning("Aborting transplant of Spot Instance %s as it is currently not hibernated!" % instance_id)
                        return None
                    instance_info = instance
    except Exception as e:
        logger.error("Unable to describe instance %s - %s" % (instance_id, e))

    # To prevent AWS from resuming the Spot Instance while we are working on it, cancel the spot request, but
    # preserve the underyling "Instance".
    cancel_spot(client, instance_id)

    # Step 1: List all volumes associated with this instance and their mountpoints, and detatch them.
    spot_volumes = {}
    for volume in instance_info["BlockDeviceMappings"]:
        volume_id = volume["VolumeId"]
        device = volume["DeviceName"]
        spot_volumes[volume_id] = device

        try:
            detatch_response = client.detatch_volume(VolumeId=volume_id, InstanceId=instance_id)
        except Exception as e:
            logger.error("Unable to detatch volume %s (%s) from Instance %s - %s" % (volume_id, device, instance_id, e))

    logger.debug("Found and detatched Volumes %s from instance %s" % (",".join(spot_volumes.keys()), instance_id))

    # Step 1 complete.
    # Step 2 & 3: Terminate the Spot Instance, but preserve the Network Interface.
    interface_args = []  # A run-instance formatted request.
    eni_ids = []
    primary_ip = None
    for interface in instance_info["NetworkInterfaces"]:
        eni_id = interface["NetworkInterfaceId"]
        network_card_index = interface["Attachment"]["NetworkCardIndex"]
        attachment_id = interface["Attachment"]["AttachmentId"]

        interface_args.append({"DeviceIndex": network_card_index, "NetworkInterfaceId": eni_id})
        eni_ids.append(eni_id)  # Only used for logging.

        if network_card_index == 0:  # Primary device.
            primary_ip = interface["PrivateIpAddress"]

        # Set this network interface to not terminate when the Instance terminates.
        try:
            modify_response = client.modify_network_interface_attribute(Attachment={"AttachmentId": attachment_id, "DeleteOnTermination": "false"}, NetworkInterfaceId=eni_id)
        except Exception as e:
            logger.error("Unable to modify network interface %s - %s" % (eni_id, e))

    logger.debug("Found (and preserving) Network Interfaces %s from instance %s" % (",".join(eni_ids), instance_id))

    # Terminate the Spot Instance
    terminate_instance(client, instance_id, {})
    logger.debug("Terminated stripped Spot Instance %s" % instance_id)

    # Step 4: Request an On-Demand Instance (the recipient) using the existing Network Interface.
    # Format the Network Interfaces 
    # NOTE: This must be the same type of instance as the Spot Instance so the boot drive is happy!
    instance_type = [instance_info["InstanceType"]]
    # Override the config to ensure we request an On-Demand instance.
    config["PurchasingOption"] = "on-demand"
    config["SubnetIds"] = [""]  # Will be specified by the Network Adapters
    config["Overrides"] = {"NetworkInterfaces": interface_args}
    recipient_info = request_new_instances(client, node_name, config, instance_type, nodegroup_prefix)
    if recipient_info is None:  # We didn't recieve a replacement instance. Transplant failed.
        logger.error("Unable to transplant instance %s as we could not request a new On-Demand instance" % instance_id)
        return None

    recipient_id = recipient_info["InstanceId"]
    logger.debug("Recieved recipient instance %s" % recipient_id)

    # Wait until this recpient is fully initialized.
    # We will ping it to ensure the OS is fully initialized.
    if primary_ip is not None:
        for i in range(0, 40):  # Check for two minutes.
            response = os.system("ping -c 1 -w2 " + primary_ip + " > /dev/null 2>&1")
            if response == 0:  # We recieved a response. Recipient is up and ready.
                break
            time.sleep(3)
    logger.debug("Recipient instance %s is online" % recipient_id)

    # Step 5: Hibernate the On-Demand instance.
    # NOTE: I am not sure if we need to hibernate this instance, or if we can transplant it from the stopped state?
    # I am just doing this to be safe.
    try:
        hibernate_response = client.stop_instances(InstanceIds=[recipient_id], Hibernate=True)
    except Exception as e:
        logger.error("Unable to hibernate On-Demand instance %s - %s" % (recipient_id, e))
        return None
    
    # Wait until the On-Demand instance is hibernated.
    recipient_hibernated = False
    for i in range(0, 40):  # Check for three minutes.
        try:
            response_describe = client.describe_instances(InstanceIds=[recipient_id])
            for reservation in response_describe["Reservations"]:
                for instance in reservation["Instances"]:
                    # Is this instance hibernated?
                    if instance["State"]["Name"] == "stopped":
                        # Hibernation complete
                        recipient_hibernated = True
                    instance_info = instance
        except Exception as e:
            logger.error("Unable to describe instance %s - %s" % (instance_id, e))
        if recipient_hibernated:
            break
        time.sleep(3)
    logger.debug("Hibernated recipient instance %s" % recipient_id)

    # Re-specify that the Network Interfaces should be deleted on instance termination.
    for interface in recipient_info["NetworkInterfaces"]:
        eni_id = interface["NetworkInterfaceId"]
        attachment_id = interface["Attachment"]["AttachmentId"]

        # Set this network interface to not terminate when the Instance terminates.
        try:
            modify_response = client.modify_network_interface_attribute(Attachment={"AttachmentId": attachment_id, "DeleteOnTermination": "true"}, NetworkInterfaceId=eni_id)
        except Exception as e:
            logger.error("Unable to modify network interface %s - %s" % (eni_id, e))

    # Step 6: Remove the original volumes from the recipient.
    # We will delete them later to give them time to fully detatch
    recipient_volumes = []
    for volume in recipient_info["BlockDeviceMappings"]:
        volume_id = volume["VolumeId"]
        recipient_volumes.append(volume_id)
        try:
            detatch_response = client.detatch_volume(VolumeId=volume_id, InstanceId=recipient_id)
        except Exception as e:
            logger.error("Unable to detatch volume %s from Instance %s - %s" % (volume_id, recipient_id, e))
    logger.debug("Removed recipient volumes %s" % ",".join(recipient_volumes))

    time.sleep(10)  # Give the drives time to fully detatch

    # Step 7: Attach the Spot volumes to the recipient instance.
    for volume_id, device in spot_volumes.items():
        try:
            client.attach_volume(Device=device, InstanceId=recipient_id, VolumeId=volume_id)
        except Exception as e:
            logger.error("Unable to attach volume %s (%s) to Instance %s - %s" % (volume_id, device, recipient_id, e))
    logger.debug("Attached Spot Instance volumes %s to recipient %s" % (",".join(spot_volumes.keys()), recipient_id))

    # Everything SHOULD be good to go!
    # Step 8: Boot the new Frankenstein'd instance
    logger.info("Finished transplanting Spot Instance %s to On-Demand instance %s" % (instance_id, recipient_id))
    logger.debug("Booting recpient %s" % recipient_id)
    try:
        client.start_instances(InstanceIds = [recipient_id])
    except Exception as e:
        logger.error("Unable to start recipient instance %s - %s" % (recipient_id, e))

    # Delete the old (donor) volumes
    for volume in recipient_info["BlockDeviceMappings"]:
        volume_id = volume["VolumeId"]
        try:
            delete_response = client.delete_volume(VolumeId=volume_id)
        except Exception as e:
            logger.error("Unable to delete volume %s - %s" % (volume_id, e))

    # (Finally) Update Slurm with the new Instance info
    common.update_node(node_name, "nodeaddr=%s nodehostname=%s comment=InstanceId:%s,SpotId:%s weight=%s" % (primary_ip, node_name, recipient_id, "", 1))

# Get a list of all nodes and associated information from Slurm.
all_node_info = scontrol_nodeinfo()
region = config["Region"]
boto3_config = botocore.config.Config(
   retries = {"max_attempts": 1,})

# Start AWS client.
client = boto3.client('ec2', region_name=region, config=boto3_config)

for partition_name, nodegroups in config["Partitions"].items():
 
    for nodegroup_name, nodegroup_atts in nodegroups.items():

        # Prevent two instances of the daemon from processing the same node group concurrently.
        nodegroup_prefix = partition_name + "-" + nodegroup_name
        lockfile = "/tmp/slurm-" + nodegroup_prefix + ".lock"
        lock = filelock.FileLock(lockfile, timeout=10)
        try:
            with lock:  # Lock partition while processing.

                # Get the list of nodes associated with this partition.
                logger.info("Examining partition: '%s' nodegroup: '%s'" % (partition_name, nodegroup_name))
                logger.debug("Obtaining Slurm node information for nodegroup")
                nodes = {x: y for x, y in all_node_info.items() if x.startswith(nodegroup_prefix) and y["Partitions"] == partition_name}  # Filter for nodes associated with this partition.

                instance_rank = get_instance_priority(nodegroup_atts["Instances"], nodegroup_atts["AllocationStrategy"], nodegroup_atts["PurchasingOption"])
                logger.debug("Instance priority for nodegroup %s: %s" % (nodegroup_prefix, " > ".join(instance_rank)))

                # Get a list of all instances created with the compute node launch template.
                launch_template = nodegroup_atts["LaunchTemplate"]
                instances, spot_requests = get_all_instances_for_nodegroup(client, launch_template, nodegroup_prefix)
                interrupt_behavior = nodegroup_atts["InteruptionBehavior"]
                logger.debug("Instances currently associated with this nodegroup: %s" % (list(instances.keys())))
                logger.debug("Spot requests currently associated with this nodegroup: %s" % (spot_requests))

                orphan_instances, orphan_spot = process_fleet_nodes(client, nodes, instances, instance_rank, spot_requests, nodegroup_atts, nodegroup_prefix)

                # Are there any extraneous instances we should clean up?
                if len(orphan_instances) > 0:
                    logger.warning("Terminating orphan instances %s " % ",".join(orphan_instances.keys()))
                    for instance_id, instance_attr in orphan_instances.items():
                        terminate_instance(client, instance_id, instance_attr)

                # Are there any orphan spot instances we should clean up?
                if len(orphan_spot) > 0:
                    logger.warning("Terminating orphan spot requests %s " % ",".join(orphan_spot))
                    for spot_id in orphan_spot:
                        cancel_spot(client, spot_id)
        except TimeoutError:
            logger.info("Unable to update process nodegroup \'%s\': Nodegroup locked" % nodegroup_prefix)

logger.info("Fleet daemon complete")
