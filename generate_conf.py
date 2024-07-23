#!/usr/bin/python3

import copy
import os
import json
import sys

import common


logger, config, partitions = common.get_common('generate_conf')

slurm_filename = 'slurm.conf.aws'
gres_filename = 'gres.conf.aws'

# We will tag all EC2 fleets with this StackID, to ensure they are cleaned up
stack_name = sys.argv[1]

# This script generates a file to append to slurm.conf
with open(slurm_filename, 'w') as f:

    # Write Slurm configuration parameters
    for item, value in config['SlurmConf'].items():
        f.write('%s=%s\n' %(item, value))
    f.write('\n')

    for partition in partitions:
        partition_nodes = ()
        partition_name = partition['PartitionName']

        for nodegroup in partition['NodeGroups']:
            nodes = common.get_node_range(partition, nodegroup)
            partition_nodes += nodes,
            nodegroup_name = nodegroup["NodeGroupName"]

            # Create a file which will contain a list of all nodes slurm is requesting for this partition.
            nodegroup_folder = config["NodePartitionFolder"]
            nodegroup_file = nodegroup_folder + os.path.sep + "_".join([partition_name, nodegroup_name, "paritions.txt"])
            with open(nodegroup_file, "w"):
                pass

            # Create an EC2 fleet for this partition.
            # Create the config.
            request_fleet = {
                'LaunchTemplateConfigs': [
                    {
                        'LaunchTemplateSpecification': nodegroup['LaunchTemplateSpecification'],
                        'Overrides': []
                    }
                ],
                'TargetCapacitySpecification': {
                    'TotalTargetCapacity': 0,
                    'OnDemandTargetCapacity': 0,
                    'SpotTargetCapacity': 0,
                    'DefaultTargetCapacityType': nodegroup['PurchasingOption']
                },
                'Type': 'maintain',
                'TagSpecifications': [
                    {
                        "ResourceType": "fleet",
                        "Tags": [
                            {
                                "Key": "StackID",
                                "Value": stack_name
                            }
                        ]
                    }
                ]
            }

            # Populate spot options.
            hibernation_required = False
            if 'SpotOptions' in nodegroup:
                request_fleet['SpotOptions'] = nodegroup['SpotOptions']
                hibernation_required =  request_fleet['SpotOptions']['InstanceInterruptionBehavior'] == 'hibernate'

            if nodegroup["PurchasingOption"] == "on-demand" and hibernation_required:
                logger.warning("Hibernation cannot be specified for On-Demand fleet. Setting to 'terminate'")
                request_fleet['SpotOptions']['InstanceInterruptionBehavior'] = 'terminate'

            # Populate on-demand options
            if 'OnDemandOptions' in nodegroup:
                request_fleet['OnDemandOptions'] = nodegroup['OnDemandOptions']

            # Create an EC2 fleet.
            client = common.get_ec2_client(nodegroup)

            # Determine the resources in this partition automatically based on selected EC2 instances.
            cpus = []
            cores = []
            threading = []
            mem_mb = []

            # Populate launch configuration overrides. Duplicate overrides for each subnet.
            for override in nodegroup['LaunchTemplateOverrides']:
                # Parse resources from this instance type.
                instance_type = override["InstanceType"]
                try:
                    response_instances = client.describe_instance_types(InstanceTypes=[instance_type])
                except Exception as e:
                    logger.error("Cannot describe EC2 instances in region: %s" % e)
                    continue

                instance_vcpus = response_instances["InstanceTypes"][0]["VCpuInfo"]["DefaultVCpus"]
                instance_cores = response_instances["InstanceTypes"][0]["VCpuInfo"]["DefaultCores"]
                instance_threads_cores = response_instances["InstanceTypes"][0]["VCpuInfo"]["DefaultThreadsPerCore"]
                instance_memory = response_instances["InstanceTypes"][0]["MemoryInfo"]["SizeInMiB"]
                instance_hibernation = response_instances["InstanceTypes"][0]["HibernationSupported"]

                cpus.append(instance_vcpus)
                cores.append(instance_cores)
                threading.append(instance_threads_cores)
                mem_mb.append(instance_memory)

                # Sanity check that this instance supports hibernation, if it is specified.
                if hibernation_required and not instance_hibernation:
                    logger.error("Instance type %s does not support hibernation, but hibernation is specified for partition %s nodegroup %s" % (instance_type, partition_name, nodegroup_name))
                    continue

                for subnet in nodegroup['SubnetIds']:
                    override_copy = copy.deepcopy(override)
                    override_copy['SubnetId'] = subnet
                    override_copy['WeightedCapacity'] = 1
                    request_fleet['LaunchTemplateConfigs'][0]['Overrides'].append(override_copy)

            try:
                logger.debug('EC2 CreateFleet request: %s' %json.dumps(request_fleet, indent=4))
                response_fleet = client.create_fleet(**request_fleet)
                logger.debug('EC2 CreateFleet response: %s' %json.dumps(response_fleet, indent=4))
            except Exception as e:
                logger.error('Failed to configure fleet for partition=%s and nodegroup=%s - %s' %(partition_name, nodegroup_name, e))
                continue

            # Get the name of this persistant fleet.
            fleet_id = response_fleet['FleetId']

            # Write a line for each node group.
            # Write a comment line with the name of this parition, and the fleet ID.
            line = '#EC2_FLEET %s %s %s' % (partition_name, nodegroup_name, fleet_id)
            f.write('%s\n' %line)
            # Write node information.
            # NOTE: To accomidate a diverse set of instances, use the common (minimum) memory and CPUs across the fleet.
            min_cpu = min(cpus)
            min_cores = cores[cpus.index(min_cpu)]
            min_threads_cores = threading[cpus.index(min_cpu)]
            min_mem = min(mem_mb)
            nodegroup_specs = "CPUs=%s Boards=1 SocketsPerBoard=1 CoresPerSocket=%s ThreadsPerCore=%s RealMemory=%s" % (min_cpu, min_cores, min_threads_cores, min_mem)
            line = 'NodeName=%s State=CLOUD %s' %(nodes, nodegroup_specs)
            f.write('%s\n' %line)

        part_options = ()
        if 'PartitionOptions' in partition:
            for key, value in partition['PartitionOptions'].items():
                part_options += '%s=%s' %(key, value),

        # Write a line for each partition
        line = 'PartitionName=%s Nodes=%s MaxTime=INFINITE State=UP %s' %(partition_name, ','.join(partition_nodes), ' '.join(part_options))
        f.write('%s\n\n' %line)

    logger.info('Output slurm.conf file: %s' %slurm_filename)

# This script generates a file to append to gres.conf
with open(gres_filename, 'w') as g:
    for partition in partitions:

        for nodegroup in partition['NodeGroups']:
            nodes = common.get_node_range(partition, nodegroup)
            if "SlurmSpecifications" not in nodegroup:
                continue
            for key, value in nodegroup['SlurmSpecifications'].items():
                if key.upper() == "GRES":

                    # Write a line for each node group with Gres
                    fields=value.split(':')
                    if len(fields) == 2:
                        name=fields[0]
                        qty=fields[1]
                        typestring=""
                    elif len(fields) == 3:
                        name=fields[0]
                        typestring="Type=%s" % fields[1]
                        qty=fields[2]
                    else:
                        assert False, "Invalid GRES field in %" % nodegroup

                    if name.upper() == "GPU":
                        qty=int(qty)
                        if qty == 1:
                            gresfilestring="File=/dev/nvidia[0]"
                        else:
                            gresfilestring="File=/dev/nvidia[0-%d]"%(int(qty) - 1)
                    else:
                        gresfilestring=""

                    line='NodeName=%s Name=%s %s %s' %(nodes, name, typestring, gresfilestring)
                    g.write('%s\n' %line)

    logger.info('Output gres.conf file: %s' %gres_filename)