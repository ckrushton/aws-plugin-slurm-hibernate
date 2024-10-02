#!/usr/bin/python3

import boto3
import common


logger, config = common.get_common('generate_conf')

slurm_filename = 'slurm.conf.aws'
gres_filename = 'gres.conf.aws'

# This script generates a file to append to slurm.conf
with open(slurm_filename, 'w') as f:

    # Write Slurm configuration parameters
    for item, value in config['SlurmConf'].items():
        f.write('%s=%s\n' %(item, value))
    f.write('\n')

    for partition_name, nodegroups in config["Partitions"].items():

        partition_nodes = ()
        for nodegroup_name, nodegroup_attrs in nodegroups.items():
            if nodegroup_attrs["NumNodes"] == 0:
                node_range = "%s-%s-0" % (partition_name, nodegroup_name)
            else:
                node_range = "%s-%s-[1-%s]" % (partition_name, nodegroup_name, nodegroup_attrs["NumNodes"])

            partition_nodes += node_range,

            client = client = boto3.client('ec2', region_name=config["Region"])

            # Determine the resources in this partition automatically based on selected EC2 instances.
            cpus = []
            cores = []
            threading = []
            mem_mb = []

            for instance_type in nodegroup_attrs["Instances"]:
                # Parse resources from this instance type.
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
                hibernation_required = nodegroup_attrs["InteruptionBehavior"] == "hibernate"

                cpus.append(instance_vcpus)
                cores.append(instance_cores)
                threading.append(instance_threads_cores)
                mem_mb.append(instance_memory)

                # Sanity check that this instance supports hibernation, if it is specified.
                if hibernation_required and not instance_hibernation:
                    logger.error("Instance type %s does not support hibernation, but hibernation is specified for partition %s nodegroup %s" % (instance_type, partition_name, nodegroup_name))
                    continue

            # Write node information.
            # NOTE: To accomidate a diverse set of instances, use the common (minimum) memory and CPUs across the fleet.
            min_cpu = min(cpus)
            min_cores = cores[cpus.index(min_cpu)]
            min_threads_cores = threading[cpus.index(min_cpu)]
            min_mem = min(mem_mb)
            nodegroup_specs = "CPUs=%s Boards=1 SocketsPerBoard=1 CoresPerSocket=%s ThreadsPerCore=%s RealMemory=%s" % (min_cpu, min_cores, min_threads_cores, min_mem)
            line = 'NodeName=%s State=CLOUD %s' %(node_range, nodegroup_specs)
            f.write('%s\n' %line)
            # Finished processing nodegroup

        # Parse additional partition options (if present)
        part_options = ()
        if partition_name in config["PartitionOptions"]:
            for key, value in config["PartitionOptions"][partition_name]:
                part_options += "%s=%s" % (key, value),

        # Write a line for each partition
        line = "PartitionName=%s Nodes=%s MaxTime=INFINITE State=UP %s" %(partition_name, ",".join(partition_nodes), " ".join(part_options))
        f.write("%s\n\n" %line)
        # Done processing partition

    logger.info('Output slurm.conf file: %s' %slurm_filename)

# This script generates a file to append to gres.conf
with open(gres_filename, 'w') as g:
    for partition_name, nodegroups in config["Partitions"].items():

        for nodegroup_name, nodegroup_attrs in nodegroups.items():
            if nodegroup_attrs["NumNodes"] == 0:
                node_range = "%s-%s-0" % (partition_name, nodegroup_name)
            else:
                node_range = "%s-%s-[1-%s]" % (partition_name, nodegroup_name, nodegroup_attrs["NumNodes"])
            if "SlurmSpecifications" not in nodegroup_attrs:
                continue
            for key, value in nodegroup_attrs["SlurmSpecifications"].items():
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
                        assert False, "Invalid GRES field in %" % nodegroup_name

                    if name.upper() == "GPU":
                        qty=int(qty)
                        if qty == 1:
                            gresfilestring="File=/dev/nvidia[0]"
                        else:
                            gresfilestring="File=/dev/nvidia[0-%d]"%(int(qty) - 1)
                    else:
                        gresfilestring=""

                    line='NodeName=%s Name=%s %s %s' %(node_range, name, typestring, gresfilestring)
                    g.write('%s\n' %line)

    logger.info('Output gres.conf file: %s' %gres_filename)