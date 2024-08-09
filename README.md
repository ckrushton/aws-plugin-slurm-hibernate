# AWS Slurm plugin supporting Spot Instance hibernation

> *DISCLAIMER: This repository and plugin are provided AS-IS, and you should DEFINITELY perform small scale tests with your use case and software before implementing it!* I have tried to ensure the fleets are managed as robustly as possible with some fail-safes, but there are most likely still lingering bugs!

This plugin is based on [Amazon's AWS plugin for slurm](https://github.com/aws-samples/aws-plugin-for-slurm), but has been significantly overhauled to utilize persistant EC2 fleets, allowing Spot fleets to be created which support instance hibernation.

## When should I use this?

If you are running a workflow which contains a long-running job (for instance, human genome sequencing alignment) which cannot be easily paused or interrupted, it is normally extremely risky to use spot instances, as such a job will have to be restarted if the spot instance is interrupted. AWS recently expanded its support for [instance hibernation](https://aws.amazon.com/about-aws/whats-new/2023/10/amazon-ec2-hibernate-supports-more-operating-systems/), enabling interrupted spot instances to be hibernated (with all memory contents stored on disk) and resumed once a new spot instance becomes avaliable. You can read more about EC2 Instance hibernation [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-hibernate-overview.html).

You can read more about the requirements for Instance hibernation [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/hibernating-prerequisites.html)

## How it works
This plugin (just like the original plugin) relies on the existing Slurm power save logic (see [Power Saving Guide](https://slurm.schedmd.com/power_save.html) and [Cloud Scheduling Guide](https://slurm.schedmd.com/elastic_computing.html) in the Slurm documentation).

When the head node is first started and Slurm is configured, this plugin creates an EC2 fleet (with size 0) for each parition specified in the Slurm configuration. All nodes that Slurm may launch in AWS must be initially declared in the Slurm configuration, but their IP address and host name don't have to be specified in advance. These nodes are placed initially in a power saving mode. When work is assigned to them by the Slurm scheduler, the headnode executes the program `ResumeProgram` and passes the list of nodes to resume as argument, which marks a list of nodes to be created. After a idle period, when nodes are no longer required, the headnode executes the program `SuspendProgram` with the list of nodes to suspend as argument, marking nodes for destruction.

The plugin daemon (`fleet_daemon.py`) is installed and configured on the head node, and runs every minute to check the status of each partion. This daemon will resize each EC2 fleet based on the number of active nodes in the corresponding partition, and AWS will automatically attempt to assign new instances to a fleet if the fleet size is increased. The daemon subsequently checks each fleet for new instances and registers them with Slurm. Note that these steps are done asyncronously; i.e. the daemon will expand a fleet when it is run, but will check for new instances in subsequent runs of the daemon instead of waiting. Any nodes suspended by Slurm are terminated, as well (as a failsafe) any instances outside the requested capacity (to avoid orphan instances if the fleet size is quickly scaled up and down).

When handling Spot Fleets, the daemon checks and updates the IP address associated with a node if the underlying instance changes (for instance, a spot instance has been replaced). It will also automatically fill the fleet with On-Demand instances if Spot capacity has been exhasted.

## Plugin files

This plugin is comprised of a set of python scripts for interfacing with slurm

## Table of Contents

* [Concepts](#tc_concepts)
* [Plugin files](#tc_files)
* [Deployment with AWS CloudFormation](#tc_cloudformation)
* [Troubleshooting and Logging](#logging)
* [Appendix: Examples of `partitions.json`](#tc_partitions)

<a name="tc_concepts"/>

## Concepts

This plugin relies on the existing Slurm power save logic (see [Power Saving Guide](https://slurm.schedmd.com/power_save.html) and [Cloud Scheduling Guide](https://slurm.schedmd.com/elastic_computing.html) in the Slurm documentation).

All nodes that Slurm may launch in AWS must be initially declared in the Slurm configuration, but their IP address and host name don't have to be specified in advance. These nodes are placed initially in a power saving mode. When work is assigned to them by the scheduler, the headnode executes the program `ResumeProgram` and passes the list of nodes to resume as argument. The program launches a new EC2 instance for each node, and updates the IP address and the host name in Slurm. After a idle period, when nodes are no longer required, the headnode executes the program `SuspendProgram` with the list of nodes to suspend as argument. The program terminates the associated EC2 instances, and the nodes are placed in power mode saving again.

This plugin consists of the programs that Slurm executes when nodes are restored in normal operation (`ResumeProgram`) or placed in power mode saving (`SuspendProgram`). It relies upon EC2 Fleet to launch instances.

<a name="tc_files"/>

## Plugin files

The plugin is composed of 6 Python files, a shell script, and a CloudFormation configuration YAML file. This section details the purpose and format of each file.

### `ami_configuration_script.sh`

This script installs all the dependencies required for Slurm, AWS-specific dependencies (awscli, boto3), and essential plugin files (resume.py, common.py, fleet_daemon.py). Installs Slurm, but does not configure or start the Slurm daemon.

### `generate_conf.py`

This script is used to create the EC2 fleets associated with each partition, and generates associated Slurm configuration files. The resulting `slurm.conf.aws` must be appended to `slurm.conf`.

- This script parses `partitions.json` and automatically determines the vCPU and memory specifications of the instances requested for each fleet. The fleet specifications are set as the minimum value for vCPU and memory across all instance types requested for that fleet.
    - For instance, if `c6i.12xlarge` instances (48 vCPUs, 96GB memory) and `r6i.8xlarge` instances (32 vCPUs, 256GB memory) are requested in the same partition, it will be configured as having 32 CPUs and 96GB of memory
- The EC2 fleet ID associated with each partition is formatted as a comment line in `slurm.conf.aws`
    - #EC2_FLEET {partition} {nodegroup} {EC2 fleet ID}

### `common.py`

This script contains variables and functions that are used by more than one Python scripts.

### `resume.py`

This script is the `ResumeProgram` program executed by Slurm to restore nodes in normal operation:

- It retrieves the list of nodes to resume, and for each partition and node group, it appends the partition-associated text file located under `/nfs/slurm/etc/aws/partitions/`
    - These files list all active nodes associated with each partition.

You can manually try the resume program by running `/fullpath/resume.py (partition_name)-(nodegroup_name)-(id)` such as  `/fullpath/resume.py partition-nodegroup-0`.

### `suspend.py`

This script is the `SuspendProgram` executed by Slurm to place nodes in power saving mode:

- It retrieves the list of nodes to suspend, and for each partition and node group, it removes that node from partition-associated text file located under `/nfs/slurm/etc/aws/partitions/`

You can manually try the suspend program by running `/fullpath/suspend.py (partition_name)-(nodegroup_name)-(id)` such as  `/fullpath/suspend.py partition-nodegroup-0`.

### `change_state.py`

This script is executed every minute by `cron` to change the state of nodes that are stuck in a transient or undesired state. For example, compute nodes that failed to respond within `ResumeTimeout` seconds are placed in a `DOWN*` state and the state must be set to `POWER_DOWN`.

### `fleet_daemon.py`

The essential linchpin of the plugin; used to manage and scale EC2 fleets, assign and register instances with slurm, and handle spot and EC2. This is run as a CRON job every minute on the head node.

- For each partition and nodegroup registered with Slurm:
    - Parses the EC2 fleet ID from `slurm.conf`
    - Parses the active node partition file under `/nfs/slurm/etc/aws/partitions/` to obtain a list of instances requested by Slurm
    - Obtains a list of all instances assigned to this EC2 fleet, and their associated Slurm nodename
    - If the number of nodes has been updated, the size of the EC2 fleet is updated accordingly.
    - Any instances associated with suspended nodes are terminated.
    - Any additional fleet instances are assigned and registered with Slurm.
       - The IP address of the new instances are added to `/etc/hosts`
    - Any extraneous instances associated with a fleet are terminated.
       - This can occur of a fleet is scaled up, then scaled back down quickly before the new instances are assigned and registered with Slurm.
       - Prevents zombie instances from being generated.

- Additionally, Spot fleets where a `spotInstanceCountLimitExceeded` or `spotFleetRequestConfigurationInvalid` error is found in the fleet history, or where no new nodes have been spun up in 4 minutes, fall back and fill outstanding capacity with On-Demand instances.

### `template.yaml`

The CloudFormation template used to initialize the cluster head node, configure Slurm, and manage fleet nodes. This is explained in more detail below.

<a name="tc_cloudformation"/>

## CloudFormation template

This template is used to create and configure the head node (and cluster nodes), perform final slurm configuration, and setup instances within the assigned VPC and subset. This template requires the following user parameters:

1. An existing VPC and subset for the cluster to reside in.
2. An SSH keypair for connecting to the instance(s).
3. The AMI to be used when creating cluster instances or the head node instance.

### AWS AMI creation.

The AWS AMI used by head node and compute nodes comes pre-installed with Slurm and associated dependencies, this plugin, NFS, and other key dependencies. This also [disables KASLR](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/hibernation-disable-kaslr.html). This AMI can be created from scratch using the following:

1. Start an EC2 instance with the following specifications.

- AMI: Ubuntu 22.04 LTS
- Instance type: t3.2xlarge (or similar)
- Key pair: SSH key of your choice
- Storage: 
    - Size: 1000 GiB
    - Volume type: gp3
    - Encrypted: Encrypted
    - KMS key: aws/ebs
- Advanced details:
    - Stop - Hibernate behavior: Enable
    - Metadata version: V2 only

2. Log onto the EC2 instance

Use the SSH key you selected above, an obtain the server IP address from EC2

```
ssh -i {your SSH key here} ubuntu@{Intance IP address here}
```

3. Run the AMI configuration script.

```
wget https://raw.githubusercontent.com/ckrushton/aws-plugin-slurm-hibernate/hibernate-support/ami_configuration_script.sh
bash ami_configuration_script.sh
```

4. Install any additional software or make any additional changes as desired.

### How it works

The head node recipe performs the following:

1. If the instance contains directly attatched storage, a filesystem is created and it is mounted under /shared/
- Note that multiple drives are stripped together into a single filesystem as RAID0

2. `/nfs/` and `/shared/` are shared via NFS
- If no instance storage is avalible, `/shared/` will correspond to the EBS storage attatched to the head node.

3. The Slurm configuration files are created.
- See below for a more detailed description.

4. This plugin and the associated daemon are started and configured

5. The Slurm Control Daemon is started.

The cluster node recipe performs the following:

1. Mounts `/nfs/` and `/shared/` from the head node

2. Starts the Slurm Daemon.

### Cleanup

A lambda function automatically deletes all fleets (and terminates instances) associated with this Stack upon stack deletion.

<a name="tc_manual"/>

### Configuration files

Note these files are embeded directly in the CloudFormation template to maximize portability and ease-of-use when making modifications to Slurm or the plugin.

#### `config.json`

This JSON file specifies the plugin and Slurm configuration parameters.

```
{
   "LogLevel": "STRING",
   "NodePartitionFolder": "STRING",
   "LogFileName": "STRING",
   "SlurmBinPath": "STRING",
   "SlurmConf": {
      "PrivateData": "STRING",
      "ResumeProgram": "STRING",
      "SuspendProgram": "STRING",
      "ResumeRate": INT,
      "SuspendRate": INT,
      "ResumeTimeout": INT,
      "SuspendTime": INT,
      "TreeWidth": INT
      ...
   }
}
```

* `LogLevel`: Logging level. Possible values are `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`. Default is `DEBUG`.
* `NodePartitionFolder`: Folder where the text files containing active nodes for each partition and nodegroup are stored. Example: `/nfs/slurm/etc/aws/partitions/`
* `LogFileName`: Full path to the log file location. Default is `PLUGIN_PATH\aws_plugin.log`.
* `SlurmBinPath`: Full path to the folder that contains Slurm binaries like `scontrol` or `sinfo`. Example: `/slurm/bin`.
* `SlurmConf`: These attributes are used by `generate_conf.py` to generate the content that must be appended to the Slurm configuration file. You must specify at least the following attributes:
   * `PrivateData`: Must be equal to `CLOUD` such that EC2 compute nodes that are idle are returned by Slurm command outputs such as `sinfo`.
   * `ResumeProgram`: Full path to the location of `resume.py`. Example: `/slurm/etc/aws/resume.py`.
   * `SuspendProgram`: Full path to the location of `suspend.py`. Example: `/slurm/etc/aws/suspend.py`.
   * `ResumeRate`: Maximum number of EC2 instances that Slurm can launch per minute. You might reach EC2 request rate limits if this value is too high. Recommended value is `100`.
   * `SuspendRate`: Maximum number of EC2 instances that Slurm can terminate per minute. You might reach EC2 request rate limits if this value is too high. Recommended value is `100`.
   * `ResumeTimeout`: Maximum time permitted (in seconds) between when a node resume request is issued and when the node is actually available for use. You should take into consideration the time it takes to launch an instance and to run your bootstrap scripts when defining this value.
   * `SuspendTime`: Nodes becomes eligible for power saving mode after being idle or down for this number of seconds. As per the Slurm documentation, it is recommended that the value of `SuspendTime` be at least as large as the sum of `SuspendTimeout` (default is 30 seconds) plus `ResumeTimeout`.
   * `TreeWidth`. Refer to the Slurm documentation. Recommended value is `60000`.

Example:

```
{
   "LogLevel": "INFO",
   "NodePartitionFolder": "/nfs/slurm/etc/aws/partitions/",
   "LogFileName": "/var/log/slurm/aws.log",
   "SlurmBinPath": "/slurm/bin",
   "SlurmConf": {
      "PrivateData": "CLOUD",
      "ResumeProgram": "/slurm/etc/aws/resume.py",
      "SuspendProgram": "/slurm/etc/aws/suspend.py",
      "ResumeRate": 100,
      "SuspendRate": 100,
      "ResumeTimeout": 300,
      "SuspendTime": 350,
      "TreeWidth": 60000
   }
}
```

#### `partitions.json`

This JSON file specifies the groups of nodes and associated partitions that Slurm can launch in AWS.

```
{
   "Partitions": [
      {
         "PartitionName": "STRING",
         "NodeGroups": [
            {
               "NodeGroupName": "STRING",
               "MaxNodes": INT,
               "Region": "STRING",
               "ProfileName": "STRING",
               "PurchasingOption": "spot|on-demand",
               "OnDemandOptions": DICT,
               "SpotOptions": DICT,
               "LaunchTemplateSpecification": DICT,
               "LaunchTemplateOverrides": ARRAY,
               "SubnetIds": [ "STRING" ],
               "Tags": [
                  {
                     "Key": "STRING",
                     "Value": "STRING"
                  }
               ]
            },
            ...
         ],
         "PartitionOptions": {
            "Option1": "STRING",
            "Option2": "STRING"
         }
      },
      ...
   ]
}
```

* `Partitions`: List of partitions
   * `PartitionName`: Name of the partition. Must match the pattern `^[a-zA-Z0-9]+$`.
   * `NodeGroups`: List of node groups for this partition. A node group is a set of nodes that share the same specifications.
      * `NodeGroupName`: Name of the node group. Must match the pattern `^[a-zA-Z0-9]+$`.
      * `MaxNodes`: Maximum number of nodes that Slurm can launch for this node group. For each node group, `generate_conf.py` will issue a line with `NodeName=[partition_name]-[nodegroup_name]-[0-(max_nodes-1)]`
      * `Region`: Name of the AWS region where to launch EC2 instances for this node group. Example: `us-east-1`.
      * [OPTIONAL] `ProfileName`: Name of the AWS CLI profile to use to authenticate AWS requests. If you don't specify a profile name, it uses the default profile name of EC2 metadata credentials.
      * `PurchasingOption`: Possible values are `spot` or `on-demand`.
      * `OnDemandOptions`: Must be included if `PurchasingOption` is equal to `on-demand` and filled in the same way than the object of the same name in the [EC2 CreateFleet API](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.create_fleet).
      * `SpotOptions`: Must be included if `PurchasingOption` is equal to `spot` and filled in the same way than the object of the same name in the [EC2 CreateFleet API](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.create_fleet).
      * `LaunchTemplateSpecification`: Must be filled in the same way than the object of the same name in the [EC2 CreateFleet API](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.create_fleet).
      * `LaunchTemplateOverrides`: Must be filled in the same way then the object of the same name in the [EC2 CreateFleet API](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html#EC2.Client.create_fleet). Do not populate the field `SubnetId` in template overrides.
      * `SubnetIds`: List of subnets where EC2 instances can be launched for this node group. If you provide multiple subnets, they must be in different availability zones, or the `CreateFleet` request may return the error message "The fleet configuration contains duplicate instance pools".
      * `Tags`: List of tags applied to the EC2 instances launched for this node group.
        * A tag `Name` is automatically added at launch, whose value is the name of the node `[partition_name]-[nodegroup_name]-[id]`. You should not delete or override this tag, because the script `suspend.py` uses it to find which instance is associated with the node to suspend.
        * You use the sequence `{ip_address}` in the value of tag, it will be replaced with the IP address. Similarly, `{node_name}` will be replaced with the name of the node, `{hostname}` with the EC2 hostname.
   * `PartitionOptions`: List of Slurm configuration attributes for the partition (optional).

Refer to the section **Examples of `partitions.json`** for examples of file content.

<a name="logging"/>

## Logging and troubleshooting

The following logs may be useful when troubleshooting issues with the head node, cluster nodes, Slurm, or the plugin.

 - `/var/log/cloud-init-output.log`: Contains all logs related to starting up the node.
 - `/var/log/slurm_plugin.log`: Contains logs related to `resume.py`, `suspend.py` and `fleet_daemon.py`.
 - `/var/log/slurmctld.log`: Contains logs from the Slurm control daemon.
 - `/var/log/slurmd.log`: Contains logs from the Slurm daemon. Only availible on cluster nodes.

<a name="tc_partitions"/>

## Appendix: Examples of `partitions.json`

### Example 1

Single `aws` partition with 2 node groups:

* One node group `ondemand` with up to 10 nodes that is used in priority (Slurm `Weight=1`)
* Another node group `spot` with up to 100 nodes and a lower priority (Slurm `Weight=2`). The scheduler will automatically launch and allocate jobs to the Spot instances when all the on-demand nodes are running and busy.

```
{
   "Partitions": [
      {
         "PartitionName": "aws",
         "NodeGroups": [
            {
               "NodeGroupName": "ondemand",
               "MaxNodes": 10,
               "Region": "us-east-1",
               "PurchasingOption": "on-demand",
               "OnDemandOptions": {
                   "AllocationStrategy": "lowest-price"
               },
               "LaunchTemplateSpecification": {
                  "LaunchTemplateName": "template-name",
                  "Version": "$Latest"
               },
               "LaunchTemplateOverrides": [
                  {
                     "InstanceType": "c5.xlarge"
                  }
               ],
               "SubnetIds": [
                  "subnet-11111111"
               ],
               "Tags": [
                  {
                     "Key": "NodeGroup",
                     "Value": "ondemand"
                  }
               ]
            },
            {
               "NodeGroupName": "spot",
               "MaxNodes": 100,
               "Region": "us-east-1",
               "PurchasingOption": "spot",
               "OnDemandOptions": {
                   "AllocationStrategy": "lowest-price"
               },
               "LaunchTemplateSpecification": {
                  "LaunchTemplateName": "template-name",
                  "Version": "$Latest"
               },
               "LaunchTemplateOverrides": [
                  {
                     "InstanceType": "c5.xlarge"
                  }
               ],
               "SubnetIds": [
                  "subnet-11111111"
               ],
               "Tags": [
                  {
                     "Key": "NodeGroup",
                     "Value": "spot"
                  }
               ]
            }
         ],
         "PartitionOptions": {
            "Default": "yes",
            "TRESBillingWeights": "cpu=4"
         }
      }
   ]
}
```

### Example 2

Single `aws` partition with 3 node groups:

* One node group `spot4vCPU` used by default (lowest Slurm weight) that launches Spot instances with c5.large or c4.large across two subnets in two different availability zones, with the lowest price strategy.
* Two node groups `spot4vCPUa` or `spot4vCPUb` that can be used by specifying the feature `us-east-1a` or `us-east-1b` to run a job with all nodes in the same availability zone.

```
{
   "Partitions": [
      {
         "PartitionName": "aws",
         "NodeGroups": [
            {
               "NodeGroupName": "spot4vCPU",
               "MaxNodes": 100,
               "Region": "us-east-1",
               "PurchasingOption": "spot",
               "OnDemandOptions": {
                   "AllocationStrategy": "lowest-price"
               },
               "LaunchTemplateSpecification": {
                  "LaunchTemplateName": "template-name",
                  "Version": "$Latest"
               },
               "LaunchTemplateOverrides": [
                  {
                     "InstanceType": "c5.xlarge"
                  },
                  {
                     "InstanceType": "c4.xlarge"
                  }
               ],
               "SubnetIds": [
                  "subnet-11111111"
               ]
            },
            {
               "NodeGroupName": "spot4vCPUa",
               "MaxNodes": 100,
               "Region": "us-east-1",
               "PurchasingOption": "spot",
               "OnDemandOptions": {
                   "AllocationStrategy": "lowest-price"
               },
               "LaunchTemplateSpecification": {
                  "LaunchTemplateName": "template-name",
                  "Version": "$Latest"
               },
               "LaunchTemplateOverrides": [
                  {
                     "InstanceType": "c5.xlarge"
                  },
                  {
                     "InstanceType": "c4.xlarge"
                  }
               ],
               "SubnetIds": [
                  "subnet-11111111"
               ]
            },
            {
               "NodeGroupName": "spot4vCPUb",
               "MaxNodes": 100,
               "Region": "us-east-1",
               "PurchasingOption": "spot",
               "OnDemandOptions": {
                   "AllocationStrategy": "lowest-price"
               },
               "LaunchTemplateSpecification": {
                  "LaunchTemplateName": "template-name",
                  "Version": "$Latest"
               },
               "LaunchTemplateOverrides": [
                  {
                     "InstanceType": "c5.xlarge"
                  },
                  {
                     "InstanceType": "c4.xlarge"
                  }
               ],
               "SubnetIds": [
                  "subnet-22222222"
               ]
            }
         ]
      }
   ]
}
```

### Example 3

Two partitions `aws` and `awsspot` with one node group in each. It uses Slurm access permissions to allow users in the "standard" account to use only Spot instances, and "VIP" account users to use Spot and On-demand instances, but weights the on-demand instances more heavily for accounting purposes.

```
{
   "Partitions": [
      {
         "PartitionName": "aws",
         "NodeGroups": [
            {
               "NodeGroupName": "node",
               "MaxNodes": 100,
               "Region": "us-east-1",
               "PurchasingOption": "on-demand",
               "OnDemandOptions": {
                   "AllocationStrategy": "lowest-price"
               },
               "LaunchTemplateSpecification": {
                  "LaunchTemplateName": "template-name",
                  "Version": "$Latest"
               },
               "LaunchTemplateOverrides": [
                  {
                     "InstanceType": "c5.xlarge"
                  },
                  {
                     "InstanceType": "c4.xlarge"
                  }
               ],
               "SubnetIds": [
                  "subnet-11111111"
               ]
            }
         ],
         "PartitionOptions": {
            "TRESBillingWeights": "cpu=30",
            "AllowAccounts": "standard,VIP"
         }
      },
      {
         "PartitionName": "awsspot",
         "NodeGroups": [
            {
               "NodeGroupName": "node",
               "MaxNodes": 100,
               "Region": "us-east-1",
               "PurchasingOption": "spot",
               "SpotOptions": {
                   "AllocationStrategy": "lowest-price"
               },
               "LaunchTemplateSpecification": {
                  "LaunchTemplateName": "template-name",
                  "Version": "$Latest"
               },
               "LaunchTemplateOverrides": [
                  {
                     "InstanceType": "c5.xlarge"
                  },
                  {
                     "InstanceType": "c4.xlarge"
                  }
               ],
               "SubnetIds": [
                  "subnet-11111111"
               ]
            }
         ],
         "PartitionOptions": {
            "TRESBillingWeights": "cpu=10",
            "AllowAccounts": "standard"
         }
      }
   ]
}
```
