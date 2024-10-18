#!/usr/bin/env python
import json
import logging
import os
import subprocess
import sys


dir_path = os.path.dirname(os.path.realpath(__file__))  # Folder where resides the Python files

logger = None  # Global variable for the logging.Logger object
config = None  # Global variable for the config parameters

# Create and return a logging.Logger object
# - scriptname: name of the module
# - levelname: log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
# - filename: location of the log file
def get_logger(scriptname, levelname, filename):

    logger = logging.getLogger(scriptname)

    # Update log level
    log_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    logger.setLevel(log_levels.get(levelname, logging.DEBUG))

    # Create a console handler
    sh = logging.StreamHandler()
    sh_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    sh.setFormatter(sh_formatter)
    logger.addHandler(sh)

    # Create a file handler
    fh = logging.FileHandler(filename)
    fh_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(fh_formatter)
    logger.addHandler(fh)

    return logger


def validate_config(config):

    assert "LogLevel" in config
    assert config["LogLevel"] in ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"), "Invalid log level specified"
    assert "LogFileName" in config
    assert "SlurmBinPath" in config
    assert "Region" in config
    assert "SlurmConf" in config
    
    assert "Partitions" in config
    assert "PartitionOptions" in config

    # Check partition formatting.
    for partition_name, nodegroups in config["Partitions"].items():
        # Check nodegroups in this partition.
        for nodegroup_name, nodegroup_attributes in nodegroups.items():
            assert "NumNodes" in nodegroup_attributes
            
            assert "PurchasingOption" in nodegroup_attributes
            assert nodegroup_attributes["PurchasingOption"] in ("spot", "on-demand")

            assert "AllocationStrategy" in nodegroup_attributes
            assert nodegroup_attributes["AllocationStrategy"] in ("rank", "lowest-price", "capacity-optimized", "price-capacity-optimized")
            
            assert "InteruptionBehavior" in nodegroup_attributes
            assert nodegroup_attributes["InteruptionBehavior"] in ("terminate", "stop", "hibernate")

            # Persistant allocations can't use a terminate interrupt strategy.
            if nodegroup_attributes["PurchasingOption"] == "spot":
                assert nodegroup_attributes["InteruptionBehavior"] != "terminate"

            assert "LaunchTemplate" in nodegroup_attributes
            assert "SubnetIds" in nodegroup_attributes

            assert "Instances" in nodegroup_attributes


# Create and return logger, config, and partitions variables
def get_common(scriptname):

    global logger
    global config

    # Load configuration parameters from ./config.json and merge with default values
    try:
        config_filename = '%s/config.json' %dir_path
        with open(config_filename, 'r') as f:
            config = json.load(f)
    except Exception as e:
        config = {'JsonLoadError': str(e)}

    with open(config_filename, 'r') as f:
        config = json.load(f)

    # Make sure that SlurmBinPath ends with a /
    if  not config['SlurmBinPath'].endswith('/'):
        config['SlurmBinPath'] += '/'

    # Create a logger
    logger = get_logger(scriptname, config['LogLevel'], config['LogFileName'])

    # Validate the structure of config.json
    if 'JsonLoadError' in config:
        logger.critical('Failed to load %s - %s' %(config['LogFileName'], config['JsonLoadError']))
        sys.exit(1)
    try:
        validate_config(config)
    except Exception as e:
        logger.critical('File config.json is invalid - %s' %e)
        raise e

    return logger, config


# Use 'scontrol update node' to update nodes
def update_node(node_name, parameters):

    parameters_split = parameters.split(' ')
    arguments = ['update', 'nodename=%s' %node_name] + parameters_split
    run_scommand('scontrol', arguments)


# Run scontrol and return output
# - command: name of the command such as scontrol
# - arguments: array
def run_scommand(command, arguments):

    scommand_path = '%s%s' %(config['SlurmBinPath'], command)
    cmd = [scommand_path] + arguments
    logger.debug('Command %s: %s' %(command, ' '.join(cmd)))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    lines = proc.communicate()[0].splitlines()
    return [line.decode() for line in lines]
