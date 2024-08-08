#!/bin/bash -x

set -euo pipefail

# URL to the Slurm tarball.
SLURM_TAR_URL=https://download.schedmd.com/slurm/slurm-23.02.2.tar.bz2
PLUGIN_GIT_URL=https://github.com/ckrushton/aws-plugin-slurm-hibernate/raw/hibernate-support/

# Configure ssh on the node.
cd /home/ubuntu/
sudo su - ubuntu -c 'ssh-keygen -t ed25519 -f /home/ubuntu/.ssh/id_ed25519 -N ""'
cat .ssh/id_ed25519.pub >> .ssh/authorized_keys

# Install dependencies
sudo apt update
sudo apt install --yes python3 python3-pip libyaml-dev libhttp-parser-dev libjwt-dev libdbus-1-dev munge libmunge-dev openssl libssl-dev libpam-dev numactl libnuma-dev hwloc libhwloc-dev lua5.4 liblua5.4-dev libreadline-dev librrd-dev libncurses-dev libibmad-dev libibumad-dev libyaml-dev libjson-c-dev libperl-dev libcurl4-openssl-dev man2html libxi-dev default-libmysqlclient-dev libhdf5-dev libnvidia-ml-dev libpmix-dev libfreeipmi-dev librdkafka-dev liblz4-dev libglib2.0-dev libgtk2.0-dev parallel nfs-kernel-server
sudo bash -c 'pip3 install boto3 awscli filelock'
sudo bash -c 'pip3 install https://s3.amazonaws.com/cloudformation-examples/aws-cfn-bootstrap-py3-latest.tar.gz'

# Configure Munge
sudo echo "billwithesciencefibillwithesciencefibillbillbill" | sudo tee /etc/munge/munge.key
sudo chown munge:munge /etc/munge/munge.key
sudo chmod 600 /etc/munge/munge.key
sudo chown -R munge /etc/munge/ /var/log/munge/
sudo chmod 0700 /etc/munge/ /var/log/munge/
sudo systemctl enable munge
sudo systemctl start munge
sleep 5

# Expand process limits
echo '*     soft    nofile 1048576' | sudo tee --append /etc/security/limits.conf

# Setup mount point
sudo mkdir --parents /shared
sudo chmod 777 /shared

# Setup and install Slurm
SCRATCH_INSTALL_DIR=/tmp/scratch/
export SLURM_HOME=/nfs/slurm
sudo mkdir -p $SCRATCH_INSTALL_DIR $SLURM_HOME
sudo chmod 777 $SCRATCH_INSTALL_DIR
sudo wget --directory-prefix $SCRATCH_INSTALL_DIR -q $SLURM_TAR_URL
sudo tar -xvf ${SCRATCH_INSTALL_DIR}/slurm-*.tar.bz2 -C ${SCRATCH_INSTALL_DIR}
SLURM_WORK_DIR="${SCRATCH_INSTALL_DIR}/slurm-*/"
cd $SLURM_WORK_DIR
sudo ./configure --prefix=$SLURM_HOME
sudo make -j 12
sudo make install -j 12
sudo make install-contrib -j 12
sleep 5
sudo mkdir -p $SLURM_HOME/etc/slurm
sudo cp ${SCRATCH_INSTALL_DIR}/slurm-*/etc/* $SLURM_HOME/etc/slurm
echo export PATH=${SLURM_HOME}/bin:'$PATH' >> /home/ubuntu/.bashrc

# Setup Slurm EC2 plugin
PLUGIN_DIR=$SLURM_HOME/etc/aws
sudo mkdir -p $PLUGIN_DIR
sudo wget --directory-prefix $PLUGIN_DIR -q ${PLUGIN_GIT_URL}common.py ${PLUGIN_GIT_URL}resume.py ${PLUGIN_GIT_URL}suspend.py ${PLUGIN_GIT_URL}generate_conf.py ${PLUGIN_GIT_URL}change_state.py ${PLUGIN_GIT_URL}fleet_daemon.py
sudo chmod +x ${PLUGIN_DIR}/*.py

# Disable KASLR (see https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/hibernation-disable-kaslr.html)
sudo sed -i '/^GRUB_CMDLINE_LINUX_DEFAULT/ s/"$/ nokaslr"/' /etc/default/grub.d/50-cloudimg-settings.cfg
sudo update-grub
sudo reboot