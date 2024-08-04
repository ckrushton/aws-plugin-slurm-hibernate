#!/bin/bash -x

set -euo pipefail

# Configure ssh on the node.
cd /home/ubuntu/
sudo su - ubuntu -c 'ssh-keygen -t ed25519 -f /home/ubuntu/.ssh/id_ed25519 -N ""'
cat .ssh/id_ed25519.pub >> .ssh/authorized_keys

# Install dependencies
sudo apt update
sudo apt install --yes python3 python3-pip
sudo apt install --yes libyaml-dev libhttp-parser-dev libjwt-dev libdbus-1-dev munge libmunge-dev openssl libssl-dev libpam-dev numactl libnuma-dev hwloc libhwloc-dev lua5.4 liblua5.4-dev libreadline-dev librrd-dev libncurses-dev libibmad-dev libibumad-dev libyaml-dev libjson-c-dev libperl-dev libcurl4-openssl-dev man2html libxi-dev default-libmysqlclient-dev libhdf5-dev libnvidia-ml-dev libpmix-dev libfreeipmi-dev librdkafka-dev liblz4-dev libglib2.0-dev libgtk2.0-dev parallel
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

sudo mkdir --parents /shared
sudo chmod 777 /shared

# Disable KASLR (see https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/hibernation-disable-kaslr.html)
sudo sed -i '/^GRUB_CMDLINE_LINUX_DEFAULT/ s/"$/ nokaslr"/' /etc/default/grub.d/50-cloudimg-settings.cfg
sudo update-grub
sudo reboot