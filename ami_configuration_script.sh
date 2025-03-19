#!/bin/bash -x

set -euo pipefail

# URL to the Slurm tarball.
SLURM_TAR_URL=https://download.schedmd.com/slurm/slurm-23.02.2.tar.bz2
PLUGIN_GIT_URL=https://github.com/ckrushton/aws-plugin-slurm-hibernate/raw/hibernate-support-v3/

# Configure ssh on the node.
cd /home/ubuntu/
sudo su - ubuntu -c 'ssh-keygen -t ed25519 -f /home/ubuntu/.ssh/id_ed25519 -N ""'
cat .ssh/id_ed25519.pub >> .ssh/authorized_keys

# Install dependencies
sudo apt update
sudo apt install --yes python3 python3-pip libyaml-dev libhttp-parser-dev libjwt-dev libdbus-1-dev munge libmunge-dev openssl libssl-dev libpam-dev numactl libnuma-dev hwloc libhwloc-dev lua5.4 liblua5.4-dev libreadline-dev librrd-dev libncurses-dev libibmad-dev libibumad-dev libyaml-dev libjson-c-dev libperl-dev libcurl4-openssl-dev man2html libxi-dev default-libmysqlclient-dev libhdf5-dev libnvidia-ml-dev libpmix-dev libfreeipmi-dev librdkafka-dev liblz4-dev libglib2.0-dev libgtk2.0-dev parallel nfs-kernel-server fio
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
export SLURM_HOME=/etc/slurm
export SLURM_NFS_HOME=/nfs/slurm
sudo mkdir -p $SCRATCH_INSTALL_DIR $SLURM_HOME $SLURM_NFS_HOME
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
PLUGIN_DIR=$SLURM_NFS_HOME/etc/aws
sudo mkdir -p $PLUGIN_DIR
sudo wget --directory-prefix $PLUGIN_DIR -q ${PLUGIN_GIT_URL}common.py ${PLUGIN_GIT_URL}resume.py ${PLUGIN_GIT_URL}suspend.py ${PLUGIN_GIT_URL}generate_conf.py ${PLUGIN_GIT_URL}fleet_daemon.py
sudo chmod +x ${PLUGIN_DIR}/*.py

# Hibernation add-in script to suspend Slurm jobs before hibernating a node.
cat > /home/ubuntu/slurm_suspend_jobs <<EOF
#!/bin/sh
set -e

if [ "\$2" = "hibernate" ] || [ "\$2" = "hybrid-sleep" ]; then
    jobsfile="/lib/ec2-hibinit-agent/slurm_jobs_suspended.txt"
    case "\$1" in
        pre)
            # To prevent any oddities with slurm jobs which are run over the network (NFS), suspend those jobs prior to hibernation.
            $SLURM_HOME/bin/squeue --states RUNNING -w \$(bash $SLURM_HOME/etc/aws/get_nodename) --format "%i" | grep -v JOBID > \$jobsfile
            while read job_id; do
                # Suspend this job.
                $SLURM_HOME/bin/scontrol suspend \$job_id
            done < \$jobsfile
            systemctl stop slurmd
    esac
fi
EOF
sudo mv /home/ubuntu/slurm_suspend_jobs /lib/systemd/system-sleep/slurm_suspend_jobs

# Script to resume suspended jobs following hibernation.
cat > /home/ubuntu/slurm_hib_resumejob.sh << EOF
#!/bin/bash
jobfile=\$(dirname \$0)/slurm_jobs_suspended.txt
while read job_id; do
    # Check and determine if this job is actually running.
    job_suspended=\$($SLURM_HOME/bin/scontrol show job | grep JobState=SUSPENDED)
    if [[ "\$job_suspended" != "" ]]; then
        $SLURM_HOME/bin/scontrol resume \$job_id
    fi
done < \$jobfile
# Reset the job file, as we have resumed all jobs.
rm \$jobfile
touch \$jobfile
EOF

sudo touch /lib/ec2-hibinit-agent/slurm_jobs_suspended.txt
sudo mv /home/ubuntu/slurm_hib_resumejob.sh /lib/ec2-hibinit-agent/slurm_hib_resumejob.sh
sudo chown root /lib/ec2-hibinit-agent/slurm_hib_resumejob.sh /lib/systemd/system-sleep/slurm_suspend_jobs
sudo chgrp root /lib/ec2-hibinit-agent/slurm_hib_resumejob.sh /lib/systemd/system-sleep/slurm_suspend_jobs
sudo chmod 755 /lib/ec2-hibinit-agent/slurm_hib_resumejob.sh /lib/systemd/system-sleep/slurm_suspend_jobs

# Patch the AWS EC2 hibernation agent to automatically "warm" the NFS mount before restarting networking.
# This is used to avoid an orphan NFS connection whereby the connection becomes stale, and the client is unable to restore it as the TCP port has changed.
cat > /home/ubuntu/hibinit-resume.patch << 'EOF'
8d7
< set -e
23c22,24
< systemctl restart --no-block systemd-networkd
---
> ls /shared/
> systemctl restart systemd-networkd
> systemctl restart --no-block slurmd
EOF
sudo patch /usr/lib/ec2-hibinit-agent/hibinit-resume /home/ubuntu/hibinit-resume.patch
rm /home/ubuntu/hibinit-resume.patch

# If AWS attempts to hibernate an instance while it is being resumed from a previous hibernation, that
# hibernation call will be ignored.
# To avoid this, don't start the next suspend operation until the previous one finishes.
cat > /home/ubuntu/sleep.patch << 'EOF'
21a22,30
>         while true; do
>             swapoff_running=$(sudo swapon --show=NAME | grep swap-hibinit)
>             if [ -z $swapoff_running ]; then
>                 break
>             else
>                 sleep 2
>             fi
>         done
>         sleep 2
EOF
sudo patch /etc/acpi/actions/sleep.sh /home/ubuntu/sleep.patch
rm /home/ubuntu/sleep.patch

# Disable KASLR (see https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/hibernation-disable-kaslr.html)
sudo sed -i '/^GRUB_CMDLINE_LINUX_DEFAULT/ s/"$/ nokaslr"/' /etc/default/grub.d/50-cloudimg-settings.cfg
sudo update-grub
sudo reboot
