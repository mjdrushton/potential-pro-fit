#! /bin/bash

apt-get update
apt-get --yes install python
apt-get --yes install language-pack-en
apt-get --yes install slurm-wlm
cp /vagrant/slurm.conf /etc/slurm-llnl/slurm.conf
/usr/sbin/create-munge-key -f
/etc/init.d/slurmd start
/etc/init.d/slurmctld start
/etc/init.d/munge start
