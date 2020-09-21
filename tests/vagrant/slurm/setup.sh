#! /bin/bash

export DEBIAN_FRONTEND=noninteractive

apt-get update -yq
apt-get -yq install python
apt-get -yq install language-pack-en
apt-get -yq install slurm-wlm
cp /vagrant/slurm.conf /etc/slurm-llnl/slurm.conf
/usr/sbin/create-munge-key -f
/etc/init.d/slurmd start
/etc/init.d/slurmctld start
/etc/init.d/munge start
