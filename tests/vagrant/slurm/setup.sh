#! /bin/bash
apt-get --yes install language-pack-en
apt-get --yes install slurm-llnl
cp /vagrant/slurm.conf /etc/slurm-llnl/slurm.conf
/usr/sbin/create-munge-key
/etc/init.d/slurm-llnl start
/etc/init.d/munge start
