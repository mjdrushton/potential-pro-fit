#! /bin/bash

export DEBIAN_FRONTEND=noninteractive

apt-get update -yq
apt-get -yq install python
apt-get -yq install language-pack-en

# Configure the master hostname for Grid Engine
echo "gridengine-master       shared/gridenginemaster string  $HOSTNAME" | sudo debconf-set-selections
echo "gridengine-master       shared/gridenginecell   string  default" | sudo debconf-set-selections
echo "gridengine-master       shared/gridengineconfig boolean false" | sudo debconf-set-selections
echo "gridengine-common       shared/gridenginemaster string  $HOSTNAME" | sudo debconf-set-selections
echo "gridengine-common       shared/gridenginecell   string  default" | sudo debconf-set-selections
echo "gridengine-common       shared/gridengineconfig boolean false" | sudo debconf-set-selections
echo "gridengine-client       shared/gridenginemaster string  $HOSTNAME" | sudo debconf-set-selections
echo "gridengine-client       shared/gridenginecell   string  default" | sudo debconf-set-selections
echo "gridengine-client       shared/gridengineconfig boolean false" | sudo debconf-set-selections
# Postfix mail server is also installed as a dependency
echo "postfix postfix/main_mailer_type        select  No configuration" | sudo debconf-set-selections

# Install Grid Engine
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y gridengine-master gridengine-client

# Set up Grid Engine
sudo -u sgeadmin /usr/share/gridengine/scripts/init_cluster /var/lib/gridengine default /var/spool/gridengine/spooldb sgeadmin
sudo service gridengine-master restart


echo 127.0.0.1 localhost | sudo tee /etc/hosts
echo 192.168.9.10 master | sudo tee -a /etc/hosts

echo "gridengine-common       shared/gridenginemaster string  $MASTER_HOSTNAME" | sudo debconf-set-selections
echo "gridengine-common       shared/gridenginecell   string  default" | sudo debconf-set-selections
echo "gridengine-common       shared/gridengineconfig boolean false" | sudo debconf-set-selections
echo "gridengine-client       shared/gridenginemaster string  $MASTER_HOSTNAME" | sudo debconf-set-selections
echo "gridengine-client       shared/gridenginecell   string  default" | sudo debconf-set-selections
echo "gridengine-client       shared/gridengineconfig boolean false" | sudo debconf-set-selections
echo "postfix postfix/main_mailer_type        select  No configuration" | sudo debconf-set-selections

sudo DEBIAN_FRONTEND=noninteractive apt-get install -y gridengine-exec gridengine-client


# Disable Postfix
sudo service postfix stop
sudo update-rc.d postfix disable

echo $MASTER_HOSTNAME | sudo tee /var/lib/gridengine/default/common/act_qmaster
sudo service gridengine-master restart
sudo service gridengine-exec restart

sudo qconf -am $USER

sudo qconf -Msconf /vagrant/scheduler.conf

echo -e "group_name @allhosts\nhostlist NONE" > ./grid
sudo qconf -Ahgrp ./grid
rm ./grid

sudo qconf -Aq /vagrant/queue.conf

sudo qconf -as $HOSTNAME
sudo qconf -ah $HOSTNAME

sudo /vagrant/sge-worker-add.sh test.q $HOSTNAME 8

