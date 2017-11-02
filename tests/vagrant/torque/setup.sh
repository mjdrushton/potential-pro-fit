#!/bin/bash

#
# HOSTS SETUP
#
cat > /etc/hosts <<EOF
127.0.0.1       localhost

# The following lines are desirable for IPv6 capable hosts
::1     ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters

127.0.0.1   master torqueserver
127.0.0.1   slave
EOF

# Create public key
sudo -u vagrant ssh-keygen -t rsa -P '' -f /home/vagrant/.ssh/id_rsa
sudo -u vagrant cat /home/vagrant/.ssh/id_rsa.pub >> /home/vagrant/.ssh/authorized_keys
ssh-keyscan -t rsa localhost > /home/vagrant/.ssh/known_hosts
chown vagrant:vagrant /home/vagrant/.ssh/known_hosts


apt-get install -y torque-server torque-scheduler torque-mom torque-client

qterm # kill running jobs

yes | pbs_server -t create

qmgr -c "set server acl_hosts=master"
qmgr -c "set server scheduling=true"

# setup queue
qmgr -c "create queue default queue_type=execution"
qmgr -c "set queue default started=true"
qmgr -c "set queue default enabled=true"
qmgr -c "set queue default resources_default.nodes=1"
qmgr -c "set queue default resources_default.walltime=3600"
qmgr -c "set server default_queue=default"
qmgr -c "set server keep_completed = 10"

# default 12 CPUs
echo  "master np=12" > /var/spool/torque/server_priv/nodes

cat > /var/spool/torque/mom_priv/config <<EOF
\$pbsserver      master
\$logevent       255
EOF

qterm
pbs_server
momctl -q check_poll_time=1
