.. _ssh-keybased-login:

SSH Key-Based Login
===================

A number of tools within the atomsscripts suite use the secure shell (SSH) to communicate with remote machines. Often these automated tools need passwordless login to the remote host. This can be achieved by using SSH key login. Alternatively a number of runners can be configured through their ``ssh-config`` option, see :ref:`ssh_config_option`. The following describes how to set-up public key based login using keys with blank pass-phrases. For the purposes of this guide it is assumed that a UNIX like system is used (e.g. Linux or MacOSX) however the principles described can be applied to other systems by referring to the documentation of the SSH client being used.

In the following, ``USERNAME`` and ``REMOTE_HOST`` are used to refer to the user-name and hostname used to log into the remote machine. Type the commands below into a terminal.

    1. **Create ~/.ssh directory.** Check that your home directory contains a ``~/.ssh`` directory on both local and remote machines::

        ls ~/.ssh

       If you do not already have an ssh directory create one with the following commands::

        mkdir ~/.ssh
        chmod 700 ~/.ssh


    2. **Create SSH keys.** Type the following in a terminal::

        ssh-keygen -t rsa

       Assign a blank pass-phrase by pressing ``Enter`` twice. 
       This will create a public, private key pair in ``~/.ssh`` named ``id_rsa.pub`` and ``id_rsa`` respecetively.

    3. **Enable key login**. Whenever you try to SSH into a remote host (and key login is enabled), by default SSH will offer the private ``~/.ssh/id_rsa`` key to the remote host as an authentication method. For this to be successful, the ``id_rsa.pub`` public key must be listed in the remote machine's ``~/.ssh/authorized_keys`` file. This can be achieved by typing::

        ssh-copy-id USERNAME@REMOTE_HOST

       As not all systems have the ``ssh-copy-id`` command available the following may be used instead::

        cat  ~/.ssh/id_rsa.pub | ssh USERNAME@REMOTE_HOST 'cat >> ~/.ssh/authorized_keys' 

    4. **Check it worked.** Check that you can log into the remote machine without a password::

        ssh USERNAME@REMOTE_HOST

.. _ssh_config_option:

Configuring SSH Based Runners through the ssh-config option
===========================================================

The description of :ref:`ssh-keybased-login` section makes use of the default ``~/.ssh/id_rsa`` key to attempt a login to a remote machine. Using the default key in this way might not always be desirable or even possible. For this reason, runners using SSH provide the ``ssh-config`` configuration directive which provides much more control of how SSH is accessed.

The ``ssh-config`` directive is supported by the following runners:

* :ref:`pprofit-runners-pbs`
* :ref:`pprofit-runners-remote`
* :ref:`pprofit-runners-SGE`
* :ref:`pprofit-runners-slurm`

Using ssh-config option
-----------------------
The ``ssh-config`` option specifies the path to a text file containing option value pairs e.g.::


    OPTION_1 VALUE_1
    OPTION_2 VALUE_2
    ...

Option value pairs allowed OpenSSH's ``ssh_config`` can appear here. A list of supported options can be found `here <https://man.openbsd.org/ssh_config>`_.


Example: Using IdentityFile to Specify Private Key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
The following example shows how a :ref:`pprofit-runners-remote` runner can be configured to use a specific SSH private key.

The example given here was used by the author to connect to a virtualbox running Linux running on their laptop. The virtual machine was configured using `Vagrant <https://www.vagrantup.com>`_. This is a convenient tool for building and managing virtual machine environments. The virtual machine runs locally meaning ``pprofit`` is configured to connect to localhost: ``127.0.0.1``.

The runner's configuration in the ``fit.cfg`` file is given as::

    [Runner:Vagrant]
    type : Remote
    remotehost : ssh://vagrant@127.0.0.1//home/vagrant/jobs
    ssh-config : vagrant.ssh

Here the ``remotehost`` line specifies the username as ``vagrant`` and states that we're connecting to localhost and will be using ``/home/vagrant/jobs`` as our upload directory.

The ``ssh-config`` line points to the ``vagrant.ssh`` file. This contains::

    Port 2223
    UserKnownHostsFile /dev/null
    StrictHostKeyChecking no
    PasswordAuthentication no
    IdentityFile /home/user/vagrant/.vagrant/machines/default/virtualbox/private_key
    IdentitiesOnly yes

The most important line is the one beginning ``IdentityFile`` this tells ``pprofit`` to connect using the private SSH key located at ``/home/user/vagrant/.vagrant/machines/default/virtualbox/private_key``. The meaning of the other options are as follows:

* ``Port 2223`` - this specifies the port on which to connect. *Note:* the port could also have been given via the runner's ``remotehost`` option like this::

        remotehost : ssh://vagrant@127.0.0.1:2223//home/vagrant/jobs

* The following disables checking remote host key checking. In most cases you should not do this as it opens up the risk of `man in the middle spoofing attack <https://en.wikipedia.org/wiki/Man-in-the-middle_attack>`_, which is insecure. However here it is used because multiple virtual machines may be running on localhost with different host keys::

        UserKnownHostsFile /dev/null
        StrictHostKeyChecking no

* ``PasswordAuthentication no`` - If key authentication fails then do not fall back on password authentication.
* ``IdentitiesOnly yes`` - Only attempt authentication with the key defined in this file through the ``IdentifyFile`` option (i.e. don't try using the system default).

**Note:** The ``vagrant.ssh`` file used here was obtained by running the ``vagrant ssh-config`` command. This provides a convenient shortcut to creating an options file for use with Vagrant and pprofit.
