.. _ssh-keybased-login:

SSH Key-Based Login
*******************

A number of tools within the atomsscripts suite use the secure shell (SSH) to communicate with remote machines. Often these automated tools need passwordless login to the remote host. This can be achieved by using SSH key login. The following describes how to set-up public key based login using keys with blank pass-phrases. For the purposes of this guide it is assumed that a UNIX like system (e.g. Linux or MacOSX) however the principles described can be applied to other systems by referring to the documentation of the SSH client being used.

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

