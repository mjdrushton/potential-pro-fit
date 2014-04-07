.. _fittingtool-runners:

#######
Runners
#######

As their names suggest runners are responsible for running jobs and making sure that their output is copied to a job's ``output/`` sub-directory. Runners are provided to run jobs on the local machine (see :ref:`fittingtool-runners-Local`) or on remote hosts (see :ref:`fittingtool-runners-Remote` and :ref:`fittingtool-runners-PBS` as examples). When jobs are run remotely, the runner is responsible for copying the job files to the remote machine, invoking ``runjob`` for each file, monitoring job completion before copying the output files back to the machine running ``fittingTool.py``.

At present the following runners are supported by the fitting tool:

  * :ref:`fittingtool-runners-Local` - Allows jobs to be run in parallel on the computer running ``fittingTool.py``.
  * :ref:`fittingtool-runners-Remote` - Uses SSH to run jobs in parallel on a remote computer.
  * :ref:`fittingtool-runners-PBS` - Submits jobs to queues on a remote machine running the PBS batch queueing system.


Configuring Runners and Associating with Jobs
=============================================

Each job has a runner associated with it and each fitting run can define multiple runners. This provides considerable flexibility. For instance, to make each iteration of a fitting complete more quickly it may be desirable to run certain jobs locally and large parallel jobs remotely on a high performance computing cluster using PBS. Running locally may avoid lengthy queing times that could damage efficiency when running large numbers short serial jobs. Whilst for an MD job the the queuing time could be offset by the speed-up afforded by running on a large number nodes. 

Runners are configured in the ``fit.cfg`` file in the root fitting directory. The basic form of a runner configuration block is as follows::

	[Runner:RUNNER_NAME]
	type : RUNNER_TYPE
	...

Where ``RUNNER_NAME`` is a label that uniquely identifies this ``RUNNER`` to the jobs with which it is associated. ``RUNNER_TYPE`` defines what sort of runner should be created (details of the available runners are listed below). The remainder of the block contains specific ``field : value pairs`` that configure the runner.

Continuing our example in which both :ref:`fittingtool-runners-Local` and remote :ref:`fittingtool-runners-PBS` runners are required, the following could be added to ``fit.cfg``::

	...

	[Runner:Local]
	type       : Local
	nprocesses : 4

	[Runner:HPC]
	type       : PBS
	remotehost : ssh://remoteuser@remotehost//home/remoteuser/jobdirectory
	...

Here the local runner (labelled ``Local``) , is configured to run a maximum of four jobs concurrently. The :ref:`fittingtool-runners-PBS` runner (labelled ``HPC``) is configured to log into ``remotehost`` as a user named ``remoteuser``, before invoking ``qsub`` for jobs that are staged in the directory ``/home/remoteuser/jobdirectory``.

Each job that constitutes a ``fittingTool.py`` needs to be associated with a runner. This is achieved by setting the 'runner' field within the ``[Job]`` section of each job's ``job.cfg`` configuration file. The format of the ``runner`` field is::

	runner : RUNNER_NAME

Where ``RUNNER_NAME`` is the label of a runner defined with ``fit.cfg``. Using the runners defined in the example above the ``[Job]`` configuration block for a local job could look like this::

	[Job]
	type   : Template
	runner : Local
	...

For comparison a job associated with the HPC, PBS runner may be configured as follows::

	[Job]
	type   : Template
	runner : HPC
	...

For completeness the ``type : Template`` directive indicates that these jobs use the :ref:`Template <fittingtool-jobfactories-Template>` job-factory (see :ref:`fittingtool-jobfactories` for more).


Runner Reference
================

.. _fittingtool-runners-local:

Local
^^^^^

:Type-Name: Local
:Description: Runs jobs on the same computer as the ``fittingTool.py`` script. 
	This runner spawns :ref:`nprocesses <fittingtool-runners-local-nprocesses>` processes. This means that a maximum of :ref:`nprocesses <fittingtool-runners-local-nprocesses>` jobs can run at the same time.

\ 


Required Fields
---------------

.. _fittingtool-runners-local-nprocesses:

:Name: nprocesses
:Arg type: integer
:Description: Number of processes to be spawned by runner. In general it makes sense to set this to the same number of cores as your machine has.

.. _fittingtool-runners-pbs:

PBS
^^^

:Type-Name: PBS
:Description: Runner that remotely submits jobs to a computational cluster running the PBS batch queuing system.


.. note::
	The PBS runner uses SSH to communicate with the PBS head-node. In order to run correctly ``fittingTool.py`` must be able to log into the remote-host and invoke the ``qsub`` command without requiring a password. This can be achieved by setting-up key based login as described in :ref:`ssh-keybased-login`. 

Required Fields
---------------

:Name: remotehost
:Format: ``ssh://[USERNAME@]PBS_HOST[:PORT]/REMOTE_PATH``
:Description: SSH URI giving the optional username (``USERNAME``), host-name (``PBS_HOST``), optional port number (``PORT``) and remote-path from which jobs should be run (``REMOTE_PATH``) on the PBS head node.
:Example: To run jobs on ``login.cx1.hpc.ic.ac.uk`` from a directory named ``/work/mjdr/jobs`` the following configuration option could be used:

	``remotehost : ssh://login.cx1.hpc.ic.ac.uk//work/mjdr/jobs``

\ 

.. note::
	If you receive errors such as ``cannot run 'qsub' on remote host`` or ``cannot run 'qstat' on remote host``, it may be:
		* the remote host is not a PBS head node. Log onto the machine and see if you can run ``qstat``.

		* the ``qsub`` and ``qstat`` commands may not be on your ``PATH`` by default when you run a non-interactive shell session. To test if the PBS runner can run ``qstat`` use the following command::

			ssh REMOTE_HOST "qstat --version"

		  This should print the PBS version to the screen. If this does not work, you may need to update you ``.bashrc`` file with an updated ``PATH`` variable. Remember that the shell session used by the fitting system to run the PBS commands in non-interactive, even though your commands may work at a log-in shell, it is important to check that they work using the command given above. Furthermore, depending on the shell you use, interactive and non-interactive shells may run different configuration files (the author has had success adding configuration to the ``.bashrc`` file).

.. note::
	The submission script generated by the PBS runner assumes that ``REMOTE_PATH`` is cross-mounted on the PBS execution nodes so that calculation files can be copied directly from ``REMOTE_PATH`` to temporary calculation directory created by PBS on the execution host. It is fairly typical of HPC clusters to mount user home directories on execution hosts and the head node, choosing for this reason choosing a sub-directory of your home directory as ``REMOTE_PATH`` will work in most cases. 

Optional Fields
---------------

:Name: pbsinclude
:Arg type: string
:Description: Provide path to a file that will be be included within the PBS job submission script used to run jobs. This can be used to specify job requirements to the queing system through ``#PBS`` option lines.
:Example: Specifying the following would include ``8cpus.pbs`` (from the root path of the fitting run) in the job submission script:

	``pbsinclude : 8cpus.pbs`` 


.. _fittingtool-runners-remote:

Remote
^^^^^^

:Type-Name: Remote
:Description: Runs jobs on a remote host using SSH as the communication mechanism. 
	This runner spawns :ref:`nprocesses <fittingtool-runners-remote-nprocesses>` processes on the remote machine. This means that a maximum of :ref:`nprocesses <fittingtool-runners-remote-nprocesses>` jobs can run at the same time.

.. note::
	The Remote runner uses SSH to communicate with the remote machine. In order to run correctly ``fittingTool.py`` must be able to log into the remote-host and invoke commands without requiring a password. This can be achieved by setting-up key based login as described in :ref:`ssh-keybased-login`. 	


Required Fields
---------------

:Name: remotehost
:Format: ``ssh://[USERNAME@]REMOTE_HOST[:PORT]/REMOTE_PATH``
:Description: SSH URI giving the optional username (``USERNAME``), host-name (``REMOTE_HOST``), optional port number (``PORT``) and remote-path from which jobs should be run (``REMOTE_PATH``) on the remote machine.
:Example: To run jobs on ``login.cx1.hpc.ic.ac.uk`` from a directory named ``/work/mjdr/jobs`` the following configuration option could be used:

	``remotehost : ssh://login.cx1.hpc.ic.ac.uk//work/mjdr/jobs``

\ 

.. _fittingtool-runners-remote-nprocesses:

:Name: nprocesses
:Arg type: integer
:Description: Number of processes to be spawned by runner. In general it makes sense to set this to the same number of cores as the remote machine.
