.. _pprofit-runners:

#######
Runners
#######

As their names suggest runners are responsible for	 running jobs and making sure that their output is copied to a job's ``output/`` sub-directory. Runners are provided to run jobs on the local machine (see :ref:`pprofit-runners-Local`) or on remote hosts (see :ref:`pprofit-runners-Remote` and :ref:`pprofit-runners-PBS` as examples). When jobs are run remotely, the runner is responsible for copying the job files to the remote machine, invoking ``runjob`` for each file, monitoring job completion before copying the output files back to the machine running ``pprofit``.

At present the following runners are supported by the fitting tool:

  * :ref:`pprofit-runners-Local` - Allows jobs to be run in parallel on the computer running ``pprofit``.
  * :ref:`pprofit-runners-Remote` - Uses SSH to run jobs in parallel on a remote computer.
  * :ref:`pprofit-runners-PBS` - Submits jobs to queues on a remote machine running the `PBS <https://www.pbspro.org>`_/`Torque <http://www.adaptivecomputing.com/products/torque/>`_ batch queueing system.
  * :ref:`pprofit-runners-Slurm` - Runner for the `Slurm <https://slurm.schedmd.com>`_ batch queueing system.
  * :ref:`pprofit-runners-SGE` - Runner for the `Sun Grid Engine <https://en.wikipedia.org/wiki/Oracle_Grid_Engine>`_ batch queueing system.


Configuring Runners and Associating with Jobs
=============================================

Each job has a runner associated with it and each fitting run can define multiple runners. This provides considerable flexibility. For instance, to make each iteration of a fitting complete more quickly it may be desirable to run certain jobs locally and large parallel jobs remotely on a high performance computing cluster using PBS. Running locally may avoid lengthy queing times that could damage efficiency when running large numbers short serial jobs. Whilst for an MD job the the queuing time could be offset by the speed-up afforded by running on a large number nodes. 

Runners are configured in the ``fit.cfg`` file in the root fitting directory. The basic form of a runner configuration block is as follows::

	[Runner:RUNNER_NAME]
	type : RUNNER_TYPE
	...

Where ``RUNNER_NAME`` is a label that uniquely identifies this ``RUNNER`` to the jobs with which it is associated. ``RUNNER_TYPE`` defines what sort of runner should be created (details of the available runners are listed below). The remainder of the block contains specific ``field : value pairs`` that configure the runner.

Continuing our example in which both :ref:`pprofit-runners-Local` and remote :ref:`pprofit-runners-PBS` runners are required, the following could be added to ``fit.cfg``::

	...

	[Runner:Local]
	type       : Local
	nprocesses : 4

	[Runner:HPC]
	type       : PBS
	remotehost : ssh://remoteuser@remotehost//home/remoteuser/jobdirectory
	...

Here the local runner (labelled ``Local``) , is configured to run a maximum of four jobs concurrently. The :ref:`pprofit-runners-PBS` runner (labelled ``HPC``) is configured to log into ``remotehost`` as a user named ``remoteuser``, before invoking ``qsub`` for jobs that are staged in the directory ``/home/remoteuser/jobdirectory``.

Each job within a Potential Pro-Fit fitting run's input needs to be associated with a runner. This is achieved by setting the 'runner' field within the ``[Job]`` section of each job's ``job.cfg`` configuration file. The format of the ``runner`` field is::

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

For completeness the ``type : Template`` directive indicates that these jobs use the :ref:`Template <pprofit-jobfactories-Template>` job-factory (see :ref:`pprofit-jobfactories` for more).


Runner Reference
================

.. _pprofit-runners-local:

Local
^^^^^

:Type-Name: Local
:Description: Runs jobs on the same computer as the ``pprofit`` script. 
	This runner spawns :ref:`nprocesses <pprofit-runners-local-nprocesses>` processes. This means that a maximum of :ref:`nprocesses <pprofit-runners-local-nprocesses>` jobs can run at the same time.

\ 


Required Fields
---------------

.. _pprofit-runners-local-nprocesses:

:Name: nprocesses
:Arg type: integer
:Description: Number of processes to be spawned by runner. In general it makes sense to set this to the same number of cores as your machine has.

\


.. _pprofit-runners-pbs:

PBS
^^^

:Type-Name: PBS
:Description: Runner that remotely submits jobs to a computational cluster running the PBS batch queuing system.


.. note::
	The PBS runner uses SSH to communicate with the PBS head-node. In order to run correctly ``pprofit`` must be able to log into the remote-host and invoke the ``qsub`` command without requiring a password. This can be achieved by setting-up key based login as described in :ref:`ssh-keybased-login`. 

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
:Name: arraysize
:Arg type: int
:Description: Where possible, jobs are submitted to PBS as array jobs. This parameter specifies the maximum number of jobs in one of these arrays. When not specified, all the jobs for a given candidate, destined for the PBS runner will all run in a single array. This means that all the jobs in this batch must be uploaded to the remote server before being submitted to PBS.
	By specifying a value for ``arraysize``, job submission can take place after a smaller number of jobs have been uploaded. By using a smaller array size, the job's output files can also start to download after the sub-job has completed rather than waiting for the candidate's entire batch to finish. In this way better use may be made of idle time whilst ``pprofit`` waits for jobs to make their way through PBS.
:Example: ``arraysize : 8``

\

:Name: debug.disable-cleanup
:Arg type: bool
:Default: False
:Description: If True, files copied to the remote host's job directory are retained. Normally these would be deleted after a job completes or the runner terminates, if this option is True, this behaviour is disabled. This is useful for debugging, but in most cases this option should be False or omitted completely.

\

:Name: header_include
:Arg type: string
:Description: Provide path to a file that will be be included within the PBS job submission script used to run jobs. This can be used to specify job requirements to the queing system through ``#PBS`` option lines.
:Example: Specifying the following would include ``8cpus.pbs`` (from the root path of the fitting run) in the job submission script:

	``header_include : 8cpus.pbs`` 

\

:Name: pollinterval
:Arg type: float
:Default: 30.0 seconds
:Description: The PBS runner monitors job completion by repeatedly running the ``qselect`` command on the remote host. The value of ``pollinterval`` specifies the time interval (in seconds) between calls to ``qselect``. Although small values of ``pollinterval`` may improve efficiency, they may also place a considerable burden on the PBS system and annoy your local system administrator. As a result you should choose a value that is at least a little bit larger than the queuing system's scheduling interval.

\

:Name: ssh-config
:Arg type: str
:Description: path to file containing options to tailor SSH connection. See :ref:`ssh_config_option`



.. _pprofit-runners-remote:

Remote
^^^^^^

:Type-Name: Remote
:Description: Runs jobs on a remote host using SSH as the communication mechanism. 
	This runner spawns :ref:`nprocesses <pprofit-runners-remote-nprocesses>` processes on the remote machine. This means that a maximum of :ref:`nprocesses <pprofit-runners-remote-nprocesses>` jobs can run at the same time.

.. note::
	The Remote runner uses SSH to communicate with the remote machine. In order to run correctly ``pprofit`` must be able to log into the remote-host and invoke commands without requiring a password. This can be achieved by setting-up key based login as described in :ref:`ssh-keybased-login`. 	


Required Fields
---------------

:Name: remotehost
:Format: ``ssh://[USERNAME@]REMOTE_HOST[:PORT]/REMOTE_PATH``
:Description: SSH URI giving the optional username (``USERNAME``), host-name (``REMOTE_HOST``), optional port number (``PORT``) and remote-path from which jobs should be run (``REMOTE_PATH``) on the remote machine.
:Example: To run jobs on ``login.cx1.hpc.ic.ac.uk`` from a directory named ``/work/mjdr/jobs`` the following configuration option could be used:

	``remotehost : ssh://login.cx1.hpc.ic.ac.uk//work/mjdr/jobs``

\ 

.. _pprofit-runners-remote-nprocesses:

:Name: nprocesses
:Arg type: integer
:Description: Number of processes to be spawned by runner. In general it makes sense to set this to the same number of cores as the remote machine.

\


Optional Fields
---------------

:Name: debug.disable-cleanup
:Arg type: bool
:Default: False
:Description: If True, files copied to the remote host's job directory are retained. Normally these would be deleted after a job completes or the runner terminates, if this option is True, this behaviour is disabled. This is useful for debugging, but in most cases this option should be False or omitted completely.

\

:Name: ssh-config
:Arg type: str
:Description: path to file containing options to tailor SSH connection. See :ref:`ssh_config_option`

\


.. _pprofit-runners-slurm:

Slurm
^^^^^

:Type-Name: Slurm
:Description: Runner that remotely submits jobs to a computational cluster running the `Slurm <https://slurm.schedmd.com>`_ batch queuing system.


.. note::
	The Slurm runner uses SSH to communicate with the PBS head-node. In order to run correctly ``pprofit`` must be able to log into the remote-host and invoke the ``sbatch`` command without requiring a password. This can be achieved by setting-up key based login as described in :ref:`ssh-keybased-login`. 

Required Fields
---------------

:Name: remotehost
:Format: ``ssh://[USERNAME@]SLURM_HOST[:PORT]/REMOTE_PATH``
:Description: SSH URI giving the optional username (``USERNAME``), host-name (``SLURM_HOST``), optional port number (``PORT``) and remote-path from which jobs should be run (``REMOTE_PATH``) on the Slurm submission host.
:Example: To run jobs on ``login.cx1.hpc.ic.ac.uk`` from a directory named ``/work/mjdr/jobs`` the following configuration option could be used:

	``remotehost : ssh://login.cx1.hpc.ic.ac.uk//work/mjdr/jobs``

\ 

Optional Fields
---------------
:Name: arraysize
:Arg type: int
:Description: Jobs are submitted to Slurm as array jobs. This parameter specifies the maximum number of jobs in one of these arrays. When not specified, all the jobs assigned to this runner, for a given candidate are run as one array. This means that all the jobs in this batch must be uploaded to the remote server before being submitted to the queueing system.
	By specifying a value for ``arraysize``, job submission can take place after a smaller number of jobs have been uploaded. By using a smaller array size, the job's output files can also start to download after the sub-job has completed rather than waiting for the candidate's entire batch to finish. In this way better use may be made of idle time whilst ``pprofit`` waits for jobs to make their way through the queueing system.
:Example: ``arraysize : 8``

\

:Name: debug.disable-cleanup
:Arg type: bool
:Default: False
:Description: If True, files copied to the remote host's job directory are retained. Normally these would be deleted after a job completes or the runner terminates, if this option is True, this behaviour is disabled. This is useful for debugging, but in most cases this option should be False or omitted completely.

\

:Name: header_include
:Arg type: string
:Description: Provide path to a file that will be be included within the Slurm submission script used to run jobs. This can be used to specify job requirements to Slurm through ``#SBATCH`` option lines.
:Example: Specifying the following would include ``8cpus.slurm`` (from the root path of the fitting run) in the job submission script:

	``header_include : 8cpus.slurm`` 

\

:Name: pollinterval
:Arg type: float
:Default: 30.0 seconds
:Description: This runner monitors job completion by repeatedly running the ``squeue`` command on the Slurm host. The value of ``pollinterval`` specifies the time interval (in seconds) between calls to ``squeue``. Although small values of ``pollinterval`` may improve efficiency, they may also place a considerable burden on the queueing system and annoy your local system administrator. As a result you should choose a value that is at least a little bit larger than the queuing system's scheduling interval.

\

:Name: ssh-config
:Arg type: str
:Description: path to file containing options to tailor SSH connection. See :ref:`ssh_config_option`

\


.. _pprofit-runners-SGE:

SGE
^^^

:Type-Name: SGE
:Description: Runner that remotely submits jobs to a computational cluster running the `Sun Grid Engine <https://en.wikipedia.org/wiki/Oracle_Grid_Engine>`_ batch queuing system.


.. note::
	The SGE runner uses SSH to communicate with the PBS head-node. In order to run correctly ``pprofit`` must be able to log into the remote-host and invoke the ``qsub`` command without requiring a password. This can be achieved by setting-up key based login as described in :ref:`ssh-keybased-login`. Or through the ``ssh-config`` option.	

Required Fields
---------------

:Name: remotehost
:Format: ``ssh://[USERNAME@]SGE_HOST[:PORT]/REMOTE_PATH``
:Description: SSH URI giving the optional username (``USERNAME``), host-name (``SGE_HOST``), optional port number (``PORT``) and remote-path from which jobs should be run (``REMOTE_PATH``) on the Slurm submission host.
:Example: To run jobs on ``login.cx1.hpc.ic.ac.uk`` from a directory named ``/work/mjdr/jobs`` the following configuration option could be used:

	``remotehost : ssh://login.cx1.hpc.ic.ac.uk//work/mjdr/jobs``

\ 

Optional Fields
---------------
:Name: arraysize
:Arg type: int
:Description: Jobs are submitted to SGE as array jobs. This parameter specifies the maximum number of jobs in one of these arrays. When not specified, all the jobs assigned to this runner, for a given candidate are run as one array. This means that all the jobs in this batch must be uploaded to the remote server before being submitted to the queueing system.
	By specifying a value for ``arraysize``, job submission can take place after a smaller number of jobs have been uploaded. By using a smaller array size, the job's output files can also start to download after the sub-job has completed rather than waiting for the candidate's entire batch to finish. In this way better use may be made of idle time whilst ``pprofit`` waits for jobs to make their way through the queueing system.
:Example: ``arraysize : 8``

\

:Name: debug.disable-cleanup
:Arg type: bool
:Default: False
:Description: If True, files copied to the remote host's job directory are retained. Normally these would be deleted after a job completes or the runner terminates, if this option is True, this behaviour is disabled. This is useful for debugging, but in most cases this option should be False or omitted completely.

\

:Name: header_include
:Arg type: string
:Description: Provide path to a file that will be be included within the SGE submission script used to run jobs. This can be used to specify job requirements to SGE through ``#$`` option lines.
:Example: Specifying the following would include ``8cpus.sge`` (from the root path of the fitting run) in the job submission script:

	``header_include : 8cpus.sge`` 

\

:Name: pollinterval
:Arg type: float
:Default: 30.0 seconds
:Description: This runner monitors job completion by repeatedly running the ``qstat`` command on the SGE host. The value of ``pollinterval`` specifies the time interval (in seconds) between calls to ``qstat``. Although small values of ``pollinterval`` may improve efficiency, they may also place a considerable burden on the queueing system and annoy your local system administrator. As a result you should choose a value that is at least a little bit larger than the queuing system's scheduling interval.

\

:Name: ssh-config
:Arg type: str
:Description: path to file containing options to tailor SSH connection. See :ref:`ssh_config_option`

