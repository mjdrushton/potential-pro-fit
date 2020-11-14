.. _pprofit-jobtasks:

#########
Job-Tasks
#########

Job tasks work with :ref:`job factories <pprofit-jobfactories>` to provide additional files required by the job. After a job-factory has done the heavy lifting of creating and initially populating the job directory, job tasks are there to do common things like potential tabulation. Depending on their function they can run before the job's ``runjob`` is executed, or after the job completes (but before the :ref:`evaluator <pprofit-evaluators>` values are determined).

Job tasks run on the same machine as ``pprofit``\ . This can be useful in a number of circumstances. For instance, to execute a script which depends on  resources only available locally.

Tasks are defined in a job's ``job.cfg`` file and are executed in the order they are defined. They have the following general form::

    [Task:TASK_ID]
    type: TASK_TYPE
    OPTIONS


Where:

    * ``TASK_ID`` - uniquely identifies the task in the job. By convention it is typical to use the name of the output file created by the task.
    * ``TASK_TYPE`` - identifies the sort of task that will be run. See the reference documentation below for a list of supported tasks.
    * ``OPTIONS`` - a list of options specific to the task. These will vary depending on task type.


.. rubric:: Example:


The following `job.cfg` contains a ``Potable`` task which would produce a tabulated potential, defined using the format used by the `atsim.potentials <https://atsimpotentials.readthedocs.io>`_ potable tool::

        [Job]
        type : Template
        runner : Local

        [Task:table.lmptab]
        type: Potable
        input_filename: table.aspot
        output_filename : table.lmptab

        [Evaluator:Cell]
        type : Regex
        filename : CaO.lmpout



Job Task Reference
==================

Potable
^^^^^^^

:Type-Name: Potable
:Description: Task used to created tabulated potential files. Files using the  `atsim.potentials <https://atsimpotentials.readthedocs.io>`_ potable format are tabulated by this task.

Required Fields
---------------

:Name: ``input_filename``
:Arg type: string
:Description: Name of the ``potable`` input file to be tabulated.


:Name: ``output_filename``
:Arg type: string
:Description: Name of the potential table file created by this task.