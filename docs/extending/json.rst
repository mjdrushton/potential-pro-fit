
.. _extending_json:

*************************
pprofitmon JSON Interface
*************************

The browser based ``pprofitmon`` graphical user interface communicates with its server through its REST based interface. This interface returns JSON formatted data and may be used as the basis for custom analysis tools or alternative monitoring tools.

The various calls supported by pprofitmon are documented here.


JSON Call Reference
===================

.. contents:: Call Hierarchy
   :local:



``/fitting``
------------

``best_candidate``
^^^^^^^^^^^^^^^^^^

.. autojson:: atsim.pro_fit.webmonitor.Fitting.best_candidate

``current_iteration``
^^^^^^^^^^^^^^^^^^^^^

.. autojson:: atsim.pro_fit.webmonitor.Fitting.current_iteration


``evaluated``
^^^^^^^^^^^^^

.. autojson:: atsim.pro_fit.webmonitor.Fitting.evaluated


``iteration_overview``
^^^^^^^^^^^^^^^^^^^^^^

.. autojson:: atsim.pro_fit.webmonitor.Fitting.iteration_overview


``run_status``
^^^^^^^^^^^^^^

.. autojson:: atsim.pro_fit.webmonitor.Fitting.run_status


``variables``
^^^^^^^^^^^^^

.. autojson:: atsim.pro_fit.webmonitor.Fitting.variables


``iteration_series/``
^^^^^^^^^^^^^^^^^^^^^

``merit_value``
""""""""""""""""

.. autojson:: atsim.pro_fit.webmonitor.IterationSeries.merit_value


.. _extending_json_iterationseries_columns:

Iteration Series Column Reference
=================================

The ``/fitting/iteration_series/`` JSON handlers allow columns to be selected for inclusion within the data tables they return. These columns and the way in which they are specified is now described.

.. _extending_json_iterationseries_columns_evaluator:

Evaluator Columns 
------------------

Column-label has format::

	evaluator:JOB_NAME:EVALUATOR_NAME:VALUE_NAME:VALUE_TYPE

* **Where:**
	- ``JOB_NAME`` - Name of job
	- ``EVALUATOR_NAME`` - Name of evaluator for which value should be produced
	- ``VALUE_NAME`` - Name of value extracted by evaluator
	- ``VALUE_TYPE`` - 'merit_value', 'extracted_value' or 'percent_difference' which give merit value, extracted value or difference in percent between expected and extracted value respectively.

Example:
^^^^^^^^

A ``pprofit`` run has a job named ``CaO`` with this evaluator definition in its ``job.cfg``:

.. code-block:: cfg

	[Evaluator:Gulp]
	type : Gulp
	filename : CaO.res
	cell_a : 4.8152
	cell_b : 4.8152
	cell_c : 4.8152
	bulkmodulus_hill : 114.0

To specify the ``cell_c`` values extracted by the Gulp evaluator this column specification may be used::

	evaluator:CaO:CaO:Gulp:cell_c:extract

* Here:

  -	``evaluator:`` is the column prefix.
  -	``CaO`` is the ``JOB_NAME``.
  -	``CaO:Gulp`` is ``EVALUATOR_NAME`` (internally ``pprofit`` combines job and evaluator names which is why ``CaO`` is repeated).
  -	``cell_c`` is the name of the evaluator record (``VALUE_NAME``) produced by the Gulp evaluator targeted by this column definition.
  -	``extract`` is ``VALUE_TYPE`` and states that the raw value extracted by the evaluator is to be used.


.. _extending_json_iterationseries_columns_variable:

Variable Columns
----------------

Variable column keys take the form::
	
	variable:VARIABLE_NAME

* Where:

    - ``VARIABLE_NAME`` identifies variable.
 

Example:
^^^^^^^^

A fitting run defines these variables in its ``fit.cfg`` file:

.. code-block:: cfg

	[Variables]
	A_OO :   1000.0 (0, 10000)
	rho_OO : 0.1    (0.1, 0.5)
	C_OO :   0.0    (0, 50)


To select ``rho_OO`` values this column specification is used::

	variable:rho_OO




.. _extending_json_iterationseries_columns_stats:

Statistics Columns
------------------

For a given iteration, statistics columns provide information about the distribution of merit values for that iteration's population of candidate solutions.


+---------------------+----------------------------------------------------+
| Column Key          | Description                                        |
+=====================+====================================================+
| ``stat:min``        | Minimum merit value across iteration's candidates  |
+---------------------+----------------------------------------------------+
| ``stat:max``        | Maximum merit value                                |
+---------------------+----------------------------------------------------+
| ``stat:mean``       | The mean of the merit values for a given iteration |
+---------------------+----------------------------------------------------+
| ``stat:median``     | Median merit value for iteration's candidates      |
+---------------------+----------------------------------------------------+
| ``stat:std_dev``    | Standard deviation of merit values                 |
+---------------------+----------------------------------------------------+
| ``stat:quartile1``, | First, second and third quartiles of merit values  |
| ``stat:quartile2``, |                                                    |
| ``stat:quartile3``  |                                                    |
+---------------------+----------------------------------------------------+


.. _extending_json_iterationseries_columns_itmetadata:

Iteration Meta-data
-------------------

The available iteration column keys are described in the following table:

+-----------------------+--------------------------------------------------+
| Column Key            | Description                                      |
+=======================+==================================================+
| ``it:is_running_min`` | Evaluates to ``True`` if no earlier iteration    |
|                       | contains a merit value than the given row.       |
|                       | Column contains ``False`` otherwise.             |
+-----------------------+--------------------------------------------------+
| ``it:is_running_max`` | Evaluates to ``True`` when current row has       |
|                       | higher merit value than any earlier row.         |
+-----------------------+--------------------------------------------------+


