.. _pprofit-metaevaluators:

###############
Meta-Evaluators
###############

Meta evaluators can be thought of as :ref:`evaluators <pprofit-metaevaluators>` that are called after job evaluation and therefore have access to the values extracted by the job level :ref:`evaluators <pprofit-metaevaluators>`. Rather than being associated with a single job, meta-evaluators are associated with the fitting run as a whole. Sitting above the jobs in this way means meta-evaluators can be used for many purposes. For example:

	* Calculate merit-values that require information from multiple jobs.
	* For a single job synthesise merit-values from records obtained from different evaluators, avoiding the need to program a new, custom, evaluator.
	* Impose relationships between different jobs. For instance, apply a merit-value penalty if the energies of a set of structures do not lie in the correct order.


Meta-evaluators are specified within the top-level ``fit.cfg`` file as one or more meta-evaluator blocks. These have the basic format::

	[MetaEvaluator:EVALUATOR_NAME]
	type : META_EVALUATOR_TYPE
	...

Where ``EVALUATOR_NAME`` uniquely identifies the evaluator and ``META_EVALUATOR_TYPE`` names a particular meta-evaluator (the meta-evaluators provided with Potential Pro-Fit are described below). The remainder of the configuration block is used to specify ``option : value`` pairs specific to the chosen ``META_EVALUATOR_TYPE``. 

Example
=======

For a cubic system, bulk modulus can be calculated as :math:`B = \frac{1}{3} (C_{11} + 2C_{12})`. Whilst the :ref:`Gulp evaluator <pprofit-evaluators-gulp>` can extract bulk modulus through the ``bulkmodulus_reuss``, ``bulkmodulus_voigt`` and ``bulkmodulus_hill`` fields, these apply a form of averaging in order to estimate the bulk-modulus of a polycrystalline material from elastic constants that have effectively been obtained for a single crystal system.  Sometimes it might be interesting to calculate :math:`B` directly from the individual elastic constants. The following example shows how the :ref:`Formula <pprofit-metaevaluators-formula>` meta-evaluator can be applied to achieve this.

A job named ``MgO`` has been set-up to run Gulp and extract the :math:`C_{11}` and :math:`C_{12}` elastic constants using a ``job.cfg`` file with the following contents::

	[Job]
	type   : Template
	runner : Local

	[Evaluator:elastic]
	type        : Gulp
	filename    : output.res
	elastic_c11 : 1.0 0.0
	elastic_c12 : 1.0 0.0

.. note::
	In this ``job.cfg`` file the weights for the ``elastic_c11`` and ``elastic_c12`` values are set as 0.0. This allows the meta-evaluator to have access to these values but prevents the values from contributing to the overall-merit value used during fitting.


The meta-evaluator is then defined within the ``fit.cfg`` thus::

	[MetaEvaluator:Bulk_Modulus]
	type : Formula
	variable_c11 		   : MgO:elastic:elastic_c11:extracted_value
	variable_c12 		   : MgO:elastic:elastic_c12:extracted_value
	expression_bulkmodulus : (1.0/3.0)*(c11 * 2*c12)

Although the ``Formula`` meta-evaluator is described in more detail :ref:`here <pprofit-metaevaluators-formula>`, this ``[MetaEvaluator]`` block performs the following:

	* Two variables are defined as ``c11`` and ``c12``.
	* These variables take their values from the ``elastic`` evaluator belonging to the ``MgO`` job. The syntax used to define these variables can be read as:
		- ``variable_VARNAME : JOB_NAME:EVALUATOR_NAME:EVALUATOR_RECORD:extracted_value``
	* The field ``expression_bulkmodulus`` defines a new evaluator record named ``bulkmodulus`` whose value is calculated from the ``c11`` and ``c12`` variables using the given expression.

.. note::
	Multiple MetaEvaluator blocks can be defined within the ``fit.cfg`` file. 


Meta-Evaluator Reference
========================

.. _pprofit-metaevaluators-formula:

Formula
^^^^^^^

:Type-name: Formula
:Description: Meta-evaluator that creates merit values from mathematical expressions. These expressions have access to the records extracted from each job run.

\

Defining expressions
--------------------

Each ``Formula`` meta-evaluator can define multiple expressions. Expressions have the form::

	expression_VALUENAME : [EXPECTED_VALUE = ] EXPRESSION

Where:
	* ``VALUENAME`` defines the name of the evaluator record as it is passed to the global merit function and :ref:`fitting tool monitor <pprofitmon>`.
	* ``EXPECTED_VALUE`` [optional] If specified, then expression evaluates to the root-squared difference between the ``EXPECTED_VALUE`` and the value obtained from evaluating ``EXPRESSION`` (i.e. :math:`\sqrt{ \left( {\mathrm EXPECTED\_VALUE} - V \right)^2 }` where :math:`V` is value obtained from evaluating ``EXPRESSION``).
		- If ``EXPECTED_VALUE`` is not specified then ``expression_VALUENAME`` is simply the result of evaluating ``EXPRESSION``.
	* ``EXPRESSION`` is a string defining the formula to be evaluated.



Expression Weights
""""""""""""""""""

Weighting values can be applied to the values obtained by evaluating expressions. The evaluated expressions are multiplied by their weight values before being passed to the merit function. By default a weight of 1.0 is used however in certain circumstances it can be useful to apply different weighting values. In particular, if a weight of 0.0 is used the expression value will be reported within the :ref:`fitting tool monitor <pprofitmon>` but will not affect the fitting process. In this way expressions can be debugged or expression data can be used for informational purposes.

Weights are defined using fields with this format::

	weight_VALUENAME : WEIGHT

Where ``VALUENAME`` is the name of the expression to be weighted and  ``WEIGHT`` is a floating point number defining the weight.

The following gives an example of an expression and its associated weight field::

	expression_sum : A + B + C
	weight_sum : 0.0


.. _pprofit-metaevaluators-expressionsyntax:

Expression Syntax
"""""""""""""""""

The expression parser used by the meta-evaluator supports standard arithmetic operators and mathematical functions. In addition variables defined using ``variable_*`` syntax can be included in expressions. Expressions are parsed using the exprtk library, although summarised below, more information about accepted syntax and available functions can be found here: http://partow.net/programming/exprtk/

	* Mathematical operators: ``+, -, *, /, %, ^``
	* Functions: ``min, max, avg, sum, abs, ceil, floor, round, roundn, exp, log, log10, logn, root, sqrt, clamp, inrange``
	* Trigonometry ``sin, cos, tan, acos, asin, atan, atan2, cosh, cot, csc, sec, sinh, tanh, d2r, r2d, d2g, g2d, hyp``
	* Equalities, Inequalities: ``==, <>, !=, <, <=, >, >=``
	* Boolean logic ``and, mand, mor, nand, nor, not, or, xor, xnor``


Defining Variables
------------------

Before being used in expression, job evaluator records must be assigned a variable label. Variables are defined using fields of the form::

	variable_VARNAME : EVALUATOR_KEY

Where ``VARNAME`` is the variable label that can be used in expressions. ``EVALUATOR_KEY`` identifies the evaluator record to which this variable relates and has the form::

	JOB_NAME:EVALUATOR_NAME:EVALUATOR_RECORD[:ATTRIBUTE]


Where:
  * ``JOB_NAME`` : Name of job.
  * ``EVALUATOR_NAME`` : Evaluator label within job.
  * ``EVALUATOR_RECORD`` : Name of value extracted by evaluator.
  * ``ATTRIBUTE`` : This can be one of:
    - ``extracted_value`` : value extracted by evaluator.
    - ``expected_value``  : value expected by evaluator.
    - ``weight``          : evaluator value weight.
    - ``merit_value``     : evaluator record merit value.

If ``ATTRIBUTE`` is not specified then ``merit_value`` is assumed by default.

Therefore if a job named ``MgO`` has an evaluator named ``Gulp`` with that extracts a value named ``lattice_energy``, you could use the extracted lattice energy within an expression as a variable ``A`` by defining the following variable::

	variable_A : MgO:Gulp:lattice_energy:extracted_value

Example
--------

In this example a job named  ``MgO`` uses the GULP code to energy minimise an :math:`8 \times 8 \times 8` MgO super-cell. The job specifies an Evaluator named ``Cell`` that extracts the a, b and c cell lengths as ``cell_a``, ``cell_b`` and ``cell_c``::


	[MetaEvaluator:Volume]
	type : Formula
	variable_A : MgO:Cell:cell_a:extracted_value
	variable_B : MgO:Cell:cell_b:extracted_value
	variable_C : MgO:Cell:cell_c:extracted_value
	
	expression_lattice_constant : 4.212 = (A+B+C)/3 / 8
	weight_lattice_constant : 0.0

	expression_volume : sqrt(((8*4.212)^3 - (A*B*C))^2)

Notes:
""""""

	1. Variables ``A``, ``B`` and ``C`` representing the three extracted unit-cell lengths are defined for use in subsequent expression::

		variable_A : MgO:Cell:cell_a:extracted_value
		variable_B : MgO:Cell:cell_b:extracted_value
		variable_C : MgO:Cell:cell_c:extracted_value


	2. The ``lattice_constant`` expression calculates the average lattice constant of the super-cell and divides this by 8 in order to allow easier comparison with experimental values. A zero weight is then assigned to this expression. This means that this ``lattice_constant`` will not contribute to the overall merit value and hence will not affect the fit. However, it will still be reported to ``pprofitmon``, allowing the value to be monitored and a percentage difference between the desired value of 4.212 and the calculated value to be displayed. This is a useful technique not only for monitoring runs but for debugging the formulae used in expressions::

		expression_lattice_constant : 4.212 = (A+B+C)/3 / 8
		weight_lattice_constant : 0.0


	3. ``expression_volume`` - Calculates the RMS difference between the desired volume (based on a lattice parameter of 4.212 and super-cell dimension of 8) and the actual volume calculated from a,b and c cell parameters::

		expression_volume : sqrt(((8*4.212)^3 - (A*B*C))^2)


	4. The same result could have been obtained by specifying an expected value a calculation of the volume thus::

		expression_volume : 305767.9111 = A*B*C


