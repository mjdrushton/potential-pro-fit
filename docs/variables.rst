.. _pprofit-variables:

#########
Variables
#########

The fitting system defines two types of variables. These are defined in the :ref:`[Variables] <pprofit-variables-variables>` and :ref:`[CalculatedVariables] <pprofit-variables-calculatedvariables>` sections of ``fit.cfg``. Conventional :ref:`[Variables] <pprofit-variables-variables>` can be modified by the :ref:`minimizer <pprofit-minimizers>` to obtain the merit function. As their name suggests, :ref:`[CalculatedVariables] <pprofit-variables-calculatedvariables>` are defined using arithmetic expressions which can contain references to conventional variables. Meaning that values can be passed to jobs that depend, however indirectly on fitting parameters. These have a number of uses, for instance to optimise for partial charges in MgO, an ionicity could be defined as a fitting parameter within the :ref:`[Variables] <pprofit-variables-variables>` section with :ref:`[CalculatedVariables] <pprofit-variables-calculatedvariables>` defined to ensure that Mg's charge was ``2 * ionicity`` and the O charge being ``-2 * ionicity``.


.. _pprofit-variables-variables:

[Variables]
===========

The following section details how fitting variables are defined to the fitting system. Variable definitions appear within the ``[Variables]`` configuration block of the ``fit.cfg`` file and have the following basic form (square brackets indicate that the section of the definition is optional)::

	VARIABLE_NAME : INITIAL_VALUE [(LOWERBOUND, UPPERBOUND)] [*]

Where:
	
	* ``VARIABLE_NAME`` is the label by which the variable is identified within the fitting system.

	* ``INITIAL_VALUE`` the starting value for the variable. 

	 Note certain :ref:`minimizers <pprofit-minimizers>` do not use this information. For example,  population based optimizers select a large number of candidate variable sets at random from a given set of variable ranges. This value must always be specified however.  See :ref:`minimizers <pprofit-minimizers>` for more information.

	* ``(LOWERBOUND, UPPERBOUND)`` specify the desired range for a particular variable. Some :ref:`minimizers <pprofit-minimizers>` treat these bounds as constraints, with the variable being prevented from going above ``UPPERBOUND`` or below ``LOWERBOUND``. Variable bounds are often used by population based :ref:`minimizers <pprofit-minimizers>` as the region of the variable space from which to choose an initial population of solutions. Again, please refer to each minimizer's documentation to establish how it treats bounds (see :ref:`pprofit-minimizers-reference`).

	* ``*``. An asterisk at the end of a variable line indicates that the variable should be optimised during fitting. If no asterisk appears, the variable will be held fixed.

Examples
^^^^^^^^

::

	[Variables]
	A : 10.0
	B : 5.1 *

\ 

	* ``A`` has initial value of 10.0 and should be held fixed during fitting.
	* ``B`` has initial value of 5.1 and should be optimised during fitting.


::

	[Variables]
	A : 1.23 (0.0, ) *
	B : 3.45 (0.0, 1.0) *

\ 

	* ``A`` has initial value of 1.23 and lower bound of 0.0 and no defined upper bounds.
	* ``B`` has an initial value of 3.45 and is bound to the range :math:`0.0 \leq B \leq 1.0`.
	* The ``*`` at the end of both lines shows both variables should be optimised during fitting.



.. _pprofit-variables-calculatedvariables:

[CalculatedVariables]
=====================

Calculated variables are defined with the optional ``[CalculatedVariables]`` section of the ``fit.cfg`` file and have the general form::

	VARIABLE_NAME : EXPRESSION


Where ``VARIABLE_NAME`` is the label used to refer to the calculated variable within the fitting system. ``EXPRESSION`` is an arithmetic expression which can contain references to variables from the :ref:`[Variables] <pprofit-variables-variables>` section. The functions and operators that can be used in expressions are the same as for the ``Formula`` meta-evaluator and are described :ref:`here <pprofit-metaevaluators-expressionsyntax>`.

Example
^^^^^^^

Partial charges are often adopted as part of a pair potential model. The formal charges for UO2 would be U=+4 and O=-2. Simply defining these charges within ``[Variables]``, defining placeholders within a job file and expecting the system to optimize for charge would be unlikely to work as charge neutrality would not be guaranteed. Instead, a single ``ionicity`` variable is defined within ``[Variables]``::

	[Variables]
	ionicity : 1.0 (0.1, 1.0) *

Note that a lower bound has been defined as, very low charges, whilst perhaps providing a better fit, may not be completely physical. The upper-bound of 1.0 means the partial charges will always less than or equal to the ion's formal charge.

The charges for uranium and oxygen would then be defined within ``[CalculatedVariables]`` as::

	[CalculatedVariables]
	Ocharge : -2.0 * ionicity
	Ucharge : 4.0 * ionicity

These variables should then be used in place of ``ionicity`` within a job's input file. If using the :ref:`Template <pprofit-jobfactories-template>` job-factory to create a GULP file, this would involve modifying the ``species`` section of the GULP input as follows::

	species
	U @Ucharge@
	O @Ocharge@

The fitting system should then be run as normal and the calculated variable values will be output both to the terminal and fitting monitor.


