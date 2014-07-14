.. _pprofit-configuration_reference:

#######################
Configuration Reference
#######################

Fitting runs are configured either at the run or job level. 

  * Run-level configuration appears within ``fit.cfg`` within the root directory of the fitting run. 
  * Jobs are configured using the ``job.cfg`` from each fitting run's ``fit-files`` directory. The contents of ``job.cfg`` files is described :ref:`here <pprofit-jobfactories>`.

Pages relating to general configuration:

.. toctree::
  mathsexpressions.rst 


Fitting Run Configuration
=========================
The following lists configuration entries that can appear in ``fit.cfg``:

.. toctree::
    fittingruncontrol
    variables
    minimizers/minimizers.rst
    runners
    metaevaluators
	
Job Configuration
=================
The following lists documentation relating to ``job.cfg`` configuration files:	

.. toctree::
    evaluators  
    jobfactories



	