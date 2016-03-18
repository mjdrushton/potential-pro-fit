.. _pprofit-minimizers:

##########
Minimizers 
##########

Minimizers are the entities within Potential Pro-Fit responsible for choosing candidate parameter sets and iteratively optimising them to minimize an objective function. In simpler terms, the minimizer gradually changes an initial set of input variables to improve the property predictions yielded by the potential set.

A single minimizer is configured for each fitting run within the ``fit.cfg`` file by specifying a ``[Minimizer]`` block. This must, as a minimum, specify a ``type`` field selecting the minimizer and any other field required by a particular minimizer (see :ref:`pprofit-minimizers-reference`). For example the :ref:`NelderMead <pprofit-minimizers-neldermead>` minimizer can be selected in this way::

  [Minimizer]
  type : NelderMead

.. _pprofit-minimizers-reference:

Minimizer Reference
-------------------

.. include:: dea.txt

.. include:: nelder_mead.txt

.. include:: particle_swarm.txt

.. include:: simulated_annealing.txt

.. include:: single.txt

.. include:: spreadsheet.txt

