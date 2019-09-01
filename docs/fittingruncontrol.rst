.. _pprofit-fittingruncontrol:

##########################
Global Fitting Run Options
##########################

The ``[FittingRun]`` section of the ``fit.cfg`` file contains options relevant to the entire fitting run sucha fitting run meta-information such as the run's title.

[FittingRun] Options
====================

:Name: title
:Description: Name of the fitting run. This is displayed in ``pprofitmon`` and is particularly useful for distinguising between multiple fitting runs.

\

:Name: bad_merit_substitute
:Type: +ve float
:Description: This option can be used to replace bad evaluator values. If an evaluator record has a merit value of nan it will be replaced by this option's value.

\
