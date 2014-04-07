##########
Known Bugs
##########

  * File cleanup following jobs is sometimes a little hit and miss.

  * Web monitor:
    
    - Printing. The screen stylesheet makes printing mostly pointless.

   * Defining a runner but having no jobs assigned to it causes fittingTool.py
   to fail.

   * The operator precedence in the expression parser used for CalculatedVariables and the Formula meta evaluator is probably wrong.

   * Having a job that doesn't run anything causes the SQLReporter to fail.