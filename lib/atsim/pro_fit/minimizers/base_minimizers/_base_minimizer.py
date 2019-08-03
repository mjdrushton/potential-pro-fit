

def NullStepCallback(minimizerResults):
    pass


class Abstract_Base_Minimizer(object):
    """Abstract base class that demonstrated the basic minimizer interface.
    
      class Minimizer:

        def minimize(self, merit):
            ...

        def stopMinimizer():
            ...

        @classmethod
        def createFromConfig(variables, configitems):
            ...

    In addition classes implementing the Minimizer interface should support
    a property named 'stepCallback', this is called after each minimisation iteration.
    This supports logging and progress monitoring. The callback is callable with the
    function prototype:

    def stepCallback(minimizerResults):
        @param minimizerResults Instance of atsim.pro_fit.minimizers.MinimizerResults
                    By convention the minimizerResults should contain the best results
                    from the last minimiztion iteration (this is because several minimizers
                    perform several sub-steps before finalizing variable updates and completing
                    a single iteration).
        ...
    """

    def __init__(self):

        """Callable called at the end of each minimization step. 
        This should be called with a single argument which is an instance of MinimizerResult"""
        self.stepCallBack = NullStepCallback

    def minimize(self, merit):
        """Performs minimization. 

        Arguments:
            merit {atsim.profit.merit.Merit} -- Merit function to be evaluated. 
                This is a callable that takes lists of `atsim.pro_fit.variables.Variables` instances
                and evaluates their merit.

        Returns:
            {atsim.pro_fit.minimizers.MinimizerResults} -- object representing the results of minimization.
        
        """
        raise NotImplementedError("Inheriting classes should implement minimize() method")


    def stopMinimizer(self):
        """May be called after minimization has started using `minimize()`. This method terminates
        the minimization loop."""
        raise NotImplementedError("Inheriting classes should implement stopMinimizer() method")

    @classmethod
    def createFromConfig(cls, variables, configitems):
        """Configures an instance of this minimizer from a list of configuration items and variables

        
        Arguments:
            variables {atsim.pro_fit.variables.Variables} -- Variables defined in fit.cfg file
            configitems {list} -- list of (OPTION_NAME, OPTION_VALUE) string tuples relevant to this minimizer.

        Returns:
            {instance of cls} -- returns an instance of this minimizer.
        """
        raise NotImplementedError("Inheriting classes should implement createFromConfig() class method")
