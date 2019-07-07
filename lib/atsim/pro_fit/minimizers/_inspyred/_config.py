from ._inspyred_common import (
    _IntConvert,
    _FloatConvert,
    _RandomSeed,
    _ChoiceConvert,
    _RandomSeed,
    _BooleanConvert,
    Population_To_Generator_Adapter,
)

from atsim.pro_fit.minimizers.population_generators import (
    Predefined_Initial_Population,
    Latin_Hypercube_InitialPopulation,
    Combine_Initial_Population,
    Uniform_Random_Initial_Population,
)


class Initial_Population_Config_Helper(object):
    """Helper class for configuring population minimizers.

    This class will parse the following keys from 
    the configuration dict used in the minimizer's createFromConfig()
    method:
        * population_size
        * population_include_orig_vars
        * random_seed
        * max_iterations

    It also acts as a factory class for the generator and 
    initial_population objects required by these minimizers."""

    def __init__(self, clsname, **defaults):
        self.defaults_dict = self._init_defaults(clsname, defaults)

    def _init_defaults(self, clsname, update_defaults):
        defaults = dict(
            max_iterations=(
                update_defaults.get("max_iterations", 1000),
                _IntConvert(clsname, "max_iterations", (1, float("inf"))),
            ),
            population_size=(
                update_defaults.get("population_size", 64),
                _IntConvert(clsname, "population_size", (2, float("inf"))),
            ),
            random_seed=(
                update_defaults.get("random_seed", None),
                _RandomSeed(clsname, "random_seed"),
            ),
            population_include_orig_vars=(
                update_defaults.get("population_include_orig_vars", "True"),
                _BooleanConvert(clsname, "population_include_orig_vars"),
            ),
        )
        return defaults

    @property
    def default_keys(self):
        return sorted(self.defaults_dict.keys())

    def parse(self, variables, cfgdict):
        optiondict = {}
        for k, (default, converter) in self.defaults_dict.items():
            optiondict[k] = converter(cfgdict.get(k, default))

        population_factory = _Initial_Population_Factory(
            variables,
            optiondict["population_size"],
            optiondict["population_include_orig_vars"],
        )
        optiondict["initial_population"] = population_factory.population

        generator = Population_To_Generator_Adapter(
            variables, Uniform_Random_Initial_Population(variables, 1)
        )

        optiondict["generator"] = generator

        return optiondict


class _Initial_Population_Factory(object):
    def __init__(
        self, initialVariables, population_size, population_include_orig_vars
    ):
        self.initialVariables = initialVariables
        self.population_size = population_size
        self.population_include_orig_vars = population_include_orig_vars

        self.population = self._init_population()

    def _init_population(self):
        population_size = self.population_size
        if self.population_include_orig_vars:
            orig_vars = Predefined_Initial_Population(
                self.initialVariables,
                from_array=[self.initialVariables.fitValues],
            )
            population_size -= 1
            if population_size == 0:
                return orig_vars

            rest_of_population = self._create_latin_hypercube(population_size)

            # Combine the two
            population = Combine_Initial_Population(
                orig_vars, rest_of_population
            )
            return population
        else:
            population = self._create_latin_hypercube(population_size)
            return population

    def _create_latin_hypercube(self, population_size):
        popn = Latin_Hypercube_InitialPopulation(
            self.initialVariables,
            population_size,
            Latin_Hypercube_InitialPopulation.Criterion.correlation,
        )
        return popn
