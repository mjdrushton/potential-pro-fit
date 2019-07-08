from ._inspyred_common import (
    _IntConvert,
    _FloatConvert,
    _RandomSeed,
    _ChoiceConvert,
    _RandomSeed,
    _BooleanConvert,
    _InfileConvert,
    Population_To_Generator_Adapter,
)

from atsim.pro_fit.minimizers.population_generators import (
    Predefined_Initial_Population,
    Latin_Hypercube_InitialPopulation,
    Combine_Initial_Population,
    Uniform_Random_Initial_Population,
    File_Initial_Population,
    Ppdump_File_Initial_Population,
)

from atsim.pro_fit.exceptions import ConfigException
from atsim.pro_fit.variables import VariableException


class Initial_Population_Config_Helper(object):
    """Helper class for configuring population minimizers.

    This class will parse the following keys from 
    the configuration dict used in the minimizer's createFromConfig()
    method:
        * max_iterations
        * population_include_orig_vars
        * population_load_from_csv
        * population_load_from_ppdump
        * population_size
        * random_seed


    It also acts as a factory class for the generator and 
    initial_population objects required by these minimizers."""

    def __init__(self, clsname, **defaults):
        self.clsname = clsname
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
            population_load_from_csv=(
                update_defaults.get("population_load_from_csv", None),
                _InfileConvert(clsname, "population_load_from_csv"),
            ),
            population_load_from_ppdump=(
                update_defaults.get("population_load_from_ppdump", None),
                _InfileConvert(clsname, "population_load_from_ppdump"),
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

        try:
            population_factory = _Initial_Population_Factory(
                variables, optiondict
            )
        except ConfigException as e:
            raise ConfigException("{} for {}".format(str(e), self.clsname))
        except VariableException as ve:
            raise ConfigException(
                "Problem with variables {} for {}".format(
                    str(ve), self.clsname
                )
            )

        optiondict["initial_population"] = population_factory.population

        generator = Population_To_Generator_Adapter(
            variables, Uniform_Random_Initial_Population(variables, 1)
        )

        optiondict["generator"] = generator

        return optiondict


class _Initial_Population_Factory(object):
    def __init__(self, initialVariables, optiondict):
        self.initialVariables = initialVariables

        self.population_size = optiondict["population_size"]
        self.population_include_orig_vars = optiondict[
            "population_include_orig_vars"
        ]

        self.population_loader = self._init_population_loader(optiondict)
        self.population = self._init_population()

    def _init_population_loader(self, optiondict):
        relevant = {
            "population_load_from_csv": File_Initial_Population,
            "population_load_from_ppdump": Ppdump_File_Initial_Population,
        }

        relevant_keys = list(relevant.keys())
        relevant_keys_found = [
            k
            for k, v in optiondict.items()
            if k in relevant_keys and v is not None
        ]

        if len(relevant_keys_found) == 0:
            return None

        if len(relevant_keys_found) > 1:
            raise ConfigException(
                "only one of {} can be specified at once".format(
                    ",".join(
                        ["'{}'".format(opt) for opt in relevant_keys_found]
                    )
                )
            )

        k = relevant_keys_found[0]
        v = optiondict[k]
        cls = relevant[k]
        filename = v

        def f(max_population_size):
            infile = open(filename)
            popn = cls(
                self.initialVariables,
                infile,
                max_population_size=max_population_size,
            )

            return popn

        return f

    def _init_population(self):
        population_size = self.population_size

        popn_init_callables = []

        if self.population_include_orig_vars:
            popn_init_callables.append(self._create_init_variables)

        if self.population_loader:
            popn_init_callables.append(self.population_loader)

        popn_init_callables.append(self._create_latin_hypercube)

        popns= []
        for c in popn_init_callables:
            p = c(population_size)
            popns.append(p)
            population_size -= p.population_size
            if population_size < 1:
                break

        if len(popns) == 1:
            return popns[0]
        else:
            popn = Combine_Initial_Population(*popns)
            return popn

    def _create_latin_hypercube(self, population_size):
        popn = Latin_Hypercube_InitialPopulation(
            self.initialVariables,
            population_size,
            Latin_Hypercube_InitialPopulation.Criterion.correlation,
        )
        return popn

    def _create_init_variables(self, population_size):
        orig_vars = Predefined_Initial_Population(
                self.initialVariables,
                from_array=[self.initialVariables.fitValues],
            )
        return orig_vars