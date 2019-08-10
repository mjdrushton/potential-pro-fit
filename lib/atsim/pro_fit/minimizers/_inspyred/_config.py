from ._inspyred_common import Population_To_Generator_Adapter

from atsim.pro_fit.cfg import (
    int_convert,
    random_seed_option,
    boolean_convert,
    infile_convert,
    choice_convert,
)

from atsim.pro_fit.minimizers.population_generators import (
    Predefined_Initial_Population,
    Latin_Hypercube_InitialPopulation,
    Combine_Initial_Population,
    Uniform_Random_Initial_Population,
    File_Initial_Population,
    Ppdump_File_Initial_Population,
    Candidate_Generator,
    Uniform_Variable_Distributions,
    PERT_Variable_Distributions,
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
                int_convert(clsname, "max_iterations", (1, float("inf"))),
            ),
            population_size=(
                update_defaults.get("population_size", 64),
                int_convert(clsname, "population_size", (2, float("inf"))),
            ),
            random_seed=(
                update_defaults.get("random_seed", None),
                random_seed_option(clsname, "random_seed"),
            ),
            population_include_orig_vars=(
                update_defaults.get("population_include_orig_vars", "True"),
                boolean_convert(clsname, "population_include_orig_vars"),
            ),
            population_load_from_csv=(
                update_defaults.get("population_load_from_csv", None),
                infile_convert(clsname, "population_load_from_csv"),
            ),
            population_load_from_ppdump=(
                update_defaults.get("population_load_from_ppdump", None),
                infile_convert(clsname, "population_load_from_ppdump"),
            ),
        )
        _Initial_Population_Factory.update_default_dict(
            clsname, defaults, update_defaults
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

        generator = population_factory.generator

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

        self._distribution_factory = _Distribution_Factory(
            self.initialVariables, optiondict
        )
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

        popn_init_callables.append(self._create_default_distribution)

        self.generator = Population_To_Generator_Adapter(
            self.initialVariables, self._create_default_distribution(1)
        )

        popns = []
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

    def _create_default_distribution(self, population_size):
        vds = self._distribution_factory.variable_distributions
        candidate_generator = Candidate_Generator(vds)

        popn = Latin_Hypercube_InitialPopulation(
            self.initialVariables,
            population_size,
            Latin_Hypercube_InitialPopulation.Criterion.correlation,
            candidate_generator=candidate_generator,
        )

        return popn

    def _create_init_variables(self, population_size):
        orig_vars = Predefined_Initial_Population(
            self.initialVariables, from_array=[self.initialVariables.fitValues]
        )
        return orig_vars

    @classmethod
    def update_default_dict(cls, clsname, default_dict, update_defaults):
        _Distribution_Factory.update_default_dict(
            clsname, default_dict, update_defaults
        )


class _Distribution_Factory:
    available_choices = ["uniform", "bias"]

    def __init__(self, initialVariables, optiondict):
        self.initialVariables = initialVariables
        self.variable_distributions = self._init_distribution(optiondict)

    def _init_distribution(self, optiondict):
        chosen_distn = optiondict.get("population_distribution", "uniform")
        chosen_distn = chosen_distn.strip()
        if chosen_distn.startswith("uniform"):
            return self._create_uniform_distribution()
        elif chosen_distn.startswith("bias"):
            return self._create_bias_distribution(chosen_distn)
        raise ConfigException(
            "Unknown population_distribution: '{}'".format(chosen_distn)
        )

    def _create_uniform_distribution(self):
        distn = Uniform_Variable_Distributions(self.initialVariables)
        return distn

    def _create_bias_distribution(self, distn_string):
        tokens = distn_string.split()
        if not tokens[0] == "bias":
            raise ConfigException(
                "Unknown population_distribution: '{}'".format(distn_string)
            )

        if len(tokens) > 1:
            shape = tokens[1]
            try:
                shape = float(shape)
            except ValueError:
                raise ConfigException(
                    "Could not parse 'shape' argument for 'population_distribution' : '{}'".format(
                        distn_string
                    )
                )
        else:
            shape = 10.0

        distn = PERT_Variable_Distributions(self.initialVariables, shape=shape)
        return distn

    @classmethod
    def update_default_dict(cls, clsname, default_dict, update_defaults):
        d = dict(
            population_distribution=(
                update_defaults.get("population_distribution", "uniform"),
                choice_convert(
                    clsname, "population_distribution", cls.available_choices
                ),
            )
        )

        default_dict.update(d)
