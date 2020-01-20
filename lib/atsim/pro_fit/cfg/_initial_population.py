import sys

from atsim.pro_fit.exceptions import ConfigException
from atsim.pro_fit.variables import VariableException

from atsim.pro_fit.minimizers import population_generators


class Population_To_Generator_Adapter(object):
    """Class the adapts initial population classes from
    atsim.pro_fit.minimizers.population_generators for use
    as generators for use with the inspyred minimizers.

    This class must be instantiated with an Initial_Population
    object that has a population size of 1. """

    def __init__(self, initial_variables, population):
        assert population.population_size == 1
        self.initialVariables = initial_variables
        self.population = population

    def __call__(self, *args):
        """ Generator.

    @return Candidate."""
        candidates = self.population.generate_candidates()
        candidate = candidates[0].tolist()
        return candidate


class _Initial_Population_Factory(object):
    def __init__(
        self,
        population_size,
        population_include_orig_vars,
        population_load_from_csv,
        population_load_from_ppdump,
        population_distribution,
    ):
        self.initialVariables = None
        self.populations = None
        self.generator = None

        self.population_size = population_size
        self.population_include_orig_vars = population_include_orig_vars

        self.population_load_from_csv = population_load_from_csv
        self.population_load_from_ppdump = population_load_from_ppdump
        self.population_loader = self._init_population_loader()

        self.population_distribution = population_distribution

    def _init_population_loader(self):
        if self.population_load_from_csv:
            cls = population_generators.File_Initial_Population
            filename = self.population_load_from_csv
        elif self.population_load_from_ppdump:
            cls = population_generators.Ppdump_File_Initial_Population
            filename = self.population_load_from_ppdump
        else:
            return None

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
            popn = population_generators.Combine_Initial_Population(*popns)
            return popn

    def _create_default_distribution(self, population_size):
        vds = self._distribution_factory.variable_distributions
        candidate_generator = population_generators.Candidate_Generator(vds)

        popn = population_generators.Latin_Hypercube_InitialPopulation(
            self.initialVariables,
            population_size,
            population_generators.Latin_Hypercube_InitialPopulation.Criterion.correlation,
            candidate_generator=candidate_generator,
        )

        return popn

    def _create_init_variables(self, population_size):
        orig_vars = population_generators.Predefined_Initial_Population(
            self.initialVariables, from_array=[self.initialVariables.fitValues]
        )
        return orig_vars

    def __call__(self, initialVariables):
        self.initialVariables = initialVariables
        self._distribution_factory = _Distribution_Factory(
            self.initialVariables, self.population_distribution
        )
        self.population = self._init_population()
        return self


class _Distribution_Factory:
    available_choices = ["uniform", "bias"]

    def __init__(self, initialVariables, population_distribution):
        self.initialVariables = initialVariables
        self._population_distribution = population_distribution
        self.variable_distributions = self._init_distribution()

    def _init_distribution(self):
        chosen_distn = self._population_distribution
        chosen_distn = chosen_distn.strip()
        if chosen_distn.startswith("uniform"):
            return self._create_uniform_distribution()
        elif chosen_distn.startswith("bias"):
            return self._create_bias_distribution(chosen_distn)
        raise ConfigException(
            "Unknown population_distribution: '{}'".format(chosen_distn)
        )

    def _create_uniform_distribution(self):
        distn = population_generators.Uniform_Variable_Distributions(
            self.initialVariables
        )
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

        distn = population_generators.PERT_Variable_Distributions(
            self.initialVariables, shape=shape
        )
        return distn


def _population_factory(
    population_size,
    population_include_orig_vars,
    population_load_from_csv,
    population_load_from_ppdump,
    population_distribution,
):
    obj = _Initial_Population_Factory(
        population_size,
        population_include_orig_vars,
        population_load_from_csv,
        population_load_from_ppdump,
        population_distribution,
    )

    return obj


def add_initial_population_options(cfgparse):
    sub_parser = cfgparse.add_sub_parser("population", _population_factory)

    sub_parser.add_int_option(
        "population_size",
        "population_size",
        bounds=(2, sys.maxsize),
        default=100,
    ).add_boolean_option(
        "population_include_orig_vars",
        "population_include_orig_vars",
        default=True,
    ).add_infile_option(
        "population_load_from_csv", "population_load_from_csv", default=None
    ).add_infile_option(
        "population_load_from_ppdump",
        "population_load_from_ppdump",
        default=None,
    ).add_str_option(
        "population_distribution",
        "population_distribution",
        default="uniform",
    )

    sub_parser.add_mutually_exclusive_constraint(
        "population_load_from_ppdump", "population_load_from_csv"
    )
