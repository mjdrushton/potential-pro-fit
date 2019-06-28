from atsim.pro_fit.exceptions import ConfigException
from . import _gulp_parse

import collections
import os
import math
import inspect
import logging
import sys

_logger = logging.getLogger("atsim.pro_fit.evaluators")

from ._common import *  # noqa


class GulpDrvParser(object):
    """Parser for files generated by Gulp 'output drv' command"""

    StrainGradient = collections.namedtuple(
        "StrainGradient", ["xx", "yy", "zz", "yz", "xz", "xy"]
    )

    def __init__(self, infile):
        """@param infile Python file object containing drv data"""
        self.gradientsCartesian = None
        self.gradientsStrain = None
        self._parse(infile)

    def _parse(self, infile):
        for line in infile:
            if line.startswith("gradients cartesian eV/Ang"):
                self._parseGradientsCartesian(line, infile)
            elif line.startswith("gradients strain eV"):
                self._parseGradientsStrain(line, infile)

    def _parseGradientsCartesian(self, currline, infile):
        tokens = currline.split()
        numlines = tokens[-1]
        numline = int(numlines)

        retlist = []
        for _i in range(numline):
            line = next(infile)[:-1]
            tokens = line.split()[1:]
            g = tuple([float(v) for v in tokens])
            retlist.append(g)
        self.gradientsCartesian = retlist

    def _parseGradientsStrain(self, currline, infile):
        retlist = []
        tokens = next(infile)[:-1].split()
        tokens = [float(v) for v in tokens]
        retlist.extend(tokens)

        tokens = next(infile)[:-1].split()
        tokens = [float(v) for v in tokens]
        retlist.extend(tokens)
        self.gradientsStrain = self.StrainGradient(*retlist)


class Gulp_DRVEvaluator(object):
    """Evaluator for processing drv files generated by Gulp's 'output drv' command"""

    _ConfigItem = collections.namedtuple("_ConfigItem", ["name", "weight"])

    _individualGradientKeys = [
        "cell_xx",
        "cell_yy",
        "cell_zz",
        "cell_yz",
        "cell_xz",
        "cell_xy",
    ]

    def __init__(self, filename, evaluatorName, configitems):
        self._filename = filename
        self._configItems = configitems
        self._evaluatorname = evaluatorName

    def _vectorMagnitude(self, vector):
        sqmag = sum([float(v) ** 2.0 for v in vector])
        return math.sqrt(sqmag)

    def __call__(self, job):
        try:
            parser = self._createParser(job)
        except Exception as e:
            return [
                ErrorEvaluatorRecord(
                    configitem.name,
                    0.0,
                    e,
                    weight=configitem.weight,
                    evaluatorName=self._evaluatorname,
                )
                for configitem in self._configItems
            ]

        evalrecords = []

        for configitem in self._configItems:
            try:
                if configitem.name == "cell_gradients":
                    val = self._cell_gradients(parser)
                elif configitem.name == "atom_gradients":
                    val = self._atom_gradients(parser)
                elif configitem.name.startswith("cell_"):
                    val = self._individual_cell_gradient(
                        configitem.name, parser
                    )
                else:
                    # SHouldn't get here but ...
                    raise KeyError("Unknown value name: %s" % (configitem.name))

                er = RMSEvaluatorRecord(
                    configitem.name,
                    0.0,
                    val,
                    weight=configitem.weight,
                    evaluatorName=self._evaluatorname,
                )
            except Exception as e:
                er = ErrorEvaluatorRecord(
                    configitem.name,
                    0.0,
                    e,
                    weight=configitem.weight,
                    evaluatorName=self._evaluatorname,
                )
            evalrecords.append(er)
        return evalrecords

    def _createParser(self, job):
        outputpath = os.path.join(job.outputPath, self._filename)
        with open(outputpath, "r") as infile:
            parser = GulpDrvParser(infile)
        return parser

    def _atom_gradients(self, parser):
        gradients = parser.gradientsCartesian
        gradients = [self._vectorMagnitude(v) for v in gradients]
        return sum(gradients)

    def _individual_cell_gradient(self, n, parser):
        assert n.startswith("cell_")
        n = n[5:]
        gradients = parser.gradientsStrain
        v = getattr(gradients, n)
        return v

    def _cell_gradients(self, parser):
        vals = [
            self._individual_cell_gradient(k, parser)
            for k in self._individualGradientKeys
        ]
        vals = [math.fabs(v) for v in vals]
        return sum(vals)

    @staticmethod
    def createFromConfig(name, jobpath, cfgitems):
        allowedFields = ["atom_gradients", "cell_gradients"]
        allowedFields.extend(Gulp_DRVEvaluator._individualGradientKeys)
        allowedFields = set(allowedFields)

        # Get the filename field
        cfgdict = dict(cfgitems)
        del cfgdict["type"]
        vals = []
        try:
            filename = cfgdict["filename"]
            del cfgdict["filename"]
        except KeyError:
            raise ConfigException(
                "Could not find required 'filename' field for Gulp_DRV evaluator: '%s' for job: '%s'"
                % (name, jobpath)
            )

        for k, w in cfgdict.items():
            if not k in allowedFields:
                raise ConfigException(
                    "Unknown field '%s' for Gulp_DRV evaluator: '%s' for job: '%s'"
                    % (k, name, jobpath)
                )

            try:
                w = float(w)
            except ValueError:
                raise ConfigException(
                    "Could not parse weight value '%s'into float for field '%s' for Gulp_DRV evaluator: '%s' for job: '%s'"
                    % (w, k, name, jobpath)
                )

            vals.append(Gulp_DRVEvaluator._ConfigItem(k, w))

        return Gulp_DRVEvaluator(filename, name, vals)


class GulpEvaluatorException(Exception):
    pass


class _GulpSubEvaluatorBase(object):
    def __init__(self, key, expect, weight):
        self.parseKey(key)
        self._expect = expect
        self._weight = weight
        self._name = key

    def parseKey(self, key):
        self._key = key.strip()

    @staticmethod
    def getAllowedFields():
        return []

    def __call__(self, gulpEvaluator, job):
        outputpath = os.path.join(
            job.outputPath, gulpEvaluator.gulpOutputFilename
        )
        with open(outputpath) as infile:
            actual = self.extractValue(infile)
            return FractionalDifferenceEvaluatorRecord(
                self._name, self._expect, actual, self._weight
            )

    def extractValue(self, infile):
        raise NotImplementedError()


class _GulpElasticSubEvaluator(_GulpSubEvaluatorBase):
    """Extracts values from Gulp output files elastic constant matrix"""

    def parseKey(self, key):
        key = key.strip()
        r = int(key[-2]) - 1
        c = int(key[-1]) - 1
        self._row, self._col = r, c

    def extractValue(self, infile):
        mat = _gulp_parse.parseElasticConstantMatrix(infile)
        return mat[self._row][self._col]

    @staticmethod
    def getAllowedFields():
        allowedFields = []
        for row in range(6):
            row = row + 1
            for col in range(6):
                col = col + 1
                allowedFields.append("elastic_c%d%d" % (row, col))
        return allowedFields


class _GulpBulkModulusSubEvaluator(_GulpSubEvaluatorBase):
    """Extracts bulk modulus from Gulp output files"""

    def parseKey(self, key):
        key = key.strip()
        tokens = key.split("_")
        self._modtype = tokens[1]

    def extractValue(self, infile):
        mechanicalproperties = _gulp_parse.parseMechanicalProperties(infile)
        return mechanicalproperties["bulkModulus"][self._modtype]

    @staticmethod
    def getAllowedFields():
        return ["bulkmodulus_reuss", "bulkmodulus_voigt", "bulkmodulus_hill"]


class _GulpShearModulusSubEvaluator(_GulpSubEvaluatorBase):
    """Extracts shear modulus from Gulp output files"""

    def parseKey(self, key):
        key = key.strip()
        tokens = key.split("_")
        self._modtype = tokens[1]

    def extractValue(self, infile):
        mechanicalproperties = _gulp_parse.parseMechanicalProperties(infile)
        return mechanicalproperties["shearModulus"][self._modtype]

    @staticmethod
    def getAllowedFields():
        return ["shearmodulus_reuss", "shearmodulus_voigt", "shearmodulus_hill"]


class _GulpCellSubEvaluator(_GulpSubEvaluatorBase):
    """Extracts cell information from gulp output"""

    _keyTranslate = {
        "cell_a": "a",
        "cell_b": "b",
        "cell_c": "c",
        "cell_alpha": "alpha",
        "cell_beta": "beta",
        "cell_gamma": "gamma",
    }

    def parseKey(self, key):
        self._key = self._keyTranslate[key]

    def extractValue(self, infile):
        celldict = _gulp_parse.parseFinalCellParametersAndDerivatives(infile)
        return celldict[self._key]

    @staticmethod
    def getAllowedFields():
        return sorted(_GulpCellSubEvaluator._keyTranslate.keys())


class _GulpEnergySubEvaluator(_GulpSubEvaluatorBase):
    """Extracts energy from gulp output """

    def extractValue(self, infile):
        energydict = _gulp_parse.parseComponentsOfEnergy(infile)[
            "componentsOfEnergyAtEnd"
        ]
        return energydict["totalLatticeEnergy"]

    @staticmethod
    def getAllowedFields():
        return ["lattice_energy"]


class _GulpEnergyAtStartSubEvaluator(_GulpSubEvaluatorBase):
    """Extracts energy from gulp output """

    def extractValue(self, infile):
        gulpdict = _gulp_parse.parseComponentsOfEnergy(infile)
        energydict = gulpdict["componentsOfEnergyAtStart"]
        return energydict["totalLatticeEnergy"]

    @staticmethod
    def getAllowedFields():
        return ["lattice_energy_at_start"]


def _find(infile, linestart):
    for line in infile:
        if line.startswith(linestart):
            return line
    raise GulpEvaluatorException(
        "Couldn't find line starting with: %s" % linestart
    )


class _GulpOptimisationPenaltySubEvaluator(object):
    """Provides a penalty if optimisation was unsuccessful"""

    def __init__(self, key, expect, weight):
        self._weight = expect
        self._expect = 0.0
        self._name = key

    def __call__(self, gulpEvaluator, job):
        outputpath = os.path.join(
            job.outputPath, gulpEvaluator.gulpOutputFilename
        )
        with open(outputpath) as infile:
            _find(infile, "*  Output for configuration")
            _find(infile, "  Start of bulk optimisation :")
            line = _find(infile, "  **** ")

            if line.strip() == "**** Optimisation achieved ****":
                penalty = 0.0
            else:
                penalty = 1.0
        return EvaluatorRecord(
            "optimisation_penalty",
            0.0,
            penalty,
            self._weight,
            penalty * self._weight,
        )

    @staticmethod
    def getAllowedFields():
        return ["optimisation_penalty"]


class _GulpNegativePhononPenaltySubEvaluator(object):
    """Provides a penalty if negative phonons are found"""

    def __init__(self, key, expect, weight):
        self._weight = expect
        self._expect = 0.0
        self._name = key

    def __call__(self, gulpEvaluator, job):
        outputpath = os.path.join(
            job.outputPath, gulpEvaluator.gulpOutputFilename
        )
        with open(outputpath) as infile:
            _find(infile, "*  Output for configuration")
            _find(infile, "  Phonon Calculation :")
            line = _find(
                infile, "  Number of k points for this configuration ="
            )
            tokens = line.split("=")
            numpoints = int(tokens[1].strip())
            line = _find(infile, "  K point")
            if self._anyNegative(numpoints, infile):
                penalty = 1.0
            else:
                penalty = 0.0
        return EvaluatorRecord(
            "negative_phonon_penalty",
            0.0,
            penalty,
            self._weight,
            self._weight * penalty,
        )

    def _anyNegative(self, numblocks, infile):
        for i in range(numblocks):
            next(infile)
            next(infile)
            next(infile)
            next(infile)
            for line in infile:
                line = line.strip()
                if not line:
                    break
                tokens = line.split()
                for v in tokens:
                    v = float(v)
                    if v < 0.0:
                        return True
            if i + 1 < numblocks:
                line = _find(infile, "  K point")
        return False

    @staticmethod
    def getAllowedFields():
        return ["negative_phonon_penalty"]


class GulpEvaluator(object):
    """Evaluator for Gulp files"""

    def __init__(self, name, gulpOutputFilename, keyExpectPairs):
        """@param name Name of evaluator
    @param gulpOutputFilename Name of gulp output file from which values are extracted
    @param keyExpectPairs List of (observable_name, observable_name, observable_weight)"""
        self.name = name
        self.gulpOutputFilename = gulpOutputFilename
        self._subEvaluators = self._createSubEvaluators(keyExpectPairs)

    def _subevaluate(self, job):
        subevalled = []
        for e in self._subEvaluators:
            try:
                subval = e(self, job)
            except Exception as exc:
                subval = ErrorEvaluatorRecord(
                    e._name, e._expect, exc, e._weight
                )
            subevalled.append(subval)
        return subevalled

    def _createSubEvaluators(self, keyExpectPairs):
        evalclasses = _evaluatorClasses()
        keytoclassdict = {}

        for cls in evalclasses:
            for k in cls.getAllowedFields():
                keytoclassdict[k] = cls

        subeval = []
        for k, v, w in keyExpectPairs:
            cls = keytoclassdict[k]
            subeval.append(cls(k, v, w))
        return subeval

    def __call__(self, job):
        evalled = self._subevaluate(job)

        for v in evalled:
            v.evaluatorName = self.name
        return evalled

    @staticmethod
    def createFromConfig(name, jobpath, cfgitems):
        cfgdict = dict(cfgitems)
        try:
            gulpOutputFilename = cfgdict["filename"]
        except KeyError:
            raise ConfigException(
                "'filename' record not found for GULP Evaluator '%s'" % name
            )

        del cfgdict["filename"]
        del cfgdict["type"]

        keyExpectPairs = []
        for k, v in cfgdict.items():
            k = k.strip()
            if not k in _allowedGulpFields:
                raise ConfigException(
                    "'%s' unknown key for GULP Evaluator '%s'" % (k, name)
                )

            tokens = v.split()
            if len(tokens) == 2:
                try:
                    weight = float(tokens[1])
                except ValueError:
                    raise ConfigException(
                        "'%s' record's weight not a valid float for GULP Evaluator '%s': '%s'"
                        % (k, name, tokens[1])
                    )
            else:
                weight = 1.0

            try:
                expectValue = float(tokens[0])
            except ValueError:
                raise ConfigException(
                    "'%s' record's expect not a valid float for GULP Evaluator '%s': '%s' "
                    % (k, name, tokens[0])
                )

            keyExpectPairs.append((k, expectValue, weight))
        return GulpEvaluator(name, gulpOutputFilename, keyExpectPairs)


# List of fields supported by
def _evaluatorClasses():
    evalclasses = []
    module = sys.modules[__name__]
    for name, cls in inspect.getmembers(module, inspect.isclass):
        if name.endswith("SubEvaluator"):
            evalclasses.append(cls)
    return evalclasses


def _allowedFields():
    allowed = []
    for cls in _evaluatorClasses():
        allowed.extend(cls.getAllowedFields())
    _logger.debug("Gulp Evaluator allowed field names: %s" % allowed)
    return set(allowed)


_allowedGulpFields = _allowedFields()
