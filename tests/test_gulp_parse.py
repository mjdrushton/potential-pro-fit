import unittest
import types
import os

from atsim.pro_fit.evaluators import _gulp_parse

from . import testutil

"""Module for atsim.pro_fit.evaluators._gulp_parse"""


def _getResourceDirectory():
    """Returns path to resources used by this test module (currently assumed to be sub-directory
    of test module called resources)"""
    return os.path.join(os.path.dirname(__file__), "resources", "gulp_parse")


class GulpParseOutputConfigurationTestCase(unittest.TestCase):
    """Tests for functions that parse values from gulp output configuration sections"""

    def testParseComponentsOfEnergy(self):
        """Test for atsim.pro_fit.evaluators._gulp_parse.parseComponentsOfEnergy()"""
        testfile = open(
            os.path.join(
                _getResourceDirectory(), "opti_conp_prop_outputsection.res"
            )
        )
        expect = {
            "componentsOfEnergyAtStart": {
                "interatomicPotentials": 26.88207570,
                "monopoleMonopoleReal": -57.34563079,
                "monopoleMonopoleRecip": -133.83771541,
                "monopoleMonopoleTotal": -191.18334619,
                "totalLatticeEnergy": -164.30127050,
                "totalLatticeEnergykJPerMoleUnitCells": -15852.5610,
            },
            "componentsOfEnergyAtEnd": {
                "interatomicPotentials": 26.88207570,
                "monopoleMonopoleReal": -57.34563079,
                "monopoleMonopoleRecip": -133.83771541,
                "monopoleMonopoleTotal": -191.18334619,
                "totalLatticeEnergy": -164.30127050,
                "totalLatticeEnergykJPerMoleUnitCells": -15852.5610,
            },
        }
        actual = _gulp_parse.parseComponentsOfEnergy(testfile)
        testutil.compareCollection(self, expect, actual)

        # Now perform a test for a file using EAM potentials
        testfile = open(
            os.path.join(
                _getResourceDirectory(),
                "opti_conp_prop_comp_eam_outputsection.res",
            ),
            "r",
        )
        expect = {
            "componentsOfEnergyAtStart": {
                "interatomicPotentials": 87.44928342,
                "manyBodyPotentials": -114.95934497,
                "monopoleMonopoleReal": 0.00,
                "monopoleMonopoleRecip": 0.00,
                "monopoleMonopoleTotal": 0.00,
                "totalLatticeEnergy": -27.51006155,
                "totalLatticeEnergykJPerMoleUnitCells": -2654.3004,
            },
            "componentsOfEnergyAtEnd": {
                "interatomicPotentials": 35.84080889,
                "manyBodyPotentials": -90.78086576,
                "monopoleMonopoleReal": 0.00,
                "monopoleMonopoleRecip": 0.00,
                "monopoleMonopoleTotal": 0.00,
                "totalLatticeEnergy": -54.94005687,
                "totalLatticeEnergykJPerMoleUnitCells": -5300.8757,
            },
        }
        actual = _gulp_parse.parseComponentsOfEnergy(testfile)
        testutil.compareCollection(self, expect, actual)

    def testParseFinalCellParametersAndDerivatives(self):
        """Test for parseFinalCellParametersAndDerivatives()"""
        testfile = open(
            os.path.join(
                _getResourceDirectory(), "opti_conp_prop_outputsection.res"
            )
        )
        expect = {
            "a": 4.212000,
            "b": 4.212000,
            "c": 4.212000,
            "alpha": 90.000000,
            "beta": 90.000000,
            "gamma": 90.000000,
            "dE/de1(xx)": 0.001110,
            "dE/de2(yy)": 0.001110,
            "dE/de3(zz)": 0.001110,
            "dE/de4(yz)": 0.000000,
            "dE/de5(xz)": 0.000000,
            "dE/de6(xy)": 0.000000,
            "primitiveCellVolume": 74.724856,
            "densityOfCell": 3.583139,
            "nonPrimitiveCellVolume": 74.724856,
        }
        actual = _gulp_parse.parseFinalCellParametersAndDerivatives(testfile)
        testutil.compareCollection(self, expect, actual)

    def testParseElasticConstantMatrix(self):
        """Test parseElasticConstantMatrix()"""
        testfile = open(
            os.path.join(
                _getResourceDirectory(), "opti_conp_prop_outputsection.res"
            )
        )
        expect = [
            [372.0647, 162.8691, 162.8691, 0.0000, 0.0000, 0.0000],
            [162.8691, 372.0647, 162.8691, 0.0000, 0.0000, 0.0000],
            [162.8691, 162.8691, 372.0647, 0.0000, 0.0000, 0.0000],
            [0.0000, 0.0000, 0.0000, 162.8714, 0.0000, 0.0000],
            [0.0000, 0.0000, 0.0000, 0.0000, 162.8714, 0.0000],
            [0.0000, 0.0000, 0.0000, 0.0000, 0.0000, 162.8714],
        ]
        actual = _gulp_parse.parseElasticConstantMatrix(testfile)
        testutil.compareCollection(self, expect, actual)

    def testParseMechanicalProperties(self):
        """Test parseMechanicalProperties()"""
        testfile = open(
            os.path.join(
                _getResourceDirectory(), "opti_conp_prop_outputsection.res"
            )
        )
        expect = {
            "bulkModulus": {
                "reuss": 232.60093,
                "voigt": 232.60093,
                "hill": 232.60093,
            },
            "shearModulus": {
                "reuss": 133.19021,
                "voigt": 139.56198,
                "hill": 136.37610,
            },
            "velocitySWave": {
                "reuss": 19.27988,
                "voigt": 19.73566,
                "hill": 19.50910,
            },
            "velocityPWave": {
                "reuss": 33.83448,
                "voigt": 34.18307,
                "hill": 34.00923,
            },
            "compressibility": 0.00429921,
            "youngsModuli": [272.88851, 272.88851, 272.88851],
            "poissonsRatio": [
                [None, 0.30447, 0.30447],
                [0.30447, None, 0.30447],
                [0.30447, 0.30447, None],
            ],
        }
        actual = _gulp_parse.parseMechanicalProperties(testfile)
        testutil.compareCollection(self, expect, actual)
