
from assertpy import assert_that

from atsim.pro_fit.runners._remoterunner import pbsIdentify

def testPBSIdentify():
  """Given a string from qstat --version identify PBS system as Torque or PBSPro"""
  # Test output from TORQUE
  versionString = "version: 2.4.16"
  actual = pbsIdentify(versionString)
  assert_that(actual.arrayFlag).is_equal_to("-t")
  assert_that(actual.arrayIDVariable).is_equal_to("PBS_ARRAYID")

  versionString = "pbs_version = PBSPro_11.1.0.111761"
  actual = pbsIdentify(versionString)
  assert_that(actual.arrayFlag).is_equal_to("-J")
  assert_that(actual.arrayIDVariable).is_equal_to("PBS_ARRAY_INDEX")

def testTerminate():
  """Test runner's .terminate() method."""
  assert(False)

def testClose():
  """Test runner's .close() method."""
  assert(False)
