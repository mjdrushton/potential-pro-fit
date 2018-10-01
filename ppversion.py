import os


def readversion():
  basedir = os.path.dirname(__file__)
  versionPath = os.path.join(basedir, "lib", "atsim", "pro_fit", "__init__.py")

  with open(versionPath) as vfile:
    for line in vfile:
      if line.startswith("__version__"):
        tokens = line.split("=")
        version = tokens[1].strip()
        # Remove quotes
        version = version[1:-1]
        return version
  raise Exception("Version number not found")