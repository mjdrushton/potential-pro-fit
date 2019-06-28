import os
import re

def version_path():
    basedir = os.path.dirname(__file__)
    return os.path.join(basedir, "lib", "atsim", "pro_fit", "_version.py")

def readversion():
    versionPath = version_path()

    with open(versionPath) as vfile:
        for line in vfile:
            if line.startswith("__version__"):
                tokens = line.split("=")
                version = tokens[1].strip()
                # Remove quotes
                version = version[1:-1]
                return version
    raise Exception("Version number not found")


def setversion(version):
    version_re = re.compile(r'[0-9]+\.[0-9]+\.[0-9](dev)?')
    assert version_re.match(version)

    # Files that need touching:
    #    * README
    #    * _version

    # Set version in readme.
    import io

    sio = io.StringIO()
    readme_path = os.path.join(os.path.dirname(__file__), "README.md")

    with open(readme_path) as infile:
        line = next(infile)
        

