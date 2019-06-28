import os
import re
import sys

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
        line = next(infile)[:-1]
        tokens = line.split()
        assert version_re.match(tokens[-1][1:])
        tokens[-1] = "v{}".format(version)
        line = " ".join(tokens)
        sio.write(line+"\n")
        
        for line in infile:
            sio.write(line)

    sio.seek(0)
    with open(readme_path, "w") as outfile:
        outfile.write(sio.read())

    # Set version in the version file
    vp = version_path()
    sio = io.StringIO()

    with open(vp) as infile:
        for line in infile:
            if line.startswith("__version__"):
                line = "__version__='{}'\n".format(version)
            sio.write(line)
    sio.seek(0)
    with open(vp, 'w') as outfile:
        outfile.write(sio.read())


def main():
    args = sys.argv[1:]

    if not args:
        print(readversion())
    else:
        version = args[0]
        setversion(version)

if __name__ == "__main__":
    main()