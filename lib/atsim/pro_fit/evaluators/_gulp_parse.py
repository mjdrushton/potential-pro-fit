import io

"""Slices for use with columnSplitGenerator() that can split up matrix lines"""
matrixSlices = [
    (float, slice(8, 20)),
    (float, slice(20, 30)),
    (float, slice(30, 40)),
    (float, slice(40, 50)),
    (float, slice(50, 60)),
    (float, slice(60, 70)),
]


def badFloatAsNone(value):
    """Converts a string to a float or None if the string is empty or cannot be converted to a float"""
    try:
        return floatOrNone(value)
    except ValueError:
        return None


def floatOrNone(value):
    """Converts a string to a float or None if the string is empty"""
    value = value.strip()
    if not value:
        return None
    return float(value)


class ParseException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return "ParseException:" + str(self.value)


def chunkIterator(glpFile):
    """Wraps a file object to produce and iterator that returns
  (chunk_name, chunk_file) tuples.

  The gulp file is basically split into different sections delimited by lines
  of the form:

  ******************...*
  *  Chunk_name     ...*
  ******************...*

  the iterator splits the file to the sections between these delimiters.
  Each chunk (chunk_file) fulfils the python file protocol."""

    # Skip the header  by getting to the fourth delimLine
    inHeader = False
    for line in glpFile:
        if not inHeader and line[0] == "*":
            inHeader = True
        elif inHeader and line[0] != "*":
            # We were in the header, now we're not
            break

    delimLine = 80 * "*"

    def makeChunk():
        # Read to the
        nextLine = None
        chunkLines = []
        for line in glpFile:
            if line[:-1] == delimLine:
                nextLine = next(glpFile)
                if nextLine.startswith("*") and nextLine[:-1] != delimLine:
                    break
                else:
                    chunkLines.append(line)
                    chunkLines.append(nextLine)
                    nextLine = None
            else:
                chunkLines.append(line)
        return nextLine, io.StringIO("".join(chunkLines))

    # Yield the first untitled chunk
    headerLine, chunk = makeChunk()
    yield ("", chunk)

    def readChunkHeader(line):
        if not line:
            return None

        if line[0] != "*":
            raise ParseException(
                "Was expecting chunk header line instead got '%s'" % line[:-1]
            )

        chunkName = line[3:-2]
        line = next(glpFile)[:-1]
        assert line == delimLine
        return chunkName

    while True:
        chunkName = readChunkHeader(headerLine)
        if not chunkName:
            break

        headerLine, chunk = makeChunk()
        yield (chunkName, chunk)


def _getFirstOutputConfigurationChunk(glpFile):
    chunkIt = chunkIterator(glpFile)
    for chunkName, chunk in chunkIt:
        if not chunkName.startswith("Output for configuration"):
            continue
        break
    return chunk


def skip(fileObj, n):
    for _i in range(n):
        next(fileObj)


def columnSplitGenerator(columnSlices):
    """Generates a function that given a set of slice objects splits a string up.
  columnSlices can contain the following:

  1) Tuples of the form (key, converter_function, slice_object)
  or
  2) Mixed list containing either slice objects or (converter_function, slice_object)

  converter_functions are used to convert string to desired type.

  If form 1 is used then a dictionary is returned by the generated function.
  If form 2 is used then a list is returned"""

    def func(line):
        l = []
        retdict = False
        for s in columnSlices:
            if len(s) == 3:
                retdict = True
                _, callable, sliceObj = s
            elif len(s) == 2:
                callable, sliceObj = s
            else:
                callable = str
                sliceObj = s
            l.append(callable(line[sliceObj]))

        # Convert to dictionary if neccessary
        if retdict:
            keys = [key for key, callable, sliceobj in columnSlices]
            return dict(list(zip(keys, l)))

        # Otherwise return list
        return l

    return func


def createMatrixParse(coldefs, rows, numSkip=4):
    splitLine = columnSplitGenerator(coldefs)

    def func(outputChunk):
        mat = []
        skip(outputChunk, numSkip)
        for _ in range(rows):
            mat.append(splitLine(next(outputChunk)))
        return mat

    return func


def makeFindStart(startsWith):
    startsWith = startsWith.strip()

    def func(fileObj):
        for line in fileObj:
            line = line.strip()
            if line.startswith(startsWith):
                return True
        return False

    return func


def parseElasticConstantMatrix(glpFile):
    """Parse 'Elastic Constant Matrix' from gulp file and return 6x6 matrix containing values.

  @param glpFile Python file object
  @return 6x6 lists containing elastic constants"""

    findStart = makeFindStart("  Elastic Constant Matrix:")
    readData = createMatrixParse(matrixSlices[:6], 6)
    if findStart(glpFile):
        return readData(glpFile)
    raise ParseException("Could not find 'Elastic Constant Matrix'")


def parseMechanicalProperties(glpFile):
    """Extracts 'Mechanical properties' section from gulp output configuration

  Returns dictionary of the form:

   { 'bulkModulus'    : {'reuss' : value, 'voigt' : value, 'hill' : value},
     'shearModulus'   : {'reuss' : value, 'voigt' : value, 'hill' : value},
     'velocitySWave'  : {'reuss' : value, 'voigt' : value, 'hill' : value},
     'velocityPWave'  : {'reuss' : value, 'voigt' : value, 'hill' : value},
     'compressibility'  : value,
     'youngsModuli'  : [x,y,z],
     'poissonsRatio'  : [ [x,y,z],
                          [x,y,z],
                          [x,y,z]] }

  @param outputChunk Python file object
  @return Dictionary of the form described above"""
    chunk = _getFirstOutputConfigurationChunk(glpFile)
    findStart = makeFindStart(" Mechanical properties :")

    basecoldefs = [
        (float, slice(27, 41)),
        (float, slice(41, 55)),
        (float, slice(55, 69)),
    ]
    listsplit = columnSplitGenerator(basecoldefs)
    rvhcolsplit = columnSplitGenerator(
        [
            (name, c, sl)
            for (name, (c, sl)) in zip(["reuss", "voigt", "hill"], basecoldefs)
        ]
    )
    poissonsplit = createMatrixParse(
        [(floatOrNone, sl) for (junk, sl) in basecoldefs], 3, 0
    )

    def readData():
        datadict = {}
        skip(chunk, 4)
        datadict["bulkModulus"] = rvhcolsplit(next(chunk))
        datadict["shearModulus"] = rvhcolsplit(next(chunk))
        skip(chunk, 1)
        datadict["velocitySWave"] = rvhcolsplit(next(chunk))
        datadict["velocityPWave"] = rvhcolsplit(next(chunk))
        skip(chunk, 1)
        datadict["compressibility"] = float(next(chunk)[basecoldefs[0][1]])
        skip(chunk, 3)
        datadict["youngsModuli"] = listsplit(next(chunk))
        skip(chunk, 1)
        datadict["poissonsRatio"] = poissonsplit(chunk)
        return datadict

    if findStart(chunk):
        return readData()
    return {}


def parseFinalCellParametersAndDerivatives(glpFile):
    """Parse 'Final cell parameters and derivatives' section from gulp output configuration.

  Returns dictionary of the form:
  { 'a' : float,
    'b' : float,
    'c' : float,
    'alpha' : float,
    'beta' : float,
    'gamma' : float,
    'dE/de1(xx)' : float,
    'dE/de2(yy)' : float,
    'dE/de3(zz)' : float,
    'dE/de4(yz)' : float,
    'dE/de5(xz)' : float,
    'dE/de6(xy)' : float,
    'primitiveCellVolume' : float,
    'densityOfCell' : float,
    'nonPrimitiveCellVolume' : float}

    @param glpFile Python file object containing gulp output
    @return Dictionary of form described above"""
    outputChunk = _getFirstOutputConfigurationChunk(glpFile)

    findStart = makeFindStart("  Final cell parameters and derivatives :")

    def extractFields(kA, kB, line):
        keyA = line[7:17]
        if not keyA.strip() == kA:
            raise ParseException(
                "Parsing 'Final cell parameters and derivatives' did not find key '%s' in line '%s'"
                % (kA, line)
            )
        valueA = float(line[18:28].strip())
        keyB = line[42:52]
        if not keyB.strip() == kB:
            raise ParseException(
                "Parsing 'Final cell parameters and derivatives' did not find key '%s' in line '%s'"
                % (kB, line)
            )
        valueB = float(line[55:65].strip())
        return valueA, valueB

    def readData():
        d = {}
        next(outputChunk)
        next(outputChunk)
        d["a"], d["dE/de1(xx)"] = extractFields(
            "a", "dE/de1(xx)", next(outputChunk)
        )
        d["b"], d["dE/de2(yy)"] = extractFields(
            "b", "dE/de2(yy)", next(outputChunk)
        )
        d["c"], d["dE/de3(zz)"] = extractFields(
            "c", "dE/de3(zz)", next(outputChunk)
        )
        d["alpha"], d["dE/de4(yz)"] = extractFields(
            "alpha", "dE/de4(yz)", next(outputChunk)
        )
        d["beta"], d["dE/de5(xz)"] = extractFields(
            "beta", "dE/de5(xz)", next(outputChunk)
        )
        d["gamma"], d["dE/de6(xy)"] = extractFields(
            "gamma", "dE/de6(xy)", next(outputChunk)
        )
        next(outputChunk)
        next(outputChunk)
        line = next(outputChunk)
        if not line.startswith("  Primitive cell volume"):
            raise ParseException(
                "Parsing 'Final cell parameters and derivatives' expected 'Primitive cell volume' in line '%s'"
                % line
            )
        d["primitiveCellVolume"] = float(line[25:46].strip())
        next(outputChunk)
        line = next(outputChunk)
        if not line.startswith("  Density of cell"):
            raise ParseException(
                "Parsing 'Final cell parameters and derivatives' expected '  Density of cell' in line '%s'"
                % line
            )
        d["densityOfCell"] = float(line[19:33].strip())
        next(outputChunk)
        line = next(outputChunk)
        if not line.startswith("  Non-primitive cell volume"):
            raise ParseException(
                "Parsing 'Final cell parameters and derivatives' expected 'Non-primitive cell volume' in line '%s'"
                % line
            )
        d["nonPrimitiveCellVolume"] = float(line[29:50].strip())
        return d

    if findStart(outputChunk):
        return readData()
    return {}


def parseComponentsOfEnergy(glpFile):
    """Extracts 'components of energy' records from an Output configuration
    chunk of a gulp output file.

    Returns dictionary of the form:

      { 'componentsOfEnergyAtStart' : energydict,
        'componentsOfEnergyAtEnd'   : energydict}

    Where:
      componentsOfEnergyAtStart - is energy before minimisation
      componentsOfEnergyAtEnd - is energy after minimisation (may not occur in output)

      energydict has the following keys values in [] brackets are optional:
        'interAtomicPotentials'
        ['manyBodyPotentials'],
        ['threeBodyPotentials'],
        'monopoleMonopoleReal'
        'monopoleMonopoleRecip'
        'monopoleMonopoleTotal'
        'totalLatticeEnergy'
        'totalLatticeEnergykJPerMoleUnitCells'

  @param glpFile Python file object
  @return Dictionary of form described above"""
    outputChunk = _getFirstOutputConfigurationChunk(glpFile)
    outputDict = {}

    findStart = makeFindStart("  Components of energy :")

    def extractField(line):
        return badFloatAsNone(line[30:51].strip())

    def startswith(search):
        def f(line):
            return line.startswith(search)

        return f

    processors = [
        (startswith("  Interatomic potentials"), "interatomicPotentials"),
        (startswith("  Three-body potentials"), "threeBodyPotentials"),
        (startswith("  Many-body potentials"), "manyBodyPotentials"),
        (startswith("  Monopole - monopole (real)"), "monopoleMonopoleReal"),
        (startswith("  Monopole - monopole (recip)"), "monopoleMonopoleRecip"),
        (startswith("  Monopole - monopole (total)"), "monopoleMonopoleTotal"),
    ]

    def readEnergy():
        d = {}
        next(outputChunk)
        next(outputChunk)
        for line in outputChunk:
            if line.startswith("-"):
                break

            for match, outkey in processors:
                if match(line):
                    d[outkey] = extractField(line)
                    break
        line = next(outputChunk)
        if not line.startswith("  Total lattice energy"):
            raise ParseException(
                'Error reading "Components of energy", expecting "Total lattice energy" got "%s"'
                % line[:-1]
            )
        d["totalLatticeEnergy"] = extractField(line)
        next(outputChunk)
        line = next(outputChunk)
        if not line.startswith("  Total lattice energy"):
            raise ParseException(
                'Error reading "Components of energy", expecting "Total lattice energy" got "%s"'
                % line[:-1]
            )
        d["totalLatticeEnergykJPerMoleUnitCells"] = extractField(line)
        next(outputChunk)
        return d

    if findStart(outputChunk):
        outputDict["componentsOfEnergyAtStart"] = readEnergy()
        if findStart(outputChunk):
            outputDict["componentsOfEnergyAtEnd"] = readEnergy()
    return outputDict
