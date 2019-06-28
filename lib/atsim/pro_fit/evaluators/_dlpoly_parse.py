import re


def parseCONFIG(infile):
    """Reads the dl_poly CONFIG file contained in 'configfilename' """

    title = infile.readline()[:-1]

    line = infile.readline()
    try:
        levcfg, imcon = re.match("(.{10})(.{10})", line).groups()
    except:
        levcfg = re.match("(.{10})", line).groups()
        imcon = 0

    levcfg = int(levcfg)
    imcon = int(imcon)

    # Read the toxyz matrix
    re_split = re.compile("(.{20})(.{20})(.{20})")
    dlvec = []
    for i in range(3):
        line = infile.readline()
        c1, c2, c3 = re_split.match(line).groups()
        dlvec.append([float(c1), float(c2), float(c3)])

    toxyz_matrix = dlvec
    atoms = []
    re_split = re.compile("(.{20})(.{20})(.{20})")

    def readcoord():
        line = infile.readline()
        if not line:
            return None
        atmnam = line[:8]
        index = line[8:18]
        if index:
            index = int(index)
        else:
            index = None

        atmnum = line[18:28].strip()
        if atmnum:
            atmnum = int(atmnum)
        else:
            atmnum = None

        line = infile.readline()
        x, y, z = re_split.match(line).groups()
        pos = (float(x), float(y), float(z))
        atom = {
            "atmnam": atmnam,
            "coord": pos,
            "atmnum": atmnum,
            "index": index,
        }
        return atom

    def readvelocity():
        atom = readcoord()
        if not atom:
            return None
        line = infile.readline()
        x, y, z = re_split.match(line).groups()
        vel = (float(x), float(y), float(z))
        atom["vel"] = vel
        return atom

    def readforce():
        atom = readvelocity()
        if not atom:
            return None
        line = infile.readline()
        x, y, z = re_split.match(line).groups()
        force = (float(x), float(y), float(z))
        atom["force"] = force
        return atom

    readatom = {0: readcoord, 1: readvelocity, 2: readforce}[levcfg]

    atom = readatom()
    while atom:
        atoms.append(atom)
        atom = readatom()

    return {
        "title": title,
        "toxyz_matrix": toxyz_matrix,
        "cell": toxyz_matrix,
        "levcfg": levcfg,
        "imcon": imcon,
        "atoms": atoms,
    }


class ParseSTATISBadOptionsException(Exception):
    pass


def parseSTATIS(infile, config=None, npt=False):
    """Parse a STATIS file into a series of dictionaries.
  Dictionaries contain the keys listed in dlpoly.parse.statisColumnKeys.

  Additional columns are listed under the 'extra_items' key. With additional
  input (config and npt options), the extra_items can be associated with their
  own keys.

  If config is specified (as returned by dlpoly.parse.parseCONFIG) then
  msd_SPECIES keys are made available containing average mean squared displacement
  for each species in the system. In addition the stressxx, stressxy, stressxz,
  stressyx, stressyy, stressyz, stresszx, stresszy and stresszz keys are made
  available if config is specified.

  If config specified and npt = True additionally provide following keys:
    cella_x, cella_y, cella_z,
    cellb_x, cellb_y, cellb_z,
    cellc_x, cellc_y, cellc_z

  These values will only be valid if file was generated from NPT dl_poly run.

  @param infile Python file in which STATIS file is contained.
  @param config Parsed CONFIG file as returned by dlpoly.parse.parseCONFIG() makes
                additional column keys available (see main description).
  @param npt If True provde additional cell column keys (see main description).
  @return Iterator that returns dictionaries with keys described in main description."""

    if npt == True and config == None:
        raise ParseSTATISBadOptionsException("npt=True but config=None")

    if config == None:
        return _parseSTATISInternal(infile)

    species = _extractSpeciesNames(config)
    return _extraFieldDictIterator(_parseSTATISInternal(infile), species, npt)


statisColumnKeys = [
    "engcns",
    "temp",
    "engcfg",
    "engsrp",
    "engcpe",
    "engbnd",
    "engang",
    "engdih",
    "engtet",
    "enthal",
    "tmprot",
    "vir",
    "virsrp",
    "vircpe",
    "virbnd",
    "virang",
    "vircon",
    "virtet",
    "volume",
    "tmpshl",
    "engshl",
    "virshl",
    "alpha",
    "beta",
    "gamma",
    "virpmf",
    "press",
]


def _parseSTATISInternal(infile):
    # Skip Title line
    next(infile)
    # Skip Energy units
    next(infile)

    keys = statisColumnKeys

    for line in infile:
        # line = next(infile)
        line = line[:-1].strip()
        block = []
        try:
            timestep, time, nument = line.split()
        except ValueError:
            if not line == None:
                try:
                    # Skip Title line
                    next(infile)
                    # Skip Energy units
                    line = next(infile)
                    timestep, time, nument = line.split()
                except:
                    return
        nument = int(nument)
        # Read block
        while len(block) < nument:
            line = next(infile)[:-1]
            lineitems = [float(item) for item in line.split()]
            block.extend(lineitems)

        # Convert block to a dictionary
        blockdict = dict(list(zip(keys, block[: len(keys)])))
        blockdict["timestep"] = int(timestep)
        blockdict["time"] = float(time)

        # Stick any extra items in here
        blockdict["extra_items"] = block[len(keys) :]
        yield blockdict
    infile.close()


def _extractSpeciesNames(config):
    species = []
    for a in config["atoms"]:
        lab = a["atmnam"]
        lab = lab.strip()
        if not lab in species:
            species.append(lab)
    return species


def _extraFieldDictIterator(statisIterator, speciesNames, nptFlag):
    extrafieldkeys = []
    if speciesNames != None:
        extrafieldkeys = ["msd_%s" % (label.strip(),) for label in speciesNames]

    extrafieldkeys.extend(
        [
            "stressxx",
            "stressxy",
            "stressxz",
            "stressyx",
            "stressyy",
            "stressyz",
            "stresszx",
            "stresszy",
            "stresszz",
        ]
    )

    if nptFlag:
        extrafieldkeys.extend(
            [
                "cella_x",
                "cella_y",
                "cella_z",
                "cellb_x",
                "cellb_y",
                "cellb_z",
                "cellc_x",
                "cellc_y",
                "cellc_z",
            ]
        )

    for b in statisIterator:
        extra_items = b["extra_items"][: len(extrafieldkeys)]

        newb = dict(b)
        del newb["extra_items"][: len(extrafieldkeys)]
        newb.update(dict(list(zip(extrafieldkeys, extra_items))))
        yield newb
