import argparse
import sys
import os

import sqlalchemy as sa
from atsim.pro_fit import db

_VARIABLE_COLUMN_SET = "variables"
_FIT_VARIABLE_COLUMN_SET = "fit-variables"
_EVALUATOR_COLUMN_SET = "evaluators"

EXIT_STATUS_DB_FILE_NOT_FOUND = 2
EXIT_STATUS_DB_CANNOT_OPEN = 3


def parseCommandLine():
    parser = argparse.ArgumentParser(
        prog="ppdump",
        description="Dump potential pro-fit fitting_run.db into a CSV file for post-processing.",
    )

    parser.add_argument(
        "-f",
        "--dbfilename",
        metavar="DB_FILENAME",
        default="fitting_run.db",
        nargs="?",
        help="Specify path of 'fitting_run.db' file.",
    )

    metadataGroup = parser.add_argument_group("Fitting Run Information")

    metadataGroup.add_argument(
        "--list-columns",
        dest="list_columns",
        action="store_const",
        const="all",
        help="list available columns from the database",
    )

    metadataGroup.add_argument(
        "--list-variable-columns",
        dest="list_columns",
        action="store_const",
        const="variable",
        help="list available variable: prefix columns from the database",
    )

    metadataGroup.add_argument(
        "--list-fit-variable-columns",
        dest="list_columns",
        action="store_const",
        const="fit-variable",
        help="list available variable: prefix columns that were free to change during a fitting run",
    )

    metadataGroup.add_argument(
        "--list-evaluator-columns",
        dest="list_columns",
        action="store_const",
        const="evaluator",
        help="list available evaluator: prefix columns from the database",
    )

    metadataGroup.add_argument(
        "--list-it-columns",
        dest="list_columns",
        action="store_const",
        const="it",
        help="list available it: prefix columns from the database",
    )

    metadataGroup.add_argument(
        "--list-stat-columns",
        dest="list_columns",
        action="store_const",
        const="stat",
        help="list available stat: prefix columns from the database",
    )

    metadataGroup.add_argument(
        "-n",
        "--num-iterations",
        action="store_true",
        help="output the number of iterations in the database",
    )

    dumpGroup = parser.add_argument_group(
        "Dump Options", description="Options for controlling the dump."
    )

    dumpGroup.add_argument(
        "-o",
        "--output",
        dest="output_file",
        nargs="?",
        type=argparse.FileType("w"),
        default=sys.stdout,
        metavar="OUTPUT_FILE",
        help="write output into OUTPUT_FILE. If not specified then output is written to STDOUT.",
    )

    dumpGroup.add_argument(
        "-c",
        "--candidate-filter",
        nargs="?",
        choices=["all", "min", "max"],
        default="min",
        help="Selects candidates from each iteration's population. 'all': dumps entire population, 'min': only dumps candidate with minimum primary-value, 'max': dumps candidate with maximum primary-value.",
    )

    columnSelectionGroup = parser.add_argument_group("Column Selection")
    columnSelectionGroup.add_argument(
        "-C",
        "--columns",
        nargs="*",
        metavar="COLUMN_LABEL",
        help="List of column keys to be included the dump. If not specified 'iteration_number', 'candidate_number' and 'merit_value' are used.",
    )

    columnSelectionGroup.add_argument(
        "--variable-columns",
        dest="column_sets",
        action="append_const",
        const=_VARIABLE_COLUMN_SET,
        help="Add the columns listed by --list-variable-columns to the column selection.",
    )

    columnSelectionGroup.add_argument(
        "--fit-variable-columns",
        dest="column_sets",
        action="append_const",
        const=_FIT_VARIABLE_COLUMN_SET,
        help="Add the columns listed by --list-fit-variable-columns to the column selection.",
    )

    columnSelectionGroup.add_argument(
        "--evaluator-columns",
        dest="column_sets",
        action="append_const",
        const=_EVALUATOR_COLUMN_SET,
        help="Add the columns listed by --list-evaluator-columns to the column selection.",
    )

    iterationSelectionGroup = parser.add_argument_group(
        "Iteration selection",
        description="Choose the minimization steps for which data will be dumped",
    )

    iterationSelectionGroup.add_argument(
        "-i",
        "--iteration",
        nargs="?",
        dest="iteration_filter",
        metavar="ITERATION",
        default="best",
        help="Choose minimizer steps to dump. ITERATION can be or 'last', 'best', 'all', 'running_min', 'running_max' or an integer giving step number (indexed from zero).",
    )

    gridGroup = parser.add_argument_group(
        "Grid extraction",
        description="Options that allow grid/matrix data-formats to be output. Typically these options are used with fitting_run.db files obtained from runs that make use of ppgrid and the Spreadsheet minimizer",
    )
    gridGroup.add_argument(
        "--grid",
        nargs="?",
        choices=["R", "GNUPlot"],
        metavar="GRID_FORMAT",
        help="extract data from fitting_run.db in a gridded format. GRID_FORMAT determines how the grid is output. A value of 'R', creates output that can be loaded into the R programming language using the 'dget' function. 'GNUPlot' creates a grid file that can be read using GNUPlot's splot command.",
    )

    gridGroup.add_argument(
        "--gridx",
        nargs="?",
        dest="gridx",
        metavar="COLUMN_LABEL",
        help="Used with --grid option COLUMN_LABEL gives the column the values of which define the x-axis of the gridded data.",
    )

    gridGroup.add_argument(
        "--gridy",
        nargs="?",
        dest="gridy",
        metavar="COLUMN_LABEL",
        help="Used with --grid option COLUMN_LABEL gives the column the values of which define the y-axis of the gridded data.",
    )

    gridGroup.add_argument(
        "--gridz",
        nargs="?",
        dest="gridz",
        metavar="COLUMN_LABEL",
        help="Used with --grid option COLUMN_LABEL gives the column from which the grid data values are taken.",
    )

    gridGroup.add_argument(
        "--grid_missing",
        nargs="?",
        dest="gridmissing",
        metavar="VALUE",
        type=float,
        help="Used with --grid option, if specified, NULL values in the fitting database are written as VALUE in the output grid file rather than None.",
    )

    options = parser.parse_args()

    if options.grid and not (
        options.gridx and options.gridy and options.gridz
    ):
        parser.error(
            "--grid cannot be specified without also providing --gridx, --gridy and --gridz options."
        )

    if not options.grid and (options.gridx or options.gridy or options.gridz):
        parser.error(
            "--gridx, --gridy and --gridz options cannot be used without also specifying --grid"
        )

    return parser, options


def _getColumnList(engine, columns, column_sets):
    outcols = []
    workingcols = []
    if columns != None:
        workingcols.extend(columns)

    if column_sets != None:
        for cset in column_sets:
            if cset == _VARIABLE_COLUMN_SET:
                workingcols.extend(
                    db.IterationSeriesTable.validVariableKeys(engine)
                )
            elif cset == _FIT_VARIABLE_COLUMN_SET:
                workingcols.extend(
                    db.IterationSeriesTable.validFittingVariableKeys(engine)
                )
            elif cset == _EVALUATOR_COLUMN_SET:
                workingcols.extend(
                    db.IterationSeriesTable.validEvaluatorKeys(engine)
                )
            else:
                # Should never get here
                raise Exception("Unknown column set")

    # Remove any duplicates having them appear in order of first appearance
    for col in workingcols:
        if not col in outcols:
            outcols.append(col)
    return outcols


def listColumns(engine, whichSet="all"):
    """List column keys to stdout

  :param engine: SQL Alchemy database engine"""
    ItT = db.IterationSeriesTable
    colsets = {
        "all": ItT.validKeys,
        "variable": ItT.validVariableKeys,
        "fit-variable": ItT.validFittingVariableKeys,
        "evaluator": ItT.validEvaluatorKeys,
        "it": ItT.validIterationKeys,
        "stat": ItT.validStatisticsKeys,
    }

    keys = colsets[whichSet](engine)
    for key in keys:
        print(key)


def outputNumIterations(engine):
    """Display number of iterations in the file.

  :param engine: SQL Alchemy database engine"""
    f = db.Fitting(engine)
    print(f.current_iteration())


def outputTable(engine, columns, iteration_filter, candidate_filter, outfile):

    iterationSeriesTable = db.IterationSeriesTable(
        engine,
        iterationFilter=iteration_filter,
        candidateFilter=candidate_filter,
        columns=columns,
    )

    for row in iterationSeriesTable:
        print(",".join([str(v) for v in row]), file=outfile)


def outputGrid(engine, gridtype, gridx, gridy, gridz, outfile, gridmissing):
    """Outputs data in grid formats.

  :param engine: SQLite Engine.
  :param gridtype: One of the supported grid types (currently 'R')
  :param gridx: Grid x-axis column label
  :param gridy: Grid y-axis column label
  :param gridz: Grid z-axis column label
  :param outfile: File to which output should be written
  :param gridmissing: Value to be used when a None value is encountered in z-data"""

    iterationSeriesTable = db.IterationSeriesTable(
        engine,
        candidateFilter="all",
        iterationFilter="all",
        columns=[gridx, gridy, gridz],
    )

    serializer = {
        "R": db.serializeTableForR,
        "GNUPlot": db.serializeTableForGNUPlot,
    }[gridtype]

    serializer(iterationSeriesTable, outfile, gridx, gridy, gridz, gridmissing)


def main():
    parser, options = parseCommandLine()

    # Check if dbfile is present.
    if not os.path.isfile(options.dbfilename):
        parser.exit(
            EXIT_STATUS_DB_FILE_NOT_FOUND,
            "Database could not be found: '%s'" % options.dbfilename,
        )

    engine = sa.create_engine("sqlite:///" + options.dbfilename)

    # Check db format
    if not db.validate(engine):
        parser.exit(
            EXIT_STATUS_DB_CANNOT_OPEN,
            "Database does not have valid structure: '%s'"
            % options.dbfilename,
        )

    if options.list_columns:
        listColumns(engine, options.list_columns)
    elif options.num_iterations:
        outputNumIterations(engine)
    elif options.grid:
        outputGrid(
            engine,
            options.grid,
            options.gridx,
            options.gridy,
            options.gridz,
            options.output_file,
            options.gridmissing,
        )
    else:
        columns = _getColumnList(engine, options.columns, options.column_sets)

        iteration_filter = options.iteration_filter

        if iteration_filter == "best":
            iteration_filter = "global_min"

        if not iteration_filter in [
            "global_min",
            "last",
            "running_min",
            "running_max",
            "all",
        ]:
            try:
                v = int(iteration_filter)
                iteration_filter = "n({})".format(v)
            except ValueError:
                parser.error(
                    " --iteration argument '{}' must be an integer representing a step number or 'best', 'last', 'running_min', 'running_max' or 'all'.".format(
                        iteration_filter
                    )
                )
        outputTable(
            engine,
            columns,
            iteration_filter,
            options.candidate_filter,
            options.output_file,
        )


if __name__ == "__main__":
    main()
