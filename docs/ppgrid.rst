.. _ppgrid:


******
ppgrid
******

Tool for creating n-dimensional grids as CSV files suitable for use with the :ref:`Spreadsheet minimiser <pprofit-minimizers-spreadsheet>`.

Usage
=====

::

	ppgrid [OPTIONS] ARGS

Create a CSV file containing an n-dimensional grid suitable for use with the pprofit Spreadsheet minimiser.

The arguments to ``ppgrid`` specify each grid axis. Grid dimensions and resolution can be specified in one of two ways:

  1. By default each grid axis is specified as::

		NAME:LOW_VALUE,STEP_SIZE,NUM_STEPS

    Where:
      * ``NAME``      - Is variable name.
      * ``LOW_VALUE`` - Axis start value.
      * ``STEP_SIZE`` - Increment between consecutive values along grid axis.
      * ``NUM_STEPS`` - Number of axis grid values.


  2. If the ``--range`` option is used then the following format is used:

      NAME:LOW_VALUE,HIGH_VALUE,NUM_STEPS

    Where:
      * ``NAME``       - Variable name
      * ``LOW_VALUE``  - Axis start value.
      * ``HIGH_VALUE`` - Axis end value.
      * ``NUM_STEPS``  - Number of axis grid values.


Example 1:
----------
To create two dimensional 10⨉10 grid for variables A and B with ranges::

  10 <= A <= 20
  0  <= B <= 5

the following command line could be used to write grid into a file named grid.csv:

  ``ppgrid --range A:10,20,10 B:0,5,10 -o grid.csv``


Example 2:
----------
To create a 5⨉10⨉5 grid with running from 0 to 5 on axes A,B and C with specific grid increments, writing to STDOUT
this could be used::

  ppgrid A:0,1,5 B:0,0.5,10 C:0,1,5



Options::

  -h, --help            show this help message and exit
  -r, --range           Arguments are specified as NAME:LOW,HIGH,NUM_STEPS
                        combinations.
  -o FILENAME, --output=FILENAME
                        Write output data in CSV format to FILENAME. If not
                        specified, output is written to STDOUT.

