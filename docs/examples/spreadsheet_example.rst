
Example Using the Spreadsheet Minimiser
=======================================

The following example shows how to use the `Spreadsheet minimiser <pprofit-minimizers-spreadsheet>`_ to systematically explore parameter space when potential space. The example aims to reproduce the method used by Read and Jackson to derive their 2010 potential set for UO\ :sub:`2` `doi:10.1016/j.jnucmat.2010.08.044 <http://dx.doi.org/10.1016/j.jnucmat.2010.08.044>`_ [Read2010]_.

Description of Problem
----------------------

Read and Jackson wanted to fit a U-O potential suitable for use with a previously derived O-O interaction employing the four-range Buckingham potential form (Buck-4). Due to the absence of a stationary point the Buck-4 description, the Buckingham potential had previously been employed to describe the U-O cation anion interaction:

  .. math::
  
    V_{\text{U-O}}(r_{ij}) = A \exp\left( - \frac{\rho}{r_{ij}} \right) - \frac{C}{r_{ij}^6}

Where :math:`V_{\text{U-O}}(r_{ij})` is the energy of two ions :math:`i` and :math:`j` separated by :math:`r_{ij}` and :math:`A`, :math:`\rho` and :math:`C` are parameters specific to the U-O interaction. 

A limitation of the Buckingham form is that, at small separations, it is prone to the so-called Buckingham catastrophe, whereby the attractive :math:`C/r_{ij}^6` term can overwhelm the repulsive exponential term. This can lead to a highly unphysical attraction which will ultimately cause atoms to collapse on to each other and subsequent failure of the simulation.

In order to overcome this limitation Jackson and Read deleted the :math:`C/r_{ij}^6` term. They then performed a systematic search of :math:`A` and :math:`\rho` values that were still able to provide a reasonable description of the UO\ :sub:`2` structure without using a :math:`C` term. The example will now show how ``pprofit`` can be used with the Spreadsheet minimiser (see :ref:`Spreadsheet minimizer <pprofit-minimizers-spreadsheet>`\ ) to perform the same search.


Worked Example
--------------

The example will now demonstrate the following:

1. Initialise fitting run.
2. Setting up ``GULP`` energy minimisation:

  * Create job directory.
  * Create input files to perform energy minimisation using fitting variables.
  * Edit ``job.cfg`` to use the :ref:`Gulp Evaluator <pprofit-evaluators-gulp>` to obtain quality of lattice constant predicted using fitting parameters against experimental value.

3. Create ``fit.cfg`` to perform systematic :math:`A` and :math:`\rho` parameter search:

  * Define ``[Variables]`` section.
  * Set-up the ``[Minimizer]`` section to use ``Spreadsheet`` minimizer.  


Initialise Fitting Run
^^^^^^^^^^^^^^^^^^^^^^
Create the skeleton of a ``pprofit`` run by typing the following in a terminal::

  pprofit --init spreadsheet_example

* This will create a directory named ``spreadsheet_example``.
* Go into the ``spreadsheet_example`` directory:

  ::

    cd spreadsheet_example

* Within the directory you will find a basic ``fit.cfg`` file and an empty ``fit_files/`` directory. 

Create ``GULP`` Job
^^^^^^^^^^^^^^^^^^^

* We will set-up a simple perfect lattice energy minimization that makes use of the ``GULP`` code. The aim of this will be to compare the lattice constant predicted for a UO\ :sub:`2` cell  for different U-O :math:`A` and :math:`\rho` values against an experimentally determined value. In so doing we will be possible to see which values give the best fit to the experimental structure.
* Initialize a new job named ``Gulp_UO2`` by typing the following command within the ``spreadsheet_example`` folder:
  
  ::

    pprofit --init-job Gulp_UO2
  
* This will create a directory named ``Gulp_UO2`` within ``fit_files``, this should contain a skeleton ``job.cfg`` and non-functional ``runjob`` file.
* At this point the ``job.cfg`` file is configured to use the ``Template`` job factory (see :ref:`Template Job Factory <pprofit-jobfactories-template>`) and the ``Local`` runner defined, by default, in the ``fit.cfg`` file.

* **Create input.gin.in:**

  * Create a file named ``input.gin.in`` within the ``fit_files/Gulp_UO2`` directory. 
  * It is important that the file has the ``.in`` suffix, as this indicates to the ``Template`` job-factory that it contains place-holders which should be replaced by fitting variable values.
  * Edit the file to have the following content (this defines a simple ``GULP`` lattice minimisation job for the UO\ :sub:`2` structure):
  
    .. literalinclude:: spreadsheet_example/fit_files/Gulp_UO2/input.gin.in


  * Note that the parameters for the U-O ``buck`` potential definition at the end of the file have been replaced by ``@A@`` and ``@rho@`` place-holders, indicating where the values of fitting variables should be inserted. In a moment, we will define these variables in the ``[Variables]`` section of the ``fit.cfg`` file.
  
* **Edit runjob:**

  * In order to tell ``pprofit`` how to run our ``GULP`` file we need to edit the ``runjob`` file.
  * Edit ``runjob`` to have the following contents:
  
    .. literalinclude:: spreadsheet_example/fit_files/Gulp_UO2/runjob
    
  
  * This states that ``gulp`` should take a file named ``input.gin`` (**note:** the ``.in`` suffix is dropped after template processing) and create an output file named ``output.gout``.
  * If your ``gulp`` binary is not on your path, or had a non standard name, this can also be specified within ``runjob``.

* **Create an Evaluator in job.cfg to extract relaxed lattice constant from output.gout**:

  * We will now define a ``GULP`` evaluator with the ``job.cfg`` file (see :ref:`Gulp evaluator <pprofit-evaluators-gulp>`).
  * This is used to extract the lattice constant from the ``output.gout`` file generated by ``GULP``.
  * This is compared against an experimentally determined value of 5.468Å.
  * The squared difference between the calculated and experimental values defines the merit-value for the fitting run. 
  * Edit ``job.cfg`` to contain:
  
    .. literalinclude:: spreadsheet_example/fit_files/Gulp_UO2/job.cfg
 

Set-up ``fit.cfg``
^^^^^^^^^^^^^^^^^^

Before being able to run ``pprofit`` it is necessary to make some changes to the ``fit.cfg`` file located in the root, ``spreadsheet_example`` folder.

* **Define [Variables]:** The first job is to define variables for the ``@A@`` and ``@rho@`` placeholders used within our ``input.gin.in`` file.
  
  * Edit the ``fit.cfg`` file and scroll to the ``[Variables]`` section and then edit it so that it reads::
  
      [Variables]
      A   : 1.0 *
      rho : 1.0 *
      
      
  * Although variable values of 1.0 are specified, these are not used during the ``pprofit`` run, as variable values will be read from the spreadsheet being used to drive the run.
  * Note that the ``*`` at the end of the variable definitions indicate that both are optimisation variables. 

* **Set-Up Spreadsheet Minimizer:** ``pprofit`` needs to be told to read variables from a spreadsheet.

  * The original Jackson and Read paper surveyed the following ranges: :math:`750 \leq A \leq 2200`\ eV and :math:`0.2 \leq \rho \leq 0.5`\ Å [Read2010]_\ . These ranges have also been adopted for the present example. A CSV file is provided covering these ranges using a 20\ :math:`\times`\ 20 grid.
  * Download the :download:`spreadsheet_example/spreadsheet.csv` file and place it in the same directory as ``fit.cfg``. 
  * Find the ``[Minimizer]`` section of ``fit.cfg`` and edit it such that it reads::

      [Minimizer]
      type : Spreadsheet
      filename : spreadsheet.csv
      

Run ``pprofit``
^^^^^^^^^^^^^^^

* You should now have all the files you need for a fitting run. 
* From the directory containing ``fit.cfg`` type the following::

    pprofit
    
* In order to monitor the progress of your run, open a separate terminal and run the monitor::

    pprofitmon
    
* Having invoked ``pprofitmon`` go to http://localhost:8080 in your web browser.


Analysing the results
---------------------

      

.. [Read2010] S.D. Read and R.A. Jackson, "Derivation of enhanced potentials for uranium dioxide and the calculation of lattice and intrinsic defect properties" *Journal of Nuclear Materials* **406** (2010) 293. 