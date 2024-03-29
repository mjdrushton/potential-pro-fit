********************************************************************************
*                       GENERAL UTILITY LATTICE PROGRAM                        *
*                                 Julian Gale                                  *
*                      Nanochemistry Research Institute                        *
*             Curtin University of Technology, Western Australia               *
********************************************************************************
* Version = 3.4.7 * Last modified =  15th April 2009                           *
********************************************************************************
*  optimise     - perform optimisation run                                     *
*  conp         - constant pressure calculation                                *
*  property     - calculate properties for final geometry                      *
*  phonon       - calculate phonons for final geometry                         *
********************************************************************************

  Job Started  at 21:19.15  3rd January    2013                               

  Number of CPUs =     1


  Total number of configurations input =   1

********************************************************************************
*  Input for Configuration =   1                                               *
********************************************************************************

  Formula = O8Ce4                                                       

  Number of irreducible atoms/shells =      24


  Total number atoms/shells =      24

  Dimensionality = 3               :  Bulk   



  Cartesian lattice vectors (Angstroms) :

        5.411000    0.000000    0.000000
        0.000000    5.411000    0.000000
        0.000000    0.000000    5.411000

  Cell parameters (Angstroms/Degrees):

  a =       5.4110    alpha =  90.0000
  b =       5.4110    beta  =  90.0000
  c =       5.4110    gamma =  90.0000

  Initial cell volume =         158.428242 Angs**3

  Shrinking factors =      2     2     2

  Temperature of configuration =  0.000     K 

  Pressure of configuration =         0.000 GPa 

  Fractional coordinates of asymmetric unit :

--------------------------------------------------------------------------------
   No.  Atomic       x           y          z         Charge      Occupancy
        Label      (Frac)      (Frac)     (Frac)        (e)         (Frac)  
--------------------------------------------------------------------------------
      1 O     c    0.250000    0.250000    0.250000      0.0400    1.000000    
      2 O     c    0.250000 *  0.750000 *  0.250000 *    0.0400    1.000000    
      3 O     c    0.750000 *  0.750000 *  0.250000 *    0.0400    1.000000    
      4 O     c    0.750000 *  0.250000 *  0.250000 *    0.0400    1.000000    
      5 O     c    0.250000 *  0.250000 *  0.750000 *    0.0400    1.000000    
      6 O     c    0.250000 *  0.750000 *  0.750000 *    0.0400    1.000000    
      7 O     c    0.750000 *  0.750000 *  0.750000 *    0.0400    1.000000    
      8 O     c    0.750000 *  0.250000 *  0.750000 *    0.0400    1.000000    
      9 Ce    c    0.000000 *  0.000000 *  0.000000 *    4.2000    1.000000    
     10 Ce    c    0.500000 *  0.500000 *  0.000000 *    4.2000    1.000000    
     11 Ce    c    0.000000 *  0.500000 *  0.500000 *    4.2000    1.000000    
     12 Ce    c    0.500000 *  0.000000 *  0.500000 *    4.2000    1.000000    
     13 O     s    0.250000 *  0.250000 *  0.250000 *   -2.0400    1.000000    
     14 O     s    0.250000 *  0.750000 *  0.250000 *   -2.0400    1.000000    
     15 O     s    0.750000 *  0.750000 *  0.250000 *   -2.0400    1.000000    
     16 O     s    0.750000 *  0.250000 *  0.250000 *   -2.0400    1.000000    
     17 O     s    0.250000 *  0.250000 *  0.750000 *   -2.0400    1.000000    
     18 O     s    0.250000 *  0.750000 *  0.750000 *   -2.0400    1.000000    
     19 O     s    0.750000 *  0.750000 *  0.750000 *   -2.0400    1.000000    
     20 O     s    0.750000 *  0.250000 *  0.750000 *   -2.0400    1.000000    
     21 Ce    s    0.000000 *  0.000000 *  0.000000 *   -0.2000    1.000000    
     22 Ce    s    0.500000 *  0.500000 *  0.000000 *   -0.2000    1.000000    
     23 Ce    s    0.000000 *  0.500000 *  0.500000 *   -0.2000    1.000000    
     24 Ce    s    0.500000 *  0.000000 *  0.500000 *   -0.2000    1.000000    
--------------------------------------------------------------------------------


  Brillouin zone sampling points :

--------------------------------------------------------------------------------
  Point number          x          y          z            Weight
--------------------------------------------------------------------------------
        1           0.250000   0.250000   0.250000         0.2500
        2           0.250000   0.250000   0.750000         0.2500
        3           0.250000   0.750000   0.250000         0.2500
        4           0.250000   0.750000   0.750000         0.2500
--------------------------------------------------------------------------------


********************************************************************************
*  General input information                                                   *
********************************************************************************

  Species output for all configurations : 

--------------------------------------------------------------------------------
  Species    Type    Atomic    Atomic    Charge       Radii (Angs)     Library
                     Number     Mass       (e)     Cova   Ionic  VDW   Symbol
--------------------------------------------------------------------------------
    Ce       Core       58     140.12     4.2000   1.830  0.000  2.730          
    Ce       Shell      58       0.00    -0.2000   1.830  0.000  2.730          
    O        Core        8      16.00     0.0400   0.730  0.000  1.360          
    O        Shell       8       0.00    -2.0400   0.730  0.000  1.360          
--------------------------------------------------------------------------------


  Lattice summation method               =    Ewald
  Accuracy factor for lattice sums       =    8.000


  Time limit = Infinity

  Maximum range for interatomic potentials =    100000.000000 Angstroms

  General interatomic potentials :

--------------------------------------------------------------------------------
Atom  Types   Potential         A         B         C         D     Cutoffs(Ang)
  1     2                                                            Min    Max 
--------------------------------------------------------------------------------
Ce   c Ce   s Spring (c-s)   178.      0.00     0.00     0.00       0.000  0.600
O    c O    s Spring (c-s)   6.30      0.00     0.00     0.00       0.000  0.600
O    s O    s Buckingham    0.955E+04 0.219     32.0     0.00       0.000 12.000
O    s Ce   s Buckingham    0.181E+04 0.355     20.4     0.00       0.000 12.000
--------------------------------------------------------------------------------

********************************************************************************
*  Output for configuration   1                                                *
********************************************************************************


  Components of energy : 

--------------------------------------------------------------------------------
  Interatomic potentials     =          72.90205902 eV
  Monopole - monopole (real) =        -164.05871670 eV
  Monopole - monopole (recip)=        -331.41752867 eV
  Monopole - monopole (total)=        -495.47624537 eV
--------------------------------------------------------------------------------
  Total lattice energy       =        -422.57418636 eV
--------------------------------------------------------------------------------
  Total lattice energy       =          -40771.9493 kJ/(mole unit cells)
--------------------------------------------------------------------------------


  Number of variables =     75

  Maximum number of calculations  =          1000
  Maximum Hessian update interval =            10
  Maximum step size               =   1.000000000
  Maximum parameter tolerance     =   0.000010000
  Maximum function  tolerance     =   0.000010000
  Maximum gradient  tolerance     =   0.001000000
  Maximum gradient  component     =   0.010000000

  Symmetry not applied to optimisation

  Cell parameters to be optimised using strains

  Newton-Raphson optimiser to be used

  BFGS hessian update to be used

  Start of bulk optimisation :

  Cycle:      0 Energy:      -422.574186  Gnorm:      0.002390  CPU:    0.030
  ** Hessian calculated **
  Cycle:      1 Energy:      -422.574207  Gnorm:      0.000000  CPU:    0.040


  **** Optimisation achieved ****


  Final energy =    -422.57420658 eV
  Final Gnorm  =       0.00000000

  Components of energy : 

--------------------------------------------------------------------------------
  Interatomic potentials     =          72.96662247 eV
  Monopole - monopole (real) =        -164.08010121 eV
  Monopole - monopole (recip)=        -331.46072785 eV
  Monopole - monopole (total)=        -495.54082906 eV
--------------------------------------------------------------------------------
  Total lattice energy       =        -422.57420658 eV
--------------------------------------------------------------------------------
  Total lattice energy       =          -40771.9513 kJ/(mole unit cells)
--------------------------------------------------------------------------------

  Final fractional coordinates of atoms :

--------------------------------------------------------------------------------
   No.  Atomic        x           y          z          Radius
        Label       (Frac)      (Frac)     (Frac)       (Angs) 
--------------------------------------------------------------------------------
     1  O     c     0.250000    0.250000    0.250000    0.000000
     2  O     c     0.250000    0.750000    0.250000    0.000000
     3  O     c     0.750000    0.750000    0.250000    0.000000
     4  O     c     0.750000    0.250000    0.250000    0.000000
     5  O     c     0.250000    0.250000    0.750000    0.000000
     6  O     c     0.250000    0.750000    0.750000    0.000000
     7  O     c     0.750000    0.750000    0.750000    0.000000
     8  O     c     0.750000    0.250000    0.750000    0.000000
     9  Ce    c     0.000000    0.000000    1.000000    0.000000
    10  Ce    c     0.500000    0.500000    1.000000    0.000000
    11  Ce    c     0.000000    0.500000    0.500000    0.000000
    12  Ce    c     0.500000    0.000000    0.500000    0.000000
    13  O     s     0.250000    0.250000    0.250000    0.000000
    14  O     s     0.250000    0.750000    0.250000    0.000000
    15  O     s     0.750000    0.750000    0.250000    0.000000
    16  O     s     0.750000    0.250000    0.250000    0.000000
    17  O     s     0.250000    0.250000    0.750000    0.000000
    18  O     s     0.250000    0.750000    0.750000    0.000000
    19  O     s     0.750000    0.750000    0.750000    0.000000
    20  O     s     0.750000    0.250000    0.750000    0.000000
    21  Ce    s     0.000000    0.000000    1.000000    0.000000
    22  Ce    s     0.500000    0.500000    1.000000    0.000000
    23  Ce    s     0.000000    0.500000    0.500000    0.000000
    24  Ce    s     0.500000    0.000000    0.500000    0.000000
--------------------------------------------------------------------------------

  Final Cartesian lattice vectors (Angstroms) :

        5.410295    0.000000    0.000000
        0.000000    5.410295    0.000000
        0.000000    0.000000    5.410295


  Final cell parameters and derivatives :

--------------------------------------------------------------------------------
       a            5.410295 Angstrom     dE/de1(xx)     0.000000 eV/strain
       b            5.410295 Angstrom     dE/de2(yy)     0.000000 eV/strain
       c            5.410295 Angstrom     dE/de3(zz)     0.000000 eV/strain
       alpha       90.000000 Degrees      dE/de4(yz)    -0.000000 eV/strain
       beta        90.000000 Degrees      dE/de5(xz)    -0.000000 eV/strain
       gamma       90.000000 Degrees      dE/de6(xy)    -0.000000 eV/strain
--------------------------------------------------------------------------------

  Primitive cell volume =           158.366306 Angs**3

  Density of cell =      7.219125 g/cm**3

  Non-primitive cell volume =           158.366306 Angs**3


  Final internal derivatives :

--------------------------------------------------------------------------------
   No.  Atomic          x             y             z           Radius
        Label          (eV)          (eV)          (eV)        (eV/Angs)
--------------------------------------------------------------------------------
      1 O     c       0.000000      0.000000      0.000000      0.000000
      2 O     c       0.000000      0.000000     -0.000000      0.000000
      3 O     c       0.000000      0.000000      0.000000      0.000000
      4 O     c       0.000000     -0.000000     -0.000000      0.000000
      5 O     c      -0.000000     -0.000000      0.000000      0.000000
      6 O     c       0.000000      0.000000      0.000000      0.000000
      7 O     c       0.000000      0.000000      0.000000      0.000000
      8 O     c       0.000000     -0.000000      0.000000      0.000000
      9 Ce    c      -0.000000     -0.000000      0.000000      0.000000
     10 Ce    c       0.000000      0.000000      0.000000      0.000000
     11 Ce    c       0.000000     -0.000000      0.000000      0.000000
     12 Ce    c      -0.000000      0.000000      0.000000      0.000000
     13 O     s      -0.000000      0.000000      0.000000      0.000000
     14 O     s      -0.000000     -0.000000      0.000000      0.000000
     15 O     s       0.000000     -0.000000      0.000000      0.000000
     16 O     s       0.000000      0.000000      0.000000      0.000000
     17 O     s      -0.000000      0.000000      0.000000      0.000000
     18 O     s      -0.000000     -0.000000     -0.000000      0.000000
     19 O     s       0.000000     -0.000000     -0.000000      0.000000
     20 O     s       0.000000      0.000000     -0.000000      0.000000
     21 Ce    s       0.000000      0.000000     -0.000000      0.000000
     22 Ce    s      -0.000000     -0.000000     -0.000000      0.000000
     23 Ce    s      -0.000000      0.000000     -0.000000      0.000000
     24 Ce    s       0.000000     -0.000000     -0.000000      0.000000
--------------------------------------------------------------------------------
  Maximum abs         0.000000      0.000000      0.000000      0.000000
--------------------------------------------------------------------------------


  Born effective charge tensors : 

-------------------------------------------------------------------------------
  Atom             x           y             z
-------------------------------------------------------------------------------
   1 O     x      -1.5299     -0.0000      0.0000
           y      -0.0000     -1.5299      0.0000
           z      -0.0000     -0.0000     -1.5299
-------------------------------------------------------------------------------
   2 O     x      -1.5299     -0.0000      0.0000
           y      -0.0000     -1.5299     -0.0000
           z       0.0000     -0.0000     -1.5299
-------------------------------------------------------------------------------
   3 O     x      -1.5299     -0.0000     -0.0000
           y      -0.0000     -1.5299      0.0000
           z      -0.0000      0.0000     -1.5299
-------------------------------------------------------------------------------
   4 O     x      -1.5299     -0.0000     -0.0000
           y      -0.0000     -1.5299      0.0000
           z      -0.0000      0.0000     -1.5299
-------------------------------------------------------------------------------
   5 O     x      -1.5299      0.0000     -0.0000
           y       0.0000     -1.5299      0.0000
           z      -0.0000      0.0000     -1.5299
-------------------------------------------------------------------------------
   6 O     x      -1.5299      0.0000      0.0000
           y      -0.0000     -1.5299      0.0000
           z      -0.0000      0.0000     -1.5299
-------------------------------------------------------------------------------
   7 O     x      -1.5299      0.0000     -0.0000
           y       0.0000     -1.5299      0.0000
           z      -0.0000      0.0000     -1.5299
-------------------------------------------------------------------------------
   8 O     x      -1.5299     -0.0000     -0.0000
           y       0.0000     -1.5299      0.0000
           z       0.0000      0.0000     -1.5299
-------------------------------------------------------------------------------
   9 Ce    x       3.0598      0.0000     -0.0000
           y       0.0000      3.0598      0.0000
           z       0.0000      0.0000      3.0598
-------------------------------------------------------------------------------
  10 Ce    x       3.0598     -0.0000      0.0000
           y      -0.0000      3.0598     -0.0000
           z      -0.0000     -0.0000      3.0598
-------------------------------------------------------------------------------
  11 Ce    x       3.0598      0.0000      0.0000
           y       0.0000      3.0598     -0.0000
           z       0.0000     -0.0000      3.0598
-------------------------------------------------------------------------------
  12 Ce    x       3.0598      0.0000     -0.0000
           y       0.0000      3.0598      0.0000
           z      -0.0000      0.0000      3.0598
-------------------------------------------------------------------------------




  Elastic Constant Matrix: (Units=GPa)

-------------------------------------------------------------------------------
  Indices      1         2         3         4         5         6    
-------------------------------------------------------------------------------
       1    554.4908  124.5780  124.5780    0.0000   -0.0000   -0.0000
       2    124.5780  554.4908  124.5780    0.0000   -0.0000   -0.0000
       3    124.5780  124.5780  554.4908    0.0000   -0.0000   -0.0000
       4      0.0000    0.0000    0.0000  123.0664   -0.0000   -0.0000
       5     -0.0000   -0.0000   -0.0000   -0.0000  123.0664    0.0000
       6     -0.0000   -0.0000   -0.0000   -0.0000    0.0000  123.0664
-------------------------------------------------------------------------------


  Elastic Compliance Matrix: (Units=1/GPa)

-------------------------------------------------------------------------------
  Indices      1         2         3         4         5         6    
-------------------------------------------------------------------------------
       1    0.001965 -0.000361 -0.000361 -0.000000 -0.000000  0.000000
       2   -0.000361  0.001965 -0.000361 -0.000000  0.000000  0.000000
       3   -0.000361 -0.000361  0.001965  0.000000  0.000000  0.000000
       4   -0.000000 -0.000000  0.000000  0.008126  0.000000  0.000000
       5   -0.000000  0.000000  0.000000  0.000000  0.008126 -0.000000
       6    0.000000  0.000000  0.000000  0.000000 -0.000000  0.008126
-------------------------------------------------------------------------------

 Mechanical properties :

-------------------------------------------------------------------------------
  Convention :                    Reuss         Voigt         Hill
-------------------------------------------------------------------------------
  Bulk  Modulus (GPa)     =     267.88223     267.88223     267.88223
  Shear Modulus (GPa)     =     148.45031     159.82238     154.13635
-------------------------------------------------------------------------------
  Velocity S-wave (km/s)  =      14.33997      14.87910      14.61202
  Velocity P-wave (km/s)  =      25.40182      25.81194      25.60770
-------------------------------------------------------------------------------
  Compressibility (1/GPa) =    0.00373298
-------------------------------------------------------------------------------
  Stress axis :                     x             y             z
-------------------------------------------------------------------------------
  Youngs Moduli (GPa)     =     508.78212     508.78212     508.78212
-------------------------------------------------------------------------------
  Poissons Ratio (x)      =                     0.18345       0.18345
  Poissons Ratio (y)      =       0.18345                     0.18345
  Poissons Ratio (z)      =       0.18345       0.18345
-------------------------------------------------------------------------------


  Piezoelectric Strain Matrix: (Units=C/m**2)

-------------------------------------------------------------------------------
  Indices      1         2         3         4         5         6    
-------------------------------------------------------------------------------
       x    -0.00000  -0.00000   0.00000  -0.00000  -0.00000   0.00000
       y     0.00000   0.00000   0.00000  -0.00000  -0.00000  -0.00000
       z    -0.00000  -0.00000  -0.00000  -0.00000  -0.00000  -0.00000
-------------------------------------------------------------------------------


  Piezoelectric Stress Matrix: (Units=10**-11 C/N)

-------------------------------------------------------------------------------
  Indices      1         2         3         4         5         6    
-------------------------------------------------------------------------------
       x    -0.00000  -0.00000   0.00000  -0.00000  -0.00000   0.00000
       y    -0.00000   0.00000   0.00000  -0.00000  -0.00000  -0.00000
       z    -0.00000  -0.00000  -0.00000  -0.00000  -0.00000  -0.00000
-------------------------------------------------------------------------------


  Static dielectric constant tensor : 

-------------------------------------------------------------------------------
              x         y         z
-------------------------------------------------------------------------------
       x    18.63120   0.00000   0.00000
       y     0.00000  18.63120  -0.00000
       z     0.00000  -0.00000  18.63120
--------------------------------------------------------------------------------

  High frequency dielectric constant tensor : 

-------------------------------------------------------------------------------
              x         y         z
-------------------------------------------------------------------------------
       x     5.88045   0.00000   0.00000
       y     0.00000   5.88045  -0.00000
       z     0.00000  -0.00000   5.88045
-------------------------------------------------------------------------------

  Static refractive indices : 

-------------------------------------------------------------------------------
    1 =    4.31639      2 =    4.31639      3 =    4.31639
-------------------------------------------------------------------------------

  High frequency refractive indices : 

-------------------------------------------------------------------------------
    1 =    2.42496      2 =    2.42496      3 =    2.42496
-------------------------------------------------------------------------------


  Phonon Calculation : 

  Number of k points for this configuration =    4

--------------------------------------------------------------------------------
  K point   1 =   0.250000  0.250000  0.250000  Weight =    0.250
--------------------------------------------------------------------------------

  Frequencies (cm-1) [NB: Negative implies an imaginary mode]:

   99.58   99.58  127.46  127.46  127.46  148.88  148.88  148.88  166.94
  181.44  181.44  181.44  211.35  211.35  220.00  220.00  220.00  220.99
  220.99  220.99  270.64  270.64  270.64  278.89  278.89  278.89  279.12
  279.12  281.37  295.89  295.89  295.89  319.84  319.84  319.84  333.01


--------------------------------------------------------------------------------
  K point   2 =   0.250000  0.250000  0.750000  Weight =    0.250
--------------------------------------------------------------------------------

  Frequencies (cm-1) [NB: Negative implies an imaginary mode]:

   99.58   99.58  127.46  127.46  127.46  148.88  148.88  148.88  166.94
  181.44  181.44  181.44  211.35  211.35  220.00  220.00  220.00  220.99
  220.99  220.99  270.64  270.64  270.64  278.89  278.89  278.89  279.12
  279.12  281.37  295.89  295.89  295.89  319.84  319.84  319.84  333.01


--------------------------------------------------------------------------------
  K point   3 =   0.250000  0.750000  0.250000  Weight =    0.250
--------------------------------------------------------------------------------

  Frequencies (cm-1) [NB: Negative implies an imaginary mode]:

   99.58   99.58  127.46  127.46  127.46  148.88  148.88  148.88  166.94
  181.44  181.44  181.44  211.35  211.35  220.00  220.00  220.00  220.99
  220.99  220.99  270.64  270.64  270.64  278.89  278.89  278.89  279.12
  279.12  281.37  295.89  295.89  295.89  319.84  319.84  319.84  333.01


--------------------------------------------------------------------------------
  K point   4 =   0.250000  0.750000  0.750000  Weight =    0.250
--------------------------------------------------------------------------------

  Frequencies (cm-1) [NB: Negative implies an imaginary mode]:

   99.58   99.58  127.46  127.46  127.46  148.88  148.88  148.88  166.94
  181.44  181.44  181.44  211.35  211.35  220.00  220.00  220.00  220.99
  220.99  220.99  270.64  270.64  270.64  278.89  278.89  278.89  279.12
  279.12  281.37  295.89  295.89  295.89  319.84  319.84  319.84  333.01


--------------------------------------------------------------------------------
  Phonon properties (per mole of unit cells): Temperature =      0.000 K
--------------------------------------------------------------------------------
  Zero point energy            =        0.505456 eV
--------------------------------------------------------------------------------

  Phonon density of states : 

--------------------------------------------------------------------------------
 Frequency (cm-1) Density of States                                             
--------------------------------------------------------------------------------
    0.00000 |                                                              0.000
    5.20336 |                                                              0.000
   10.40671 |                                                              0.000
   15.61007 |                                                              0.000
   20.81343 |                                                              0.000
   26.01678 |                                                              0.000
   31.22014 |                                                              0.000
   36.42350 |                                                              0.000
   41.62685 |                                                              0.000
   46.83021 |                                                              0.000
   52.03357 |                                                              0.000
   57.23692 |                                                              0.000
   62.44028 |                                                              0.000
   67.64364 |                                                              0.000
   72.84699 |                                                              0.000
   78.05035 |                                                              0.000
   83.25371 |                                                              0.000
   88.45706 |                                                              0.000
   93.66042 |                                                              0.000
   98.86378 |*******************                                           0.056
  104.06714 |                                                              0.000
  109.27049 |                                                              0.000
  114.47385 |                                                              0.000
  119.67721 |                                                              0.000
  124.88056 |*****************************                                 0.083
  130.08392 |                                                              0.000
  135.28728 |                                                              0.000
  140.49063 |                                                              0.000
  145.69399 |*****************************                                 0.083
  150.89735 |                                                              0.000
  156.10070 |                                                              0.000
  161.30406 |                                                              0.000
  166.50742 |*********                                                     0.028
  171.71077 |                                                              0.000
  176.91413 |*****************************                                 0.083
  182.11749 |                                                              0.000
  187.32084 |                                                              0.000
  192.52420 |                                                              0.000
  197.72756 |                                                              0.000
  202.93091 |                                                              0.000
  208.13427 |*******************                                           0.056
  213.33763 |                                                              0.000
  218.54098 |***********************************************************   0.167
  223.74434 |                                                              0.000
  228.94770 |                                                              0.000
  234.15105 |                                                              0.000
  239.35441 |                                                              0.000
  244.55777 |                                                              0.000
  249.76112 |                                                              0.000
  254.96448 |                                                              0.000
  260.16784 |                                                              0.000
  265.37119 |                                                              0.000
  270.57455 |*****************************                                 0.083
  275.77791 |*************************************************             0.139
  280.98127 |*********                                                     0.028
  286.18462 |                                                              0.000
  291.38798 |*****************************                                 0.083
  296.59134 |                                                              0.000
  301.79469 |                                                              0.000
  306.99805 |                                                              0.000
  312.20141 |                                                              0.000
  317.40476 |*****************************                                 0.083
  322.60812 |                                                              0.000
  327.81148 |*********                                                     0.028
--------------------------------------------------------------------------------



  Time to end of optimisation =       0.1150 seconds


  Peak dynamic memory used =       0.63 MB 


  Timing analysis for GULP :

--------------------------------------------------------------------------------
  Task / Subroutine                                          Time (Seconds)
--------------------------------------------------------------------------------
  Calculation of reciprocal space energy and derivatives          0.0304
  Calculation of real space energy and derivatives                0.0634
--------------------------------------------------------------------------------
  Total CPU time                                                  0.1150
--------------------------------------------------------------------------------


  Job Finished at 21:19.15  3rd January    2013                               

