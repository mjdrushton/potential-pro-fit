# Changelog

## [0.10.0] - Unreleased
### Changed

### Added
- New feature job-tasks:
    + `Potable` job-task allows tabulation of `atsim.potentials` potable files within a fitting run.
- Added the 'bad_merit_substitute' for the `[FittingRun]` section of `fit.cfg`. This allows invalid merit values (nan) to be substituted with a large float.
- Added the --iteration/-i option to the ppdump tool. This allows the last, best or numbered steps from a fitting run to be dumped.
- Population minimizers:
    + Population based minimizers now include initial variable values in their initial populations, this behaviour can be controlled using the `population_include_orig_vars` fit.cfg option.
    + Initial populations can be loaded from a CSV file using the new `population_load_from_csv` fit.cfg option or from a `ppdump` formatted CSV file using the `population_load_from_ppdump` option.
    + Initial population selection is performed using the Latin Hypercube Method
    + Initial population selection can be biased to initial variable values using the ``population_distribution : bias`` configuration option.


### Fixed
- Reset terminal after a run. This should avoid the need for users to type 'reset' after each run.
- actually made the Simulated_Annealing minimizer useful by initialising the minimizer with original variable values.
    

## [0.9.0] - 2019-6-27
### Changed
- Updated Potential Pro-Fit to use Python 3. Python 2.7 is now not supported for the main code base. Remote runners will still support python 3 to allow jobs to run on legacy systems.

### Added
- Plugins installed into the `atsim.pro_fit.plugins` namespace are now registered automatically with `pprofit`

## [0.8.3] - 2019-6-4
### Fixed
- csvbuild "File overwriting disabled" messages now no longer appear in the log file.
- Configuration errors experienced during minimisation are (e.g. bad variable name) are now shown correctly in the console.
- Dependency `more-itertools` no longer supports python 2.7 - this was causing problems in running `pprofitmon` for some users. Setup.py now specifies an earlier version. 


## [0.8.2] - 2018-12-18
### Fixed
- Bug when formatting error message for bad nprocesses option for Local runner.
- `pprofitmon` was not working because Cherrypy versions >=18.0.0 no longer support python 2.7.


## [0.8.1] - 2018-10-22
### Fixed
- Added `ppversion.py` file to `MANIFEST.in` - missing file was preventing installation from source distributions.


## [0.8.0] - 2018-10-01
### Added
- Slurm runner. Allows jobs to run against Slurm queuing system.
- SGE runner. Runner for Sun Grid Engine queueing systems.
- PBS, Slurm, SGE and Remote runners have added options: 
  + `sshconfig` option allowing OpenSSH ssh_config style options to be used when establishing connections.
  + `debug.disable-cleanup` allows job files to preserved on remote machine.
- `--disable-console` option for the `pprofit` command. Primarily his was added to enable easier debugging.


## [0.7.5] - 2018-07-01
### Changed
- Allow python style format strings to be used in template placeholders.
- Fixed bug that was preventing the `pprofit --create-files` option from working.

## [0.7.4] - 2018-04-24
### Changed
- Set some of pro-fit's dependencies to specific versions as downstream updates to 3rd party packages had broken pprofit.
- Identified a bug in the recently added keep-alive logic which meant that it was not functioning.

## [0.7.3] - 2017-11-4
### Changed
- File transfer channels and runners now send keep-alive messages every 10 seconds. This should prevent them timing out for very long jobs.
