# Changelog

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
