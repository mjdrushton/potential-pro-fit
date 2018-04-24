# Changelog

## [0.7.4] - 2018-04-24
### Changed
- Set some of pro-fit's dependencies to specific versions as downstream updates to 3rd party packages had broken pprofit.
- Identified a bug in the recently added keep-alive logic which meant that it was not functioning.

## [0.7.3] - 2017-11-4
### Changed
- File transfer channels and runners now send keep-alive messages every 10 seconds. This should prevent them timing out for very long jobs.
