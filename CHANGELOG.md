# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2019-11-04
### Fixed
- Added `{ useUnifiedTopology: true }` option to `MongoClient` constructor to fix the deprecation warning `DeprecationWarning: current Server Discovery and Monitoring engine is deprecated, and will be removed in a future version`

## [0.0.1] - 2019-08-08
### Added
- Initial implementation of the package that supports the same features as `$fh.db` (see [documentation](https://access.redhat.com/documentation/en-us/red_hat_mobile_application_platform_hosted/3/html/cloud_api/fh-db)) and also has Promises support.