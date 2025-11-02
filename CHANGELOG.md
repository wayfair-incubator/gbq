# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0] - 2025-11-02

### Added
- Support for Python 3.10, 3.11, 3.12, 3.13, and 3.14
- Replaced `pip` with `uv` package manager for faster dependency resolution
- Replaced `black`, `isort`, `flake8`, and `autoflake` with `ruff` for unified linting and formatting

### Changed
- Upgraded Pydantic from v1 to v2.12.3
- Upgraded all core dependencies to latest versions:
  - `google-cloud-bigquery` to 3.38.0
  - `google-api-core` to 2.28.1
  - `google-auth` to 2.42.1
  - `google-cloud-core` to 2.5.0
  - `google-crc32c` to 1.7.1
  - `google-resumable-media` to 2.7.2
  - `googleapis-common-protos` to 1.71.0
- Updated documentation dependencies for Python 3.14 compatibility:
  - `mkdocs` to 1.6.1
  - `mkdocs-material` to 9.5.49
  - `mkdocstrings[python]` to 0.27.0
  - `mike` to 2.1.1
- Updated CI workflow to use Docker Compose V2 (`docker compose` instead of `docker-compose`)
- Moved bandit configuration from `.bandit` file to `pyproject.toml`
- Updated `pyproject.toml` with ruff configuration for linting and formatting

### Removed
- Support for Python 3.8 and 3.9
- Individual linter configurations (`.flake8`, `[tool.black]`, `[tool.isort]`)

### Fixed
- Fixed `SchemaField` API compatibility with google-cloud-bigquery 3.x
- Fixed `QueryJob` instantiation in tests for google-cloud-bigquery 3.x compatibility
- Fixed REPEATED mode detection for list-based schema fields in `get_bq_schema_from_record()`
- Fixed Pydantic v2 `model_validator` compatibility issues
- Fixed Docker Compose V2 compatibility in CI workflows

### Internal
- Updated Dockerfile to use Python 3.14
- Installed `uv` in Docker images for package management
- Updated test fixtures and helpers for new API compatibility
- Regenerated `requirements.lock` with updated dependencies

## [1.0.5] - 2024-08-01

### Fixed
- Fix BigQuery SchemaField descriptions

### Internal

- Use [hatch][hatch] for build backend.

## [1.0.4] - 2023-10-24

### Changed
- Upgrade to Pydantic V2

## [1.0.2] - 2023-10-10

### Fixed
- Docs publishing pipeline

## [1.0.2] - 2023-10-10

### Changed
- Update dependencies

## [1.0.1] - 2022-12-27

### Changed
- Update dependencies

## [1.0.0] - 2022-09-19

### Added
- Support for executing a SQL statement

### Changed
- Removed support for creating views using SQL statement
- Update dependencies

## [0.5.0] - 2022-02-05

- Remove support for Python 3.6
- Update dependencies

## [0.4.0] - 2021-11-28

- Monthly release to keep dependencies up to date

## [0.3.0] - 2021-10-27

### Added

- Python version `3.10` tested during CI
- Python version `3.10` added to package classifiers

## [0.2.0] - 2021-08-07

* Updated dependencies to most recent version
* Updated CI documentation

## [0.1.5] - 2021-06-01

### Updated

* Updated dependencies to most recent version

## [0.1.4] - 2021-04-26

### Added

* APIs to delete `dataset` and `table`

## [0.1.3] - 2021-03-25

### Fixed

* `dataclasses` and `typing_extensions` was not listed as a dependency for versions of Python < 3.6

## [0.1.2] - 2021-03-25

### Fixed

Fix Project Classifiers

## [0.1.1] - 2021-03-25

### Added

Initial Release

