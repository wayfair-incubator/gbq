# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0] - 2026-10-18

### Added
- Support for Python 3.15
- `BigQuery(credentials=...)` accepts any `google.auth` credentials object, and
  `svc_account` is now optional: without either, Application Default Credentials are used
- `execute()` accepts `query_parameters` (use these instead of formatting values into
  SQL) and a `job_config`
- `delete_dataset()` accepts `delete_contents`

- `fetch_rows()` runs a query through the BigQuery `jobs.query` fast path and returns
  its rows, avoiding job creation and polling for short queries

### Performance
- `get_table_in_project()` lists tables for up to 8 datasets concurrently (configurable
  with `max_workers`) instead of one dataset at a time
- `delete_dataset()` and `delete_table_or_view()` make one API call instead of two
- `create_or_update_structure()` skips the update call when an existing table or view
  already matches the definition, and otherwise sends only the fields that changed.
  This also avoids BigQuery's per-table metadata update rate limit when a pipeline
  re-applies unchanged definitions

### Security
- An invalid `svc_account` now raises `GbqException` instead of `json.JSONDecodeError`
  or `ValueError`, without chaining the original error, whose `doc` attribute held the
  full key

### Removed
- Support for Python 3.10, which reached end of life on 2026-10-01

### Changed
- **Breaking:** `delete_dataset()` no longer deletes the tables, views and routines in a
  dataset unless you pass `delete_contents=True`. By default BigQuery now refuses to
  delete a non-empty dataset
- `gbq.__version__` is read from the installed package's metadata, so `pyproject.toml` is
  the single source of the version. It is `0.0.0+unknown` when `gbq` is imported from
  source without being installed
- Releases are made only through hyper-bump-it version bump pull requests; the unused
  release-please configuration was removed, and the development guide describes the flow
- When `create_or_update_structure()` updates an existing table or view, the definition
  is now the source of truth: labels, description and clustering that the definition
  omits are removed. Previously they could be set but never cleared
- Updating an existing table whose partitioning differs from the definition now raises
  `InvalidDefinitionException` instead of silently ignoring the partitioning. A changed
  time partition expiration is applied, since BigQuery allows that in place
- Methods that take a `project` argument no longer change the client's default project
  as a side effect. Previously, `execute()` ran its job in whichever project the last
  such call used; it now runs in the project `BigQuery` was created with, or in the
  project passed with the new `project=` argument (also on `fetch_rows()`)
- Upgraded `pydantic` to 2.14.1 and `google-crc32c` to 1.9.0, which ship Python
  3.15 wheels
- Development dependencies moved from `requirements*.txt` and `requirements.lock` to
  `[dependency-groups]` in `pyproject.toml`, locked in `uv.lock` for every supported Python
- CI, the devbox image and distribution builds now install a pinned uv instead of an
  unpinned install script, `pip install build` and a third-party mypy action
- Invalid structure definitions now raise `InvalidDefinitionException` instead of a raw
  `KeyError` (unknown partition, time or argument types) or a later `AttributeError`
  (a definition that is not a dict or list)

### Fixed
- The documentation's changelog page showed the text `../CHANGELOG.md` instead of the
  changelog, and the API reference page was missing. Both are restored
- Merging a version bump pull request did not release: the workflow needed a personal
  access token that wasn't configured. It now creates the release with the workflow's own
  token and starts the publish workflow itself
- Stored procedure arguments of type `ARRAY` or `STRUCT` were sent without an element type
  or fields, which BigQuery rejects. `data_type` now accepts full BigQuery types such as
  `ARRAY<INT64>` and `STRUCT<id INT64, tags ARRAY<STRING>>`, and invalid types raise
  `InvalidDefinitionException`. `JSON` and `INTERVAL` arguments are also supported
- Schema conversion kept only name, type, mode, description and nested fields, dropping
  attributes such as `policyTags`, `precision`, `scale`, `maxLength` and
  `defaultValueExpression`. Updates therefore always rewrote the schema and stripped
  policy tags set on columns. All attributes are now kept, and `STRUCT` columns get their
  nested fields like `RECORD` columns
- `get_table_in_project()` is annotated as returning `TableListItem` objects, which is
  what it has always returned, instead of `Table`
- `from gbq import *` raised `TypeError`; `__all__` now lists `BigQuery`, `GbqException`
  and `InvalidDefinitionException`, which are also importable from `gbq` directly
- `get_bq_schema_from_record` duplicated the previous field or raised `UnboundLocalError`
  for `None` values; nulls now map to a `NULLABLE STRING` field, and unsupported value
  types raise `InvalidDefinitionException`
- Parsing a structure definition no longer mutates the caller's input dictionaries

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

- Use [hatch](https://hatch.pypa.io/) for build backend.

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

