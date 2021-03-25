[![CI pipeline status](https://github.com/wayfair-incubator/gbq/workflows/CI/badge.svg?branch=main)][ci]
[![PyPI](https://img.shields.io/pypi/v/gbq)][pypi]
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/gbq)][pypi]
[![codecov](https://codecov.io/gh/wayfair-incubator/gbq/branch/main/graph/badge.svg)][codecov]
[![Checked with mypy](https://img.shields.io/badge/mypy-checked-blue)][mypy-home]
[![Code style: black](https://img.shields.io/badge/code%20style-black-black.svg)][black-home]


# GBQ

Python wrapper for interacting Google BigQuery.

This package provides an interface by wrapping Google's low level library. It exposes options to provide input as json objects which can be used for various CI/CD tools.

## Usage

### Basic Usage

```python
from gbq import BigQuery

# BigQuery project id as listed in the Google Developers Console.
project_id = 'project_id'

# BigQuery dataset id as listed in the Google Developers Console.
dataset_id = 'dataset_id'

# BigQuery table/view id as listed in the Google Developers Console.
structure_id = 'structure_id'

# BigQuery structure definition as defined in the Google Developers Console.
json_schema = {"type": "table", "schema": [{"id": "integer"}]}

# Service account email address as listed in the Google Developers Console.
svc_account = '{"type": "service_account",   "project_id": "project_id"}'

bq = BigQuery(svc_account=svc_account, project=project_id)

bq.create_or_update_structure(project_id, dataset_id, structure_id, json_schema)
```

## Documentation

Check out the [project documentation](https://wayfair-incubator.github.io/gbq/)

[ci]: https://github.com/wayfair-incubator/gbq/actions
[codecov]: https://codecov.io/gh/wayfair-incubator/gbq
[mypy-home]: http://mypy-lang.org/
[black-home]: https://github.com/psf/black
[install-docker]: https://docs.docker.com/install/
[pdbpp-home]: https://github.com/pdbpp/pdbpp
[pdb-docs]: https://docs.python.org/3/library/pdb.html
[pdbpp-docs]: https://github.com/pdbpp/pdbpp#usage
[pytest-docs]: https://docs.pytest.org/en/latest/
[mypy-docs]: https://mypy.readthedocs.io/en/stable/
[black-docs]: https://black.readthedocs.io/en/stable/
[isort-docs]: https://pycqa.github.io/isort/
[flake8-docs]: http://flake8.pycqa.org/en/stable/
[bandit-docs]: https://bandit.readthedocs.io/en/stable/
[sem-ver]: https://semver.org/
[pypi]: https://semver.org/
[gbq-docs]: https://wayfair-incubator.github.io/gbq/
