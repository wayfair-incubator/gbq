[![CI pipeline status](https://github.com/wayfair-incubator/gbq/workflows/CI/badge.svg?branch=main)][ci]
[![PyPI](https://img.shields.io/pypi/v/gbq)][pypi]
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/gbq)][pypi]
[![codecov](https://codecov.io/gh/wayfair-incubator/gbq/branch/main/graph/badge.svg)][codecov]
[![Checked with mypy](https://img.shields.io/badge/mypy-checked-blue)][mypy-home]
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)][ruff-home]


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
json_schema = {
    "type": "table",
    "schema": [{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}],
}

# Authenticates with Application Default Credentials: Workload Identity, an attached
# service account, GOOGLE_APPLICATION_CREDENTIALS, or `gcloud auth application-default login`.
bq = BigQuery(project=project_id)

bq.create_or_update_structure(project_id, dataset_id, structure_id, json_schema)
```

### Authentication

`BigQuery` uses [Application Default Credentials][adc] unless you pass credentials explicitly.
Prefer this, or a `google.auth` credentials object, over long-lived service account keys:

```python
import google.auth
from google.auth import impersonated_credentials

source, _ = google.auth.default()
credentials = impersonated_credentials.Credentials(
    source_credentials=source,
    target_principal="deployer@project_id.iam.gserviceaccount.com",
    target_scopes=["https://www.googleapis.com/auth/bigquery"],
)
bq = BigQuery(project=project_id, credentials=credentials)

# A service account key as a JSON string is still supported
bq = BigQuery(svc_account=key_json, project=project_id)
```

### Running Queries

Pass values as query parameters instead of formatting them into the SQL string, so they
can't change the query (SQL injection):

```python
from google.cloud import bigquery

job = bq.execute(
    "SELECT * FROM `project_id.dataset_id.users` WHERE name = @name",
    query_parameters=[bigquery.ScalarQueryParameter("name", "STRING", user_input)],
)
rows = list(job.result())
```

## Documentation

Check out the [project documentation](https://wayfair-incubator.github.io/gbq/)

[ci]: https://github.com/wayfair-incubator/gbq/actions
[codecov]: https://codecov.io/gh/wayfair-incubator/gbq
[mypy-home]: http://mypy-lang.org/
[ruff-home]: https://github.com/astral-sh/ruff
[install-docker]: https://docs.docker.com/install/
[pdbpp-home]: https://github.com/pdbpp/pdbpp
[pdb-docs]: https://docs.python.org/3/library/pdb.html
[pdbpp-docs]: https://github.com/pdbpp/pdbpp#usage
[pytest-docs]: https://docs.pytest.org/en/latest/
[mypy-docs]: https://mypy.readthedocs.io/en/stable/
[ruff-docs]: https://docs.astral.sh/ruff/
[bandit-docs]: https://bandit.readthedocs.io/en/stable/
[sem-ver]: https://semver.org/
[pypi]: https://pypi.org/project/gbq/
[gbq-docs]: https://wayfair-incubator.github.io/gbq/
[adc]: https://cloud.google.com/docs/authentication/application-default-credentials
