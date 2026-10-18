# GBQ - 1.1.0

[![CI pipeline status](https://github.com/wayfair-incubator/gbq/workflows/CI/badge.svg?branch=main)][ci]
[![PyPI](https://img.shields.io/pypi/v/gbq)](https://pypi.org/project/gbq/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/gbq)](https://pypi.org/project/gbq/)
[![codecov](https://codecov.io/gh/wayfair-incubator/gbq/branch/main/graph/badge.svg)][codecov]

## Example

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

## Where to Start?

To learn the basics of how to start using `gbq`, read the [Getting Started][getting_started] page.

## Detailed Documentation

To learn more about the various ways `gbq` can be used, read the Usage Guide page.

[ci]: https://github.com/wayfair-incubator/gbq/actions
[codecov]: https://codecov.io/gh/wayfair-incubator/gbq
[getting_started]: getting-started.md
[adc]: https://cloud.google.com/docs/authentication/application-default-credentials
