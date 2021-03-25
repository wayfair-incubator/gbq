# GBQ - 0.1.1

[![CI pipeline status](https://github.com/wayfair-incubator/gbq/workflows/CI/badge.svg?branch=main)][ci]
[![codecov](https://codecov.io/gh/wayfair-incubator/gbq/branch/main/graph/badge.svg)][codecov]
TODO: PyPi Release Badge

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
json_schema = {"type": "table", "schema": [{"id": "integer"}]}

# Service account email address as listed in the Google Developers Console.
svc_account = '{"type": "service_account",   "project_id": "project_id"}'

bq = BigQuery(svc_account=svc_account, project=project_id)

bq.create_or_update_structure(project_id, dataset_id, structure_id, json_schema)
```

## Where to Start?

To learn the basics of how to start using `gbq`, read the [Getting Started][getting_started] page.

## Detailed Documentation

To learn more about the various ways `gbq` can be used, read the Usage Guide page.

## API Reference

To find detailed information about a specific function or class, read the [API Reference][api_reference].

[ci]: https://github.com/wayfair-incubator/gbq/actions
[codecov]: https://codecov.io/gh/wayfair-incubator/gbq
[getting_started]: getting-started.md
