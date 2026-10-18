# Getting Started

## Installation

`gbq` requires Python 3.11 or later. Install it from [PyPI][pypi]:

```bash
python -m pip install gbq
```

## Introduction

`gbq` was developed internally at Wayfair to be used in a CD pipeline to create and update tables on Google BigQuery.

At Wayfair `gbq` is used with a BigQuery plugin which reads `json` schema files and updates the schemas on Google BigQuery.

## What's Next?

* The [Overview][docs-main] shows how to authenticate, create and update tables, views and stored procedures, and
  run queries.
* The [API Reference][api] documents every method of `BigQuery` and the exceptions it raises.
* The [Changelog][changelog] lists what changed in each release, including breaking changes.

[docs-main]: index.md
[api]: api.md
[changelog]: changelog.md
[pypi]: https://pypi.org/project/gbq/
