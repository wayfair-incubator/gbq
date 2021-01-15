[![CI pipeline status](https://github.com/wayfair-incubator/gbq/workflows/CI/badge.svg?branch=main)][ci]
TODO: PyPi Release Badge
[![codecov](https://codecov.io/gh/wayfair-incubator/gbq/branch/main/graph/badge.svg)][codecov]
[![Checked with mypy](https://img.shields.io/badge/mypy-checked-blue)][mypy-home]
[![Code style: black](https://img.shields.io/badge/code%20style-black-black.svg)][black-home]


# GBQ

Python wrapper for interacting Google BigQuery.

This package provides an interface by wrapping Google's low level library. It exposes options to provide input as json objects which can be used for various CI/CD tools.

## Usage

TODO: Add usage documentation.

## Develop

The GBQ project uses Docker to ease setting up a consistent development environment. The Docker documentation has
details on how to [install docker](https://docs.docker.com/install/) on your computer.

Once that is configured, the test suite can be run locally:

```bash
docker-compose run --rm test
```

If you want to be able to execute code in the container:

```bash
docker-compose run --rm devbox
(your code here)
```

In the devbox environment you'll be able to enter a python shell and import `gbq` or any dependencies.

## Testing

You'll be unable to merge code unless the linting and tests pass. You can run these in your container via:

```bash
docker-compose run --rm test
```

This will run the same tests, linting, and code coverage that are run by the CI pipeline. The only difference is that,
when run locally, `black` and `isort` are configured to automatically correct issues they detect.

Generally we should endeavor to write tests for every feature. Every new feature branch should increase the test
coverage rather than decreasing it.

We use [pytest][pytest-docs] as our testing framework.

#### Stages

To customize / override a specific testing stage, please read the documentation specific to that tool:

1. [PyTest][pytest-docs]
2. [MyPy][mypy-docs]
3. [Black][black-docs]
4. [Isort][isort-docs]
4. [Flake8][flake8-docs]
5. [Bandit][bandit-docs]

### `setup.py`

Setuptools is used to packaging the library.

**`setup.py` must not import anything from the package** When installing from source, the user may not have the
packages dependencies installed, and importing the package is likely to raise an `ImportError`. For this reason, the
**package version should be obtained without importing**. This is explains why `setup.py` uses a regular expression to
grabs the version from `__init__.py` without actually importing.

### Requirements

* **requirements.txt** - Lists all direct dependencies (packages imported by the library).
* **Requirements-test.txt** - Lists all direct requirements needed to run the test suite & lints.

## Publishing the Package

TODO: Not currently implemented.

Once the package is ready to be released, there are a few things that need to be done:

1. Create a new Pull Request that increase the version number from the previous release in `gbq/__init__.py` and
    `CHANGELOG.md`. [Semantic versioning][sem-ver] is used to make it clear to people using your package what types of
    changes are in the new version.
2. Ensure this change has been merged to the default branch with all the code that should be included in the release.
3. Create a new GitHub release. The tag should be named: `vX.Y.Z`

This will trigger the CI system to build a wheel, and a source distributions of the package and push them to
[PyPI][pypi].

## Documentation

TODO: Use Github Pages to host documentation.
Check out the [project documentation][gbq-docs]

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
[gbq-docs]: https://github.com/wayfair-incubator/gbq/
