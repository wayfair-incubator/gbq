# Development Guide

Welcome! Thank you for wanting to make the project better. This section provides an overview on how repository structure
and how to work with the code base.

Before you dive into this, it is best to read:

* The [Code of Conduct][code of conduct]
* The [Contributing][contributing] guide

## Docker

The GBQ project uses Docker to ease setting up a consistent development environment. The Docker documentation has
details on how to [install docker][install-docker] on your computer.

Once that is configured, the test suite can be run locally:

```bash
docker compose run --rm test
```

If you want to be able to execute code in the container:

```bash
docker compose run --rm devbox
(your code here)
```

In the devbox environment you'll be able to enter a python shell and import `gbq` or any dependencies.

## Debugging

The docker container has [pdb++][pdbpp-home] install that can be used as a debugger. (However, you are welcome to set up
a different debugger if you would like.)

This allows you to easily create a breakpoint anywhere in the code.

```python
def my_function():
    breakpoint()
    ...
```

When your the code, you will drop into an interactive `pdb++` debugger.

See the documentation on [pdb][pdb-docs] and [pdb++][pdbpp-docs] for more information.

## Testing

You'll be unable to merge code unless the linting and tests pass. You can run these in your container via:

```bash
docker compose run --rm test
```

This will run the same tests, linting, and code coverage that are run by the CI pipeline. The only difference is that,
when run locally, `ruff` is configured to automatically fix issues it detects (with the `--format-code` flag).

Generally we should endeavor to write tests for every feature. Every new feature branch should increase the test
coverage rather than decreasing it.

We use [pytest][pytest-docs] as our testing framework.

#### Stages

To customize / override a specific testing stage, please read the documentation specific to that tool:

1. [PyTest][pytest-docs]
2. [MyPy][mypy-docs]
3. [Ruff][ruff-docs]
4. [Bandit][bandit-docs]

### Building the Library

`gbq` is [PEP 517][pep-517] compliant. [uv][uv-docs] is used as the frontend tool for building the library
(`uv build`), and [Hatchling][hatchling] is the build backend. `pyproject.toml` contains the library metadata.

### Dependencies

All dependencies are declared in `pyproject.toml` and managed with [uv][uv-docs]:

* **`[project] dependencies`** - Runtime dependencies of the library, installed by anyone who installs `gbq`.
* **`[dependency-groups]`** - Development dependencies, pinned to exact versions:
    * `test` - the test suite and lints
    * `docs` - building and publishing the documentation
    * `dev` - everything in `test` and `docs`, plus local debugging tools. This is the default group.
* **`uv.lock`** - The fully resolved lock file for every supported Python version. CI and the devbox install from it
  with `uv sync --locked`, which fails if it is out of date with `pyproject.toml`.

After changing dependencies in `pyproject.toml`, update the lock file and rebuild the devbox:

```bash
docker compose run --rm lock
docker compose build devbox
```

To upgrade every locked dependency to its latest allowed version, run `docker compose run --rm lock --upgrade`.
Renovate also opens a weekly lock file maintenance pull request that does this.

## Publishing a New Version

Once the package is ready to be released, there are a few things that need to be done:

1. Start with a local clone of the repo on the default branch with a clean working tree.
2. Have [uv][uv-docs] installed.
3. Perform the version bump part name (`major`, `minor`, or `patch`).
    Example: `uv run hyper-bump-it by minor`

    This wil create a new branch, updates all affected files with the new version, commit the changes to the branch, and 
    push the branch.
4. Create a new pull request for the pushed branch.
5. Get the pull request approved.
6. Merge the pull request to the default branch.

Merging the pull request will trigger a GitHub Action that will create a new release. The creation of this new
release will trigger a GitHub Action that will to build a wheel & a source distributions of the package and push them to
[PyPI][pypi].

!!! warning
    The action that uploads the files to PyPI will not run until a repository maintainer acknowledges that the job is
    ready to run. This is to keep the PyPI publishing token secure. Otherwise, any job would have access to the token. 

In addition to uploading the files to PyPI, the documentation website will be updated to include the new version. If the
new version is a full release, it will be made the new `latest` version.

## Continuous Integration Pipeline

The Continuous Integration (CI) Pipeline runs to confirm that the repository is in a good state. It will run when 
someone creates a pull request or when they push new commits to the branch for an existing pull request. The pipeline
runs multiple different jobs that helps verify the state of the code.

This same pipeline also runs on the default branch when a maintainer merges a pull request.

### Lints

The first set of jobs that run as part of the CI pipeline are linters that perform static analysis on the code. This
includes: [MyPy][mypy-docs], [Ruff][ruff-docs], and [Bandit][bandit-docs].

### Tests

The next set of jobs run the unit tests using [PyTest][pytest-docs]. The pipeline runs the tests cases across each
supported version of Python to ensure compatibility.

For each run of the test cases, the job will record the test results and code coverage information. The pipeline uploads
the code coverage information to [CodeCov][codecov] to ensure that a pull request doesn't significantly reduce the total
code coverage percentage or introduce a large amount of code that is untested.

### Distribution Verification

The next set of jobs build the wheel distribution, installs in into a virtual environment, and then runs Python to
import the library version. This works as a smoke test to ensure that the library can be packaged correctly and used.
The pipeline runs the tests cases across each supported version of Python to ensure compatibility.

[code of conduct]: https://github.com/wayfair-incubator/gbq/blob/main/CODE_OF_CONDUCT.md
[contributing]: https://github.com/wayfair-incubator/gbq/blob/main/CONTRIBUTING.md
[install-docker]: https://docs.docker.com/install/
[pdbpp-home]: https://github.com/pdbpp/pdbpp
[pdb-docs]: https://docs.python.org/3/library/pdb.html
[pdbpp-docs]: https://github.com/pdbpp/pdbpp#usage
[pytest-docs]: https://docs.pytest.org/en/latest/
[mypy-docs]: https://mypy.readthedocs.io/en/stable/
[ruff-docs]: https://docs.astral.sh/ruff/
[bandit-docs]: https://bandit.readthedocs.io/en/stable/
[sem-ver]: https://semver.org/
[pep-517]: https://www.python.org/dev/peps/pep-0517
[uv-docs]: https://docs.astral.sh/uv/
[hatchling]: https://hatch.pypa.io/latest/
[pypi]: https://pypi.org/project/gbq/
[codecov]: https://about.codecov.io/
