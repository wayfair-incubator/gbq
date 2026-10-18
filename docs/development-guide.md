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

Releases are made by merging a version bump pull request created with [hyper-bump-it][hyper-bump-it]. The version
lives in `pyproject.toml`; `gbq.__version__` is read from the installed package's metadata.

1. Make sure the `## [Unreleased]` section of `CHANGELOG.md` describes everything in the release, marking breaking
   changes. The bump turns it into the new version's section, and that section becomes the GitHub release notes.
2. Start with a local clone of the repo on the default branch with a clean working tree, and have [uv][uv-docs]
   installed.
3. Bump the version by part (`major`, `minor`, or `patch`), for example:

    ```bash
    uv run hyper-bump-it by minor
    ```

    This updates the version in `pyproject.toml`, `uv.lock` and `docs/index.md`, adds a heading for it to
    `CHANGELOG.md`, then commits to a new `bump_version_to_<version>` branch and pushes it.
4. Create a pull request for the pushed branch, keeping the `Bump version: <old> → <new>` title.
5. Get the pull request approved and merge it.

Merging the pull request starts the release:

1. The `Version Bump merged` workflow tags the merge commit as `v<version>`, creates the GitHub release with that
   version's CHANGELOG section as its notes, and starts the `Publish Release` workflow for the tag.
2. `Publish Release` checks out the tag and:
    * publishes the documentation for the version with [mike][mike]. A full release becomes the documentation's
      `latest` version; a prerelease (e.g. `2.1.0-rc.1`) doesn't.
    * builds the wheel and source distribution and uploads them to [PyPI][pypi]. This job uses
      [trusted publishing][trusted-publishing] and runs in the `Publish Release` environment, so it waits until a
      repository maintainer approves it in the workflow run.

Don't create releases by hand: the version would not be updated in the files above. If publishing fails after the
release was created, fix the cause and re-run the failed jobs, or start `Publish Release` again from the Actions tab
(**Run workflow**, with the release tag, e.g. `v2.0.0`).

### Documentation Deploys

The documentation is published to GitHub Pages from the `gh-pages` branch with [mike][mike], one directory per
version plus a `dev` version:

* Every push to `main` rebuilds the `dev` documentation (the `update-dev-docs` job of the CI workflow).
* Every release publishes its version, as described above.

The two kinds of deploy share a concurrency group, so only one pushes to `gh-pages` at a time. To preview the
documentation locally, run `docker compose run --rm --service-ports mkdocs` and open <http://localhost:8000>.

### Repository Configuration

The release relies on these settings outside the repository:

* **GitHub environment `Publish Release`** with required reviewers, which gates the PyPI upload. It needs no secrets.
* **PyPI trusted publisher** for the `gbq` project: repository `wayfair-incubator/gbq`, workflow
  `publish_release.yml`, environment `Publish Release`.
* **GitHub environment `Create Release`**, used by `Version Bump merged`. It needs no secrets: the release is created
  with the workflow's own `GITHUB_TOKEN`.
* **GitHub Pages** serving the `gh-pages` branch.
* **Repository secret `CODECOV_TOKEN`** for coverage uploads in CI.

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
[hyper-bump-it]: https://github.com/plannigan/hyper-bump-it
[trusted-publishing]: https://docs.pypi.org/trusted-publishers/
[mike]: https://github.com/jimporter/mike
