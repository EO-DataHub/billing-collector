# UK EO Data Hub Platform: Billing Collector

Tracks CPU and memory usage for each user's workspace.

Uses **Prometheus**  to query resource usage and uses **Apache Pulsar** to send billing events every X seconds.


### For Local Testing

You must have `kubectl` connected to the correct environment's k8s cluster.

```bash
k port-forward -n pulsar svc/pulsar-proxy 6650:6650 # in one terminal

k port-forward svc/prometheus-server 9090:9090 -n prometheus # in another terminal

cd billing_collector
python consumer.py # in another terminal

python -m billing_collector # in another terminal
```


# Development of this component

## Getting started

### Prerequisites

You will need [uv](https://docs.astral.sh/uv/) installed.

### Install via makefile

```commandline
make setup
```

This will install all dependencies via `uv sync` and set up `pre-commit` hooks.

It's safe and fast to run `make setup` repeatedly as it will only update things if they have changed.

After `make setup` you can run `make pre-commit` to run pre-commit checks on staged changes and
`make pre-commit-all` to run them on all files.

## Building and testing

This component uses `pytest` tests and `ruff` for linting and formatting, and `pyright` for type checking.

A number of `make` targets are defined:
* `make test`: run tests continuously (via pytest-watcher)
* `make testonce`: run tests once
* `make format`: format and fix lint issues
* `make check`: run all linting, formatting, type checking and pyproject validation
* `make dockerbuild`: build a `latest` Docker image (use `make dockerbuild VERSION=1.2.3` for a release image)
* `make dockerpush`: push a `latest` Docker image (again, you can add `VERSION=1.2.3`) - normally this should be done
  only via the build system and its GitHub actions.

## Managing dependencies

Dependencies are specified in `pyproject.toml` and locked in `uv.lock`. After changing dependencies:

* Run `uv sync` (or `make update`) to regenerate the lockfile and install.
* Test your changes.
* Commit `pyproject.toml` and `uv.lock`.

## Releasing

Ensure that `make check` and `make testonce` pass before continuing.

Releases tagged `latest` and targeted at development environments can be created from the `main` branch. Releases for
installation in non-development environments should be created from a Git tag named using semantic versioning. For
example, using

* `git tag v1.2.3`
* `git push --tags`

Docker images will be built automatically after pushing to the EO-DataHub repos via GitHub Actions.
