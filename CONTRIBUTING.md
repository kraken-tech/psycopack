# Contributing to Psycopack

## Installing development dependencies

```sh
make dev
```

## Running tests

The entire test suite can be run with:

```sh
make test
```

The test suite is supported by pystest. You can pass flags to pytest by running:

```sh
PYTEST_FLAGS="tests/test_repack.py --pdb -vv" make test
```

To run tests against a specific database, set the `DATABASE_URL` environment variable:

```sh
DATABASE_URL=postgres://marcelo.fernandes:postgres@localhost:5415/postgres make test
```

## Coverage

We try to keep code coverage as high as possible. There are a few coverage commands:

Run tests and produce a report in the terminal:

```sh
make coverage
```

Run tests and produce a rich HTML report (more info than int the terminal command):

```sh
make coverage_html
```

## Linter

We use ruff and mypy for linting. To run both linters, run:

```sh
make lint
```
