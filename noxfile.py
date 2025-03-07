"""
This `noxfile.py` is configured to run the test suite with multiple versions of
Python and multiple versions of psycopg.
"""

import contextlib
import os
import tempfile
from typing import IO, Generator

import nox


# Use uv to manage venvs.
nox.options.default_venv_backend = "uv"


@contextlib.contextmanager
def temp_lock_file() -> Generator[IO[str], None, None]:
    with tempfile.NamedTemporaryFile(mode="w", prefix="packages.", suffix=".txt") as f:
        yield f


@nox.session()
@nox.parametrize(
    "dependency_file",
    [
        nox.param("pytest-in-nox-psycopg3", id="psycopg3"),
        nox.param("pytest-in-nox-psycopg2", id="psycopg2"),
    ],
)
@nox.parametrize(
    "python",
    [
        nox.param("3.10", id="python=3.10"),
        nox.param("3.11", id="python=3.11"),
        nox.param("3.12", id="python=3.12"),
    ],
)
def tests(session: nox.Session, dependency_file: str) -> None:
    """
    Run the test suite.
    """
    with temp_lock_file() as lock_file:
        # Compile a new development lock file with the additional package
        # constraints from this session. Use a unique lock file name to avoid
        # session pollution.
        session.run(
            "uv",
            "pip",
            "compile",
            "--quiet",
            "--resolver=backtracking",
            "--strip-extras",
            f"--extra={dependency_file}",
            "pyproject.toml",
            "--output-file",
            lock_file.name,
        )

        # We have to open the file again since after `session.run` is
        # called, a `lock_file.write()` call won't write to the file anymore.
        with open(lock_file.name, "a") as f:
            # Add this project dependency to the lock file.
            f.write(".\n")

        # Use `uv sync` so that all packages that aren't included in the
        # dependency file are removed beforehand. This allow us to not end up
        # with two versions of a varying library (psycopg, for example).
        session.run("uv", "pip", "sync", lock_file.name)

    # Add the project directory to the PYTHONPATH for running the test Django
    # project.
    project_dir = os.path.abspath(".")
    session.env["PYTHONPATH"] = project_dir

    session.run("pytest", *session.posargs)
