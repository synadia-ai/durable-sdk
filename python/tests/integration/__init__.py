"""Package marker so mypy sees ``integration.conftest`` — the per-test hygiene fixture — as a module
distinct from the session-level ``tests/conftest.py``. pytest still puts ``tests/`` on ``sys.path``
(the first directory above this one without an ``__init__.py``), so ``from conftest import …``
keeps resolving to the session file, as every test here expects."""
