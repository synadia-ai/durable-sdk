# Publish the Python package

`synadia-durable` goes to PyPI from a tag `python-v<version>`, by the workflow `.github/workflows/release-python.yml` and PyPI's trusted publishing: the publishing job gets a short-lived credential from GitHub's OIDC token, in the environment `pypi`. No PyPI token belongs in this repository or its settings.

## What the workflow does

A pull request that changes the package (`python/`) or the pipeline (`release-python.yml`, `scripts/python-release.py`, its tests) runs the first three jobs. A `python-v*` tag runs all four.

| Job       | What it does                                                                                                                                                                                                                                                                                                                                                                                                                     |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `source`  | `ruff check`, `ruff format --check`, `mypy` and `pytest` in `python/`, without an engine; `check-suite` then reads the JUnit report and fails unless the unit suites passed with at most a handful of skips and every integration test was skipped, most for want of an engine (a skip, never a pass of zero)                                                                                                                    |
| `build`   | the release controls' own tests; the tag must name `python/pyproject.toml`'s version; the sdist from the committed source (`git archive`), the wheel from that sdist, both with the pinned build backend; both inspected (names, members, metadata, links, the public boundary, the backend that built the wheel, the wheel's package byte for byte the sdist's) and their checksums recorded in `artifacts.json`; uploaded once |
| `verify`  | downloads that upload, checks the checksums and the source commit, and installs the wheel and the sdist on Python 3.11, 3.12 and 3.13, each alone and with the `crypto` extra, then runs the smoke there                                                                                                                                                                                                                         |
| `publish` | on a tag only, in environment `pypi`, the only job with `id-token: write`: downloads the same upload, verifies it again, and hands those bytes to `pypa/gh-action-pypi-publish`. It never rebuilds                                                                                                                                                                                                                               |

The smoke imports the installed package, checks its version, builds a client and a worker on a connection that is never opened, and runs a workflow and its replay in memory (the step is not run again); with the extra it round-trips a payload through `aes_gcm_payload_codec`, and without it checks that the codec names the extra. It opens no network connection. Every action is pinned by commit.

**External dependencies** (the build backend, and what the install checks resolve: `nats-py`, `cryptography`) resolve as of the cutoff in `scripts/python-release.py`, `CUTOFF = "2026-11-09T00:00:00Z"`, at least seven days before the release. The release build pins the backend with a build constraint, `BUILD_BACKEND = "hatchling==1.33.0"`, and `inspect` checks that the wheel names it as its generator; the published sdist keeps the project's own `hatchling>=1.27` (the licence expression needs 1.27). A user's install is not pinned: the package asks for `nats-py>=2.15.0` and, with the extra, `cryptography>=42`. **For a later release**, move `CUTOFF` deliberately to a date at least seven days before it, take the newest `hatchling` released before that date for `BUILD_BACKEND` (or keep the one there), and commit both in the release's pull request. The artifact record carries both; `verify` refuses a record made under another policy.

**Stated honestly:** the workflow never runs the integration suites; the engine is not public.

## Once, in the repository

1. **Actions:** check that they are allowed. Settings → Actions → General: at least `actions/checkout`, `actions/upload-artifact`, `actions/download-artifact`, `astral-sh/setup-uv` and `pypa/gh-action-pypi-publish`.
2. **The environment.** Settings → Environments → New environment, name `pypi`. Under "Deployment branches and tags" choose "Selected branches and tags", then "Add deployment branch or tag rule": ref type **Tag**, name pattern `python-v*`. Add no branch rule: a branch rule does not admit the tag's run, and the environment must accept no branch or pull-request deployment. Required reviewers are optional; the environment holds no secret.
3. **Tag protection** (recommended): a tag ruleset. Settings → Rules → Rulesets → New ruleset → New tag ruleset; target the pattern `python-v*`; restrict creations, updates and deletions; let only the release maintainers bypass it.

## Each release

**1. Qualify against the engine.** The release commit is qualified by hand against the engine before the tag: the full suite, with the variables `docs/DEVELOPING.md` names (Testing), and no test failing.

**2. Build and install locally** (optional: the workflow does the same), with Python ≥ 3.11 and `uv` on `PATH`, into a new directory outside the checkout:

```sh
python3 -m unittest discover -s scripts/tests -p test_python_release.py -v
python3 scripts/python-release.py build --version 0.2.0 --artifacts <new directory>
python3 scripts/python-release.py verify --version 0.2.0 --artifacts <new directory> --install
uvx twine check --strict <new directory>/*.whl <new directory>/*.tar.gz
```

**3. The pending publisher on PyPI** (first release only), just before the tag. A pending publisher reserves nothing: the name is taken by whoever uploads first, and the project is created under the account that registered the publisher. PyPI → Account settings → Publishing → "Add a new pending publisher" → GitHub, with exactly:

| Field             | Value                |
| ----------------- | -------------------- |
| PyPI Project Name | `synadia-durable`    |
| Owner             | `synadia-ai`         |
| Repository name   | `durable-sdk`        |
| Workflow name     | `release-python.yml` |
| Environment name  | `pypi`               |

**4. Tag** the merge commit on `main` whose `python/pyproject.toml` names the version; the build refuses a tag that names another:

```sh
git switch main && git pull
git tag -a python-v0.2.0 -m "synadia-durable 0.2.0"
git push origin python-v0.2.0
```

Then follow the run under Actions.

- **A job before `publish` fails:** nothing was uploaded. Fix, merge, and tag the new commit (delete the failed tag first if it names the same version).
- **`publish` fails** (a publisher value that does not match, for example): correct the publisher on PyPI, then _Re-run failed jobs_ on the same run. It takes the same artifacts (kept 14 days); no new tag. PyPI never accepts a file name twice, so a partial upload (one of the two files) needs a new version.

**5. After the upload:** `pip install synadia-durable==<version>` in a new environment, and the date of the upload in `CHANGELOG.md`. First release only: the project belongs to the account that registered the publisher, and the pending publisher is now the project's trusted publisher; add the other owners (PyPI → Your projects → `synadia-durable` → Manage → Collaborators).
