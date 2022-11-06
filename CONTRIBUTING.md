# Project initialization

Project uses `poetry`. Was initialized using `poetry new --name modelstar --src modelstar`.

To create the virtualenv inside the project path and for vscode recognize it, use: `poetry config virtualenvs.in-project true`

To install the local developing project into the environment in editable mode use `poetry install` which is an equivalent of `pip install -e .`.

## Publish the package on PyPI

```shell
poetry publish --build --username __token__ --password $PYPI_MODELSTAR_KEY --build --skip-existing
```

## Using with `pyenv` + `poetry`

- Find out which and where is the Python version that is needed (Py-3.8), using: `pyenv which python`
- Use that path to activate the env of Poetry: `poetry env use <path>`
- Install the dependencies for the project: `poetry install`
- Spawn the virtual environment: `poetry shell`