# Project initialization

Project uses `poetry`. Was initialized using `poetry new --name modelstar --src modelstar`.

To create the virtualenv inside the project path and for vscode recognize it, use: `poetry config virtualenvs.in-project true`

To install the local developing project into the environment in editable mode use `poetry install` which is an equivalent of `pip install -e .`.

## Publish the package on PyPI

```shell
poetry publish --build --username __token__ --password $PYPI_MODELSTAR_KEY --build --skip-existing
```
