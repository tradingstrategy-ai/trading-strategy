"""Check we manage to import and create API client under Pyodide."""

from pytest_pyodide import run_in_pyodide


@run_in_pyodide
def test_pyodide_smoke(selenium):
    """Does Pyodide work at all?"""
    assert 1 + 1 == 2


@run_in_pyodide
def test_pyodide_document(selenium):
    """Access JS document scope"""
    # https://pyodide.org/en/stable/usage/type-conversions.html?highlight=window#importing-javascript-objects-into-python
    import js
    assert js.document.title == "pyodide"


