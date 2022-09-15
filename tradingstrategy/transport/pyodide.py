"""Pyodide support.

See `Pyodide <https://pyodide.org/>`_ for more information.
"""


#: Special API key used in JupyterLite client.
#:
#: This is a transitonary API key
#: that can be used from browser hosted pages like localhost
#: at tradingstrategy.ai with referrals checks in place.
#:
#: Pyodide does HTTP requests using XMLHttpRequest and
#: needs some special logic in place.
#:
PYODIDE_API_KEY = "secret-token:tradingstrategy-d15c94d954abf9d98847f88d54403720ce52e41f267f5aaf16e63fcd30256af0"

