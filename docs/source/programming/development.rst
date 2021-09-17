Development
===========

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Preface
-------

In this chapter, we will discuss how to use `capitalgram` locally with your Python or Jupyter Notebook to development new strategies or the library itself.

Installation for developer
--------------------------

To install Trading Strategy using pip do:

.. code-block:: shell

    pip install -e "https://github.com/miohtama/capitalgram-onchain-dex-quant-data.git

Dataset cache
-------------

The default cacle location for the downloaded datasets is `~/.cache/capitalgram`.

```shell
ls -lha ~/.cache/capitalgram
```

```
total 56M
drwxr-xr-x  5 moo staff  160 Jul 19 23:14 ./
drwx------ 14 moo staff  448 Jul 18 15:49 ../
-rw-r--r--  1 moo staff  49M Jul 19 23:14 candles-24h.feather
-rw-r--r--  1 moo staff  95K Jul 18 15:49 exchange-universe.json
-rw-r--r--  1 moo staff 6.3M Jul 19 21:57 pair-universe.json.zstd
```

You can clear this out manually from the UNIX shell

```shell
rm -rf ~/.cache/capitalgram
```

Developing the library itself
-----------------------------

Developing locally
~~~~~~~~~~~~~~~~~~

Check out from Github.

Then

.. code-block::

    poetry shell
    poetry install


Deploying new documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Github Actions workflow will deploy on Netlify. You need Netlify `AUTH_TOKEN` and `SITE_ID`.

`AUTH_TOKEN` can be generated in the user settings.

Making a release
----------------

`Release with poetry <https://python-poetry.org/docs/cli/>`_.

.. code-block:: shell

    poetry build
    poetry publish