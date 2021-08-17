Troubleshooting
===============

Preface
-------

Some common issues you might encounter and how to tackle them.

Resetting the API key
---------------------

You can reset the API key by deleting the config file:

.. code-block:: shell

    rm ~/.capitalgram/settings.json

Resetting the download cache
----------------------------

Downloaded files might get corrupted e.g. due to partial download.

You can see a message like:

.. code-block:: txt

    OSError: Could not open parquet input source '<Buffer>': Invalid: Parquet magic bytes not found in footer. Either the file is corrupted or this is not a parquet file.

To fix this issue, you can remove all files in the download cache.

.. code-block:: shell

    rm -rf ~/.cache/capitalgram/


