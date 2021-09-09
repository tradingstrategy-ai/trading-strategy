Running examples
================

Preface
-------

The documentation contains various notebooks for

* How to use TradingStrategy.ai API

* How to develop your own automated trading strategies

Google Colab
------------

This is the easiest method to start your research if you are not familiar with Python.

Click "Open Colab" badge on the top of the example page in the documentation. You need a Google account. A Google cloud server will be automatically allocated for you to run the code. Currently Colab servers are free.

Local Jupyter Server
--------------------

Writing your own notebook
~~~~~~~~~~~~~~~~~~~~~~~~~

TODO

[Get an example as a starting point from Github](https://github.com/miohtama/capitalgram-onchain-dex-quant-data/tree/master/docs/source).

Check `examples and `algoritms` folder.

Save `.ipynb` file locally.

Editing examples
~~~~~~~~~~~~~~~~

This is the best method if you want to edit the existing examples in the project.

Take a git checkout.

Install using poetry.

.. code-block:: shell

    poetry install

Then start Jupyter server at the root folder.

.. code-block:: shell

    ipython notebook

Navigate to a file you want to edit in your web browser.

Terminal IPython
----------------

You might want to run notebooks in a terminal using `ipython` command e.g. for better debugging facilities.

You can run example notebooks in a terminal after git checkout and poetry install:

.. code-block:: shell

    ipython --TerminalIPythonApp.file_to_run=docs/source/examples/getting-started.ipynb

This is especially useful if you want to use `ipdb` or other well-established Python command line debuggers.