Running examples
================

Preface
-------

The documentation contains various notebooks for

* How to use Capitalgram API

* How to develop your own automated trading strategies

Running in Google Colab
-----------------------

This is the easiest method to start your research if you are not familiar with Python.

Click "Open Colab" badge on the top of the example page in the documentation. You need a Google account. A Google cloud server will be automatically allocated for you to run the code. Currently Colab servers are free.

Running in terminal IPython
---------------------------

You might want to run notebooks in a terminal using `ipython` command e.g. for better debugging facilities.

You can run example notebooks in a terminal after git checkout and poetry install:

.. code-block:: shell

    ipython --TerminalIPythonApp.file_to_run=docs/source/examples/getting-started.ipynb
