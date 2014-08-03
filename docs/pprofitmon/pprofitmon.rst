.. _pprofitmon:

**********
pprofitmon
**********

This tool provides is a web browser based monitor used to analyse the progress of :ref:`pprofit` runs. 

Usage
=====

::

	pprofitmon [options] 


Run monitor from the same directory as ``pprofit`` run and then open http://localhost:8080 in a web browser.

Options:
^^^^^^^^

``-h, --help``            	show help message and exit
``-p PORT, --port=PORT``    set the port on which fitting monitor runs (useful for running multiple instances of the monitor on the same machine). By default this is port 8080.


See Also
========

* :ref:`extending_json`
  