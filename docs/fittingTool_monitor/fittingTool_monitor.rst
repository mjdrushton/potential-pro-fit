.. _fittingToolMonitor:
**********************
fittingTool_monitor.py
**********************

This tool provides is a web browser based monitor used to analyse the progress of :ref:`fittingTool.py <fittingTool>` runs. 

Usage
=====
::

	fittingTool_monitor.py [options] 

Run monitor from the same directory as fittingTool.py run and then open http://localhost:8080 in a web browser.

Options:
^^^^^^^^

``-h, --help``            	show help message and exit
``-p PORT, --port=PORT``    set the port on which fitting monitor runs (useful for running multiple instances of the monitor on the same machine). By default this is port 8080.