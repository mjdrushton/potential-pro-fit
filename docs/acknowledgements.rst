################
Acknowledgements
################


Potential Pro-Fit builds on the work of others and the author would like to acknowledge the following:

	* Python libraries on which the tool relies:
	  
	  - CherryPy provides the backend to the fitting monitor: http://cherrypy.org/
	  	+ CherryPy/SQLAlchemy integration uses work from webpyte: https://code.google.com/p/webpyte/

	  - Execnet. Underpins the remote runners: http://codespeak.net/execnet/
	  
	  - Inspyred library. Used for population based minimisers: http://inspyred.github.com/

	  - Mystic. Optimisation library, provides a number of minimizers:
	  
	  	+ M.M. McKerns, L. Strand, T. Sullivan, A. Fang and M.A.G. Aivazis, "Building a framework for predictive science", *Proceedings of the 10th Python in Science Conference*, 2011. http://arxiv.org/pdf/1202.1056

	  	+ Michael McKerns, Patrick Hung, and Michael Aivazis, "mystic: a simple model-independent inversion framework", 2009- ; http://dev.danse.us/trac/mystic

	  - SQLAlchemy is used to manage the SQLite database used to store fitting results: http://www.sqlalchemy.org/ and http://www.sqlite.org/

	  - The expression parser used by the calculated variables and formula meta-evaluator uses the ``exprtk`` C++ library by Arash Partow:  http://partow.net/programming/exprtk/
	 

	* The fitting monitor GUI relies on several javascript libraries:

	  - JQuery: http://jquery.com

	  	+ A number of JQuery plugins are also used:
	  		+ jQuery UI Layout: http://layout.jquery-dev.net/
	  		+ jQuery Mousewheel: https://github.com/brandonaaron/jquery-mousewheel
	  		+ jQuery Resize: http://benalman.com/code/projects/jquery-resize/docs/files/jquery-ba-resize-js.html
	  		+ Tooltipster provides tooltips: http://calebjacob.com/tooltipster/
	  		+ jQuery datatables provides the sortable and searchable tables: http://datatables.net

	  - D3.js is used as the basis of the plotting system: http://d3js.org

	  - Knockout : http://knockoutjs.com