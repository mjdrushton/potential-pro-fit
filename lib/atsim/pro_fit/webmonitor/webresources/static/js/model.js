var _tableRowToDict = function(columns, row)
/** Associate column names with value in a row to create a dict
  * @param columns Column labels (keys for returned dictionary)
  * @param row Parallel array to columns containing dictionary values
  * @return Dictionary associating columns with row values
  */
{
	var outdict = {};
	$.each(columns, function(i, col){
		outdict[col] = row[i];
	});
	return outdict;
};

function Evaluated(iteration_number, candidate_number)
{
	var self = this;


	var _errorRender = function ( data, type, full ) 
	{
		if (type === 'display')
		{
			if (data != null)
			{
				return "<span class='flagged'></span>";
			}
			return "";
		}
		return data;
	};	

	self.iteration_number = iteration_number;
	self.candidate_number = candidate_number;

	self.columns = ko.observableArray([
		'error_message',
		'job_name',
		'evaluator_name',
		'value_name',
		'expected_value',
		'extracted_value',
		'percent_difference',
		'weight',
		'merit_value']);
	self.values = ko.observableArray();
   

   self.aoColumns = ko.observable([
     {sTitle : '<span class="head_icon"></span>', mRender : _errorRender, sClass : 'error_message' },
     {sTitle : 'Job'},
     {sTitle : 'Evaluator'},
     {sTitle : 'Value Name'},
     {sTitle : 'Expected'},
     {sTitle : 'Extracted', sClass : 'pinnable'},
     {sTitle : 'Δ%', sClass : 'pinnable'},
     {sTitle : 'Weight'},
     {sTitle : 'Merit', sClass : 'pinnable'}
   	]);

	self.table_selection = ko.observable({col : null, row :null});
	self._table_selection = ko.observable({});

	//Hash of column names allowed to be pinned, keys are column names, values give the VALUE_TYPE portion of column label for JSON call.
	self._selectionCols = { 'extracted_value' : 'extract',
							'merit_value' : 'merit',
							'percent_difference' : 'percent'};

	self.table_selection = ko.computed({
		read : function(){
			// Transform selection back into {row : ..., col: ...} form. 
			// also filter any selections that might have become invalid due to table changes
			var rows = self.values();
			var cols = self.columns();

			var new_selection = {};
			var retval = [];
			var tselect = self._table_selection();
			for (var k in tselect)
			{
				var kobj = tselect[k];
				var rownum = self._findRowNumFromEvaluatorAndValueName(kobj.job, kobj.evaluator_name, kobj.value_name);
				var colnum = $.inArray(kobj.col_name, cols);
				colnum = colnum != -1 ? colnum : null;

				if (rownum != null && colnum != null)
				{
					new_selection[k] = kobj;
					retval.push({row : rownum, col : colnum});
				}
			}
			self._table_selection(new_selection);
			return retval;
		},

		write : function(v){
			//Write takes single {row : ..., col: ...} dict
			//read returns a list
			var kobj = self._makeKey(v.row, v.col);
			var tselect = self._table_selection(); 
			if (kobj != null)
			{
				// If item is in tselect already, remove it
				if (kobj.key in tselect)
				{
					delete tselect[kobj.key];
				}
				else
				{
					// Only allow selection of allowed columns...
					if (kobj.col_name in self._selectionCols)
					{
						tselect[kobj.key] = kobj;
					}
				}
			}
			self._table_selection(tselect);
		},
		owner : self
	});


	// Used to pin extra evaluator columns to the overview_table. 
	// returns a list of 'evaluator:JOB_NAME:EVALUATOR_NAME:VALUE_NAME:VALUE_TYPE' keys as supported by
	// the /fitting/iteration_series JSON call. 
	self.selected_column_keys = ko.computed(function(){
		var tselect = self._table_selection();
		var retlist = [];
		$.each(tselect, function(i,v){
			retlist.push(v);
		});

		$.each(retlist, function(i,v){
			retlist[i] = 'evaluator:'+v.job+":"+v.evaluator_name+":"+v.value_name+":"+self._selectionCols[v.col_name];
		});

		retlist.sort();
		return retlist;
	});

	self._makeKey = function(rownum, colnum)
	{
		var row = self.values()[rownum];
		var col = self.columns()[colnum];

		if (row === undefined || col === undefined)
		{
			return null;
		}

		var rd = _tableRowToDict(self.columns(), row);

		return {key : rd.job_name+":"+rd.evaluator_name+":"+rd.value_name+":"+col, 
			col_name: col, 
			job : rd.job_name,
			evaluator_name : rd.evaluator_name, 
			value_name : rd.value_name};
	}

	self._findRowNumFromEvaluatorAndValueName = function(jobName, evaluatorName, valueName)
	// Return row index in self.values based on value of 'variable_name' being equal to evaluatorName parameter
	{
		var rows = self.values();
		var cols = self.columns();
		var foundIdx = null;

		$.each(rows, function(i, row)
		{
			var rowdict = _tableRowToDict(cols, row);
			if (jobName == rowdict.job, evaluatorName == rowdict.evaluator_name && valueName == rowdict.value_name)
			{
				foundIdx = i;
				return false;
			}
		});
		return foundIdx;
	}

	self._performUpdate = function(iterationNumber, candidateNumber)
	{
		var it = iterationNumber;
		var cn = candidateNumber;
		$.getJSON("/fitting/evaluated/"+it+"/"+cn, function(data)
		{
			console.log("Updating evaluator records for iteration_number="+it+" and candidate_number="+cn);
			var newValues = [];
			$.each(data, function(i, row){
				var newrow = [];
				$.each(self.columns(), function(j, col){
					var v;
					v = row[col];
					newrow.push(v);
				});
				newValues.push(newrow);	
			});
			self.values(newValues);	
		});
	};

	ko.computed(function(){
		var itNum = ko.utils.unwrapObservable(self.iteration_number);
		var cnNum = ko.utils.unwrapObservable(self.candidate_number);

		if (itNum != null && cnNum != null)
		{
			self._performUpdate(itNum, cnNum);
		}
	}, self).extend({'throttle' : 100});

};

function Variables(iteration_number, candidate_number)
{
	var self = this;

	self.iteration_number = iteration_number;
	self.candidate_number = candidate_number;

	self.onlyShowFittingVariables = ko.observable(true);
	
	// mRender callback that creates span with class of 'flagged' for cells containing values evaluating to true. If false don't return anything.
	var _flagRender = function ( data, type, full ) 
		{
			if (type === 'display')
			{
				if (data)
				{
					return "<span class='flagged'></span>";
				}
				return "";
			}
			return data;
		};


	self.columns = ko.observableArray([
		'variable_name',
		'value',
		'low_bound',
		'upper_bound',
		'fit_flag',
		'calculation_expression',
		'calculated_flag']);

	self.aoColumns = ko.observableArray([
			{sTitle : 'Name'},
			{sTitle : 'Value', sClass : 'pinnable'},
			{sTitle : 'Low Constraint'},
			{sTitle : 'Upper Constraint'},
			{sTitle : 'Fit Flag?', mRender : _flagRender, sClass : 'fit_flag'},
			{bVisible : false}, // calculation_expression
			{sTitle : '<span class="head_icon"></span>', mRender : _flagRender, sClass : 'calculated_flag'}
			]);


	self.values = ko.observableArray();

	self.table_selection = ko.observable({col : null, row :null});
	self._table_selection = ko.observable({});

	//Hash of column names allowed to be pinned.
	self._selectionCols = { 'value' : null};

	self.table_selection = ko.computed({
		read : function(){
			// Transform selection back into {row : ..., col: ...} form. 
			// also filter any selections that might have become invalid due to table changes
			var rows = self.values();
			var cols = self.columns();

			var new_selection = {};
			var retval = [];
			var tselect = self._table_selection();
			for (var k in tselect)
			{
				var kobj = tselect[k];
				var rownum = self._findRowNumFromVariableName(kobj.variable_name);
				var colnum = $.inArray(kobj.col_name, cols);
				colnum = colnum != -1 ? colnum : null;

				if (rownum != null && colnum != null)
				{
					new_selection[k] = kobj;
					retval.push({row : rownum, col : colnum});
				}
			}
			self._table_selection(new_selection);
			return retval;
		},

		write : function(v){
			//Write takes single {row : ..., col: ...} dict
			//read returns a list
			var kobj = self._makeKey(v.row, v.col);
			var tselect = self._table_selection(); 
			if (kobj != null)
			{
				// If item is in tselect already, remove it
				if (kobj.key in tselect)
				{
					delete tselect[kobj.key];
				}
				else
				{
					// Only allow selection of allowed columns...
					if (kobj.col_name in self._selectionCols)
					{
						tselect[kobj.key] = kobj;
					}
				}
			}
			self._table_selection(tselect);
		},
		owner : self
	});


	// Used to pin extra variable columns to the overview_table. 
	// returns a list of 'variable:VARIABLE_NAME' keys as supported by
	// the /fitting/iteration_series JSON call. 
	self.selected_column_keys = ko.computed(function(){
		var tselect = self._table_selection();
		var retlist = [];
		$.each(tselect, function(i,v){
			retlist.push(v);
		});

		$.each(retlist, function(i,v){
			retlist[i] = 'variable:'+v.variable_name;
		});

		retlist.sort();
		return retlist;
	});

	self._makeKey = function(rownum, colnum)
	{
		var row = self.values()[rownum];
		var col = self.columns()[colnum];

		if (row === undefined || col === undefined)
		{
			return null;
		}

		var rowdict = _tableRowToDict(self.columns(), row);
		var var_name = rowdict['variable_name'];

		return {key : var_name+":"+col, col_name: col, variable_name: var_name, row_num : rownum};
	}

	self._findRowNumFromVariableName = function(variableName)
	// Return row index in self.values based on value of 'variable_name' being equal to variableName parameter
	{
		var rows = self.values();
		var cols = self.columns();
		var foundIdx = null;

		$.each(rows, function(i, row)
		{
			var rowdict = _tableRowToDict(cols, row);
			if (variableName == rowdict['variable_name'])
			{
				foundIdx = i;
				return false;
			}
		});
		return foundIdx;
	}


	self.fitCfgVariables = ko.computed(function(){
		//.. create the fit.cfg Variables text
		var lines = ["[Variables]"];
		var calculatedVariables = [];
		$.each(self.values(), function(idx, row){
			var v = _tableRowToDict(self.columns(), row);
			if (!v.calculated_flag)
			{
				var linetokens = [v.variable_name, ":", v.value];

				//Process bounds
				var lowbound = (v.low_bound == "-Infinity") ? null : v.low_bound;
				var upperbound = (v.upper_bound == "Infinity") ? null : v.upper_bound;

				if (!(lowbound ==null && upperbound == null))
				{
					lowbound = (lowbound == null) ? "" : lowbound;
					upperbound = (upperbound == null) ? "" : upperbound;
					//extend line tokens with bounds
					linetokens.push.apply(linetokens, ["("+lowbound, ",", upperbound+")"]);
				}

				//Process fit_flag
				if (v.fit_flag)
				{
					linetokens.push("*");
				}
				var line = linetokens.join(" ");
				lines.push(line);
			}
			else
			{
				calculatedVariables.push(
					[v.variable_name, ":", v.calculation_expression].join(" "));
			}
		});

		if (calculatedVariables.length > 0)
		{
			lines.push("");
			lines.push("[CalculatedVariables]");
			$.each(calculatedVariables, function(idx,v){
				lines.push(v);
			});
		}

		lines = lines.join("\n");

		return lines;
	});

	self._performUpdate = function(iterationNumber, candidateNumber)
	{
		var it = iterationNumber;
		var cn = candidateNumber;	
		$.getJSON("/fitting/variables/"+it+"/"+cn, function(data)
		{
			console.log("Updating variables for iteration_number="+it+" and candidate_number="+cn);
			var newResults = [];
			$.each(data, function(i, jsonRow){
				var newRow = [];
				$.each(self.columns(), function(j, colName){
					newRow.push(jsonRow[colName]);
				});
				newResults.push(newRow);
			});
			self.values(newResults);
		});
	};

	ko.computed(function(){
		var itNum = ko.utils.unwrapObservable(self.iteration_number);
		var cnNum = ko.utils.unwrapObservable(self.candidate_number);

		if (itNum != null && cnNum != null)
		{
			self._performUpdate(itNum, cnNum);
		}
	}, self).extend({'throttle' : 100});
};

function CandidateSummary()
{
	var self = this;
	self.id = ko.observable();
	self. iteration_number = ko.observable();
	self.candidate_number = ko.observable();

	self.merit_value = ko.observable();
	self.merit_value_display = ko.computed(function(){
		var mv = self.merit_value();
		if (mv)
		{
			if (mv > 1.0)
			{
				return mv.toFixed(4);
			}
			else
			{
				return mv.toPrecision(4);				
			}
		}
		else
		{
			return '';
		}
	});

	self.variables = new Variables(self.iteration_number, self.candidate_number);
	self.evaluated = new Evaluated(self.iteration_number, self.candidate_number);

	self.update = function(data)
	{
		self.id(data.id);
		self.iteration_number(data.iteration_number);
		self.candidate_number(data.candidate_number);
		self.merit_value(data.merit_value);
	};
};

// Knockoutjs model for presenting iteration summary.
function IterationSummary(iteration_number){
	var self = this;
	self.iterationNumber = null;
	self.title = ko.observable(null);
	self.iteration_number = iteration_number;
	self.num_candidates = ko.observable(null);
	
	self.standard_deviation = ko.observable(null);
	self.mean = ko.observable(null);

	self.minimum = new CandidateSummary();
	self.maximum = new CandidateSummary();

	self.isPopulation = ko.computed(function(){
		var flag = self.num_candidates() > 1;
		return flag;
	});


	self._performUpdate = function(itNum)
	{
		$.getJSON("/fitting/iteration_overview/"+itNum, function(data){
			console.log("IterationSummary performing update");
			self.num_candidates(data.num_candidates);
			self.mean(data.mean);
			self.standard_deviation(data.standard_deviation);
			self.minimum.update(data.minimum);
			self.maximum.update(data.maximum);
		});
	};

	// Automatically update model when iteration_number is updated
	ko.computed(function(){
		var itNum = ko.utils.unwrapObservable(self.iteration_number);
		if(itNum != null)
		{	
			self._performUpdate(itNum);
		}
	}, self);
};

function OverViewTable(current_iteration, best_iteration) {
	var self = this;
	
	// These two properties are set by the ViewModel class
	// after instantiation as the are obtained from 
	// Variables and Evaluated instances elswhere in the model.
	// To prevent premature evaluation of computed values relying on these,
	// the enabled property is set to false until the object is ready.
	self.variable_columns = null;
	self.evaluator_columns = null;

	self.enabled = ko.observable(false);

	self.url = ko.computed(function(){
		var extracolumns = ['it:is_running_min'];
		// Add pinned variable columns to extracolumns
		$.merge(extracolumns, self.variable_columns());

		//...add pinned evaluator columns
		$.merge(extracolumns, self.evaluator_columns());

		// Build URL.
		var colStr = extracolumns.join();
		return "/fitting/iteration_series/merit_value/all/min?columns="+colStr;
	}, this, { deferEvaluation: true });
	self.columns = ko.observableArray(null);
	self.values = ko.observableArray(null);

	// Limit observer updates to 5 times a second.
	self.values.extend({rateLimit : 200});

	self._staticColumnDefinitions = {
		iteration_number : {sTitle : 'Iteration'},
		candidate_number : {sTitle : 'Candidate'},
		merit_value : {sTitle : 'Merit'},
		'it:is_running_min' : {bVisible : false}
	};

	self.aoColumns = ko.computed(function(){
		var cols = ko.utils.unwrapObservable(self.columns);
		if (cols == null)
		{
			return null;
		}

		var retlist = [];
		$.each(cols, function(i, col){
			if (col in self._staticColumnDefinitions)
			{
				retlist.push(self._staticColumnDefinitions[col]);
			}
			else if (col.match(/^variable:/))
			{
				var colTitle = col.replace(/^variable:/, "V: ")
				retlist.push({sTitle : colTitle, sClass : 'variable_column'});
			}
			else if (col.match(/^evaluator:/))
			{
				var match = /^evaluator:(.*?):(.*):(.*):(.*)$/.exec(col);

				var jobName = match[1];
				var evalName = match[2];
				var valueName = match[3];
				var valueType = match[4];
				valueType = valueType == 'percent' ? '∆%': valueType;

				var colTitle;
				if (jobName == 'meta_evaluator')
				{
					colTitle = "ME: "+valueName+" (<em>"+evalName+","+valueType+"</em>)";
				}
				else
				{
					colTitle = "E: "+valueName+" (<em>"+evalName+","+valueType+"</em>)";
				}

				retlist.push({sTitle : colTitle, sClass : 'evaluator_column'});
			}
			else
			{
				retlist.push({sTitle : col});
			}
		});

		return retlist;
	});

	self._selected = ko.observable({col : null, row : null});
	self.selected = ko.computed({
		read : function() {
			var selected = self._selected();
			return selected;
		},

		write : function(selected) {
			self._setSelected(selected);
			self.selection_follow_mode('manual');
		},
		owner : self
	});

	self._setSelected = function(selected)
	{
		// Check that selected row lies within value bounds, if not set null.
		var selected_row = selected.row;

		var setval = null;
		var len = self.values().length;
		if (selected_row != null && len > 0 && selected_row >= 0 && selected_row < len)
		{
			setval = selected_row;
		}

		self._selected({col : null, row : setval});
	};

	self.selected_iteration = ko.computed({
		read : function() {
			var rowNum = self.selected().row;
			if (rowNum != null)
			{
				return self._getIterationForRow(rowNum);
			}
			else
			{
				return null;
			}
		},

		write : function(selected_iteration) {
			var rowNum = self._getRowForIteration(selected_iteration);
			self._setSelected({col : null, row : rowNum});		
		},
		owner : self
	});

	self.current_iteration = current_iteration;
	self.best_iteration = best_iteration;

	// Allowed modes:
	// * 'manual' - selected_row will not change.
	// * 'auto-best' - When best_iteration changes, selected_row will update to current best iteration.
	// * 'auto-current' - When iteration changes, selected_row  will update to the current_iteration.
	self.selection_follow_mode = ko.observable('auto-best');

	self._performUpdate = function(){
		$.getJSON(self.url(), function(data){
			self.columns(data.columns);
			self.values(data.values);
		});

	};

	self._getRowForIteration = function(itNum)
	/* Return the row index for given iteration number. Or return null if not found */
	{
		var columns = self.columns();
		var values = self.values();

		var found = null;
		$.each(values, function(rowNum,v)
		{
			var rowdict = _tableRowToDict(columns, v);
			if (rowdict.iteration_number == itNum)
			{
				found = rowNum;
				return;
			}
		});
		return found;
	};

	self._getIterationForRow = function(rowNum)
	{
		var columns = self.columns();
		var values = self.values();
		var row = values[rowNum];

		if (!row)
		{
			return null;
		}

		row = _tableRowToDict(columns, row);
		return row.iteration_number;
	};

	// Update data when iteration changes. Or selected columns change.
	ko.computed(function(){
		if (! self.enabled())
		{
			return;
		}
		var itNum = ko.utils.unwrapObservable(self.current_iteration);

		// Make sure this ko.computed has dependency on url.
		self.url();

		if (itNum != null)
		{
			self._performUpdate();
		}
	}, self);

	// Perform selection auto-update
	ko.computed(function(){
		var mode = self.selection_follow_mode();
		var best = self.best_iteration();
		var curr = self.current_iteration();
		self.columns();
		self.values();

		if (mode == 'manual')
		{
			return;
		}
		else if (mode == 'auto-current')
		{
			self.selected_iteration(curr);
		}
		else if (mode == 'auto-best')
		{
			self.selected_iteration(best);
		}
		else
		{
			throw new Exception('Bad mode: '+mode);
		}
	}, self).extend({'throttle' : 1000 });
};

function PlotModel(srctable)
{
	var self = this;
	self._srctable = srctable;

	self.x_column = 'iteration_number';

	self._runningMinCol = 'it:is_running_min';
	self._y_default_column =  'merit_value';
	self._y_column = ko.observable(self._y_default_column);

	self._isValidColumn = function(column_label)
	{
		var cols = self._srctable.columns();
		return(cols.indexOf(column_label) != -1);
	};

	self.y_column = ko.computed({
		read: function(){
			if (!self._isValidColumn(self._y_column()))
			{
				self._y_column(self._y_default_column);
			}
			return self._y_column();
		},

		write: function(column_label)
		{
			if (! self._isValidColumn(column_label))
			{
				self._y_column(self._y_default_column);
			}
			else
			{
				self._y_column(column_label);
			}
		}
	});

	self.y_title = ko.computed(function(){
		var aocols = self._srctable.aoColumns();
		var cols = self._srctable.columns();
		var ycol = self.y_column();
		if (! self._srctable.enabled())
		{
			return "";
		}
		var colidx = cols.indexOf(ycol);
		if (colidx == -1)
		{		
			return "";
		}
		return aocols[colidx].sTitle;
	});

	self._all_data       = ko.observableArray();
	self._best_data      = ko.observableArray();
	self._all_minus_best = ko.observableArray();

	self.all_data = ko.computed(function(){
		return self._all_data();
	});

	self.best_data = ko.computed(function(){
		return self._best_data();
	});

	self.all_minus_best = ko.computed(function(){
		return self._all_minus_best();
	});

	// Computed variable that keeps the, all, best and all_minus_best data 
	// in sync with overview_table.
	// all, best and all_minus_best are formatted as [[x0,y0], [x1,y1], [x2,y2], ..., [xn,yn]]
	// for use with the plotting framework contained in plot.js
	ko.computed(function(){
		var data = self._srctable;
		var xcol = ko.utils.unwrapObservable(self.x_column);
		var ycol = ko.utils.unwrapObservable(self.y_column);
		data.values();

		if (!data.enabled())
		{
			self._all_data.removeAll();
			self._best_data.removeAll();
			self._all_minus_best.removeAll();
			return;
		}

		var all = [];
		var best = [];
		var all_minus_best = [];

		var xcolidx = $.inArray(xcol, data.columns());
		var ycolidx = $.inArray(ycol, data.columns());
		var rminidx = $.inArray(self._runningMinCol, data.columns());

		var x,y, isRunningMin, dp;
		$.each(data.values(), function(rowidx,row){
			x = row[xcolidx];
			y = row[ycolidx];
			isRunningMin  = row[rminidx];
			dp = [x,y];
			all.push(dp);

			if (isRunningMin)
			{
				best.push(dp);
			}
			else
			{
				all_minus_best.push(dp);
			}
		});

		// Update the series
		self._all_data(all);       
		self._best_data(best);      
		self._all_minus_best(all_minus_best); 
	});
};

// Root knockoutjs model for GUI
function ViewModel() {

	var self = this;

	self.current_iteration = ko.observable(null);
	self.best_iteration = ko.observable(null);

	self.run_status = ko.observable(null);
	self.title = ko.observable(null);

	self.overview_table = new OverViewTable(self.current_iteration, self.best_iteration);
	self.selected_iteration_summary = new IterationSummary(self.overview_table.selected_iteration);
	
	// These properties provide extra column keys to support dynamic addition of columns using values
	// from the variables and evaluators table.
	self.overview_table.variable_columns = self.selected_iteration_summary.minimum.variables.selected_column_keys;
	self.overview_table.evaluator_columns = self.selected_iteration_summary.minimum.evaluated.selected_column_keys;
	self.overview_table.enabled(true);

	self.current_iteration_summary = new IterationSummary(self.current_iteration);
	self.current_iteration_summary.title('Current');

	self.best_iteration_summary = new IterationSummary(self.best_iteration);
	self.best_iteration_summary.title('Best');

	self.plot_model = new PlotModel(self.overview_table);

	// Update best_iteration when current_iteration changes
	ko.computed(function(){
		var itNum = self.current_iteration();
		if (itNum != null)
		{
			$.getJSON("/fitting/best_candidate", function(data){
				self.best_iteration(data.iteration_number);				
			});
		}
	}, self);
};
