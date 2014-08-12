// Set-up some custom bindings

var formatNumber = function (numberString)
{
	if (numberString === null || numberString === '')
	{
		return "NaN";
	}
	var number = new Number(numberString);
	return number.toFixed(6);
};

ko.bindingHandlers.format_number = {
	update : function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext)
	{
		var number = ko.utils.unwrapObservable(valueAccessor());			
		number = formatNumber(number);
		$(element).text(number);
	} 
};

var _scrollRowIntoView = function(dataTable, rowId, animate)
/** Scrolls row of a particular dataTable into curent view port.
	@param dataTable dataTables instance
	@param rowId Index of row to be moved to top of view-port.
	@param animate If true scrolling to new row will be animated
*/
{
	//Get scroll row
	var scrollRow = dataTable.fnGetNodes(rowId);
	scrollRow = $(scrollRow);

	// Now get the element that can be  used to scroll this <tr> into view
	// Note: this uses a method provided by jquery-ui
	var scrollParent = scrollRow.scrollParent();

	// Calculate the distance we need to scroll by
	var scrollOffset = scrollRow.offset().top - scrollParent.offset().top;

	// Update the scroll...
	var scrollStart = scrollParent.scrollTop();
	var scrollEnd = scrollStart + scrollOffset;

	scrollParent.stop(jumpToEnd = true);

	if (!animate)
	{	
		scrollParent.scrollTop(scrollEnd);
	}
	else
	{
		scrollParent.animate({'scrollTop' : scrollEnd}, 100);
	}
};

// Binding to show tooltipster tooltip over element.
ko.bindingHandlers.tooltip = {
	init: function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext)
	{
		$(element).tooltipster();
	},

	update : function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext)
	{
		var contents = ko.utils.unwrapObservable(valueAccessor());
		$(element).tooltipster('update', contents);		
		if (contents == '' || contents == null)
		{
			$(element).tooltipster('disable')
		}
		else
		{
			$(element).tooltipster('enable');
		}
	}
};

// Binding to show tooltipster tooltip over datatable elements, 
//
// Binding syntax:
// { 
//	table_model : TABLE_MODEL,
//   tooltip_columns : TOOLTIP_COLUMNS  
// }
// 
// Where: 
//   table_model - object that has .columns and .values properties defining model for a table.
//   tooltip_columns - List of target_column data_column pairs as listed below.
//
// Column pairs:
// These pairs are objects with .target_column and .data_column properties.
//
// .target_column - gives sName for column whose cells lead to tooltip being displayed
// .data_column   - gives sName for column containing data for tooltip, displayed when .target_column is hovered.
// 
// i.e. on hovering over a target cell, the binding gets data for the hovered row from the data column.
//
//
ko.bindingHandlers.datatable_tooltip = {
	init: function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext)
	{
		// Return true if a tooltip should be created. When sName is in target pairs.
		var shouldHandle = function(sName, config)
		{
			var ttCols = ko.utils.unwrapObservable(config.tooltip_columns);
			var found = $.grep(ttCols, function(v,i){
				return sName == v.target_column;
			});
			return found.length > 0;
		};

		var toolTipContents = function(sName, rowIdx, config)
		{
			var ttCols = ko.utils.unwrapObservable(config.tooltip_columns);
			var found = $.grep(ttCols, function(v,i){
				return sName == v.target_column;
			});
			var dataSName = found[0].data_column;
			var tableModel = ko.utils.unwrapObservable(config.table_model);
			var columns = ko.utils.unwrapObservable(tableModel.columns);
			var values = ko.utils.unwrapObservable(tableModel.values);
			var colIdx = $.inArray(dataSName, columns);
			return values[rowIdx][colIdx];
		};

		var handlerIn = function(eventObject)
		{
			var dataTable = $(element).dataTable();

			// Which column are we hovering over?
			var whichCell = dataTable.fnGetPosition(this);
			var rowIdx = whichCell[0];
			var colIdx = whichCell[2];

			var sName = dataTable.fnSettings().aoColumns[colIdx].sName;

			var config = valueAccessor();
			if (shouldHandle(sName, config))
			{
				var ttContent = toolTipContents(sName, rowIdx, config);
				var ttElement = $(this);
				if (ttContent != null)
				{
					ttElement.tooltipster({
						content : ttContent,
						trigger : 'custom'
					});

					ttElement.tooltipster('show');
				}
			}
		};

		var handlerOut = function(eventObject)
		{
			$(this).tooltipster().tooltipster('destroy');
		};

		$(element).on('mouseenter', "tbody tr td", 
			handlerIn);
		$(element).on('mouseleave', "tbody tr td", 
			handlerOut);
	}
};


_makeColumnDefs = function(columns, aoColumns)
{
	var colDefs = [];

	if (aoColumns != null)
	{
		$.each(columns, function(i,v){
			var coldef = $.extend(true, {}, aoColumns[i]);
			coldef.sName = v;
			colDefs.push(coldef);
		});
	}
	else
	{
		$.each(columns, function(i,v){
			colDefs.push({"sTitle" : v, "sName" : v});
		});	
	}
	return colDefs;
};

_dtinit = function(element, allBindingsAccessor, columns, values, aoColumns){
		//Initializes table for datatable binding.
		console.log("_dtinit");
		var dtInitVars = {};
		if (allBindingsAccessor().dtconfig != null)
		{
			$.extend(dtInitVars, allBindingsAccessor().dtconfig);
		}

		var colDefs;
		if (aoColumns != null)
		{
			colDefs = aoColumns;
		}
		else
		{
			colDefs = _makeColumnDefs(columns, aoColumns);
		}

		dtInitVars.aaData = values;
		dtInitVars.aoColumns = colDefs;

		var dt = $(element).dataTable(dtInitVars);
		resizeTable($(element));
		return dt;
	};

_haveColumnsChanged = function(table, coldefs)
{
	var oldColDefs = $

	//Have columns changed
	var oldColDefs =[];
	$.each(table.fnSettings().aoColumns, function(i, v){
		oldColDefs.push(v.sName);
	});

	if (oldColDefs.length != coldefs.length)
	{
		return true;
	}

	$.each(oldColDefs, function(i,v){
		if (v !== coldefs[i].sName){
			return true;
		}
	});
	return false;
};


// Binding to link model.js Table (with columns and values properties) to datatable.net table.
ko.bindingHandlers.datatable = {
	init: function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext)
	{
		var dtInitVars = {};
		_dtinit(element, allBindingsAccessor, [''], [['']]);
		
		ko.computed({
			read : function() {
				var contents = valueAccessor();

				var columns = ko.utils.unwrapObservable(contents.columns);
				var values = ko.utils.unwrapObservable(contents.values);

				var aoColumns = null;
				if ('aoColumns' in contents)
				{
					aoColumns = ko.utils.unwrapObservable(contents.aoColumns);
				}

				if (columns.length == 0 || values.length == 0)
					return;

				var jqele = $(element);
				var table = jqele.dataTable();

				var coldefs = _makeColumnDefs(columns, aoColumns);
				var colsChanged = _haveColumnsChanged(table, coldefs);

				var scrollParent = jqele.scrollParent();
				var scrollTop = scrollParent.scrollTop();
				var scrollLeft = scrollParent.scrollLeft();

				if (!colsChanged)
				{
					console.log("Column definitions have not changed. Clearing table.");
					table.fnClearTable();
					table.fnAddData(values);
				}
				else
				{
					console.log("Columns have changed. Destroying table.");
					
					var aaSorting = null;
					if (table.fnSettings().aaSorting !== undefined)
					{
						aaSorting = table.fnSettings().aaSorting[0];

						// Translate sort columns into sName values
						if ($.isArray(aaSorting)){
							var colidx = aaSorting[0];
							aaSorting = [
								table.fnSettings().aoColumns[colidx].sName, aaSorting[1]];
						}
						else
						{
							aaSorting = null;
						}
					}

					table.fnDestroy();
					jqele.empty();
					table = _dtinit(jqele, allBindingsAccessor, columns, values, coldefs);			
					if (aaSorting)
					{
						// If column name still exists, set sort order.
						var sNames = [] 
						$.each(table.fnSettings().aoColumns, function(i,v){sNames.push(v.sName)});
						var colidx = $.inArray(aaSorting[0], sNames);
						if (colidx == -1)
						{
							table.fnSort([[0, "asc"]]);
						}
						else
						{
							table.fnSort([[colidx, aaSorting[1]]]);
						}
					}
				}

				//Restore scroll position.	
				scrollParent = table.scrollParent();
				scrollParent.scrollLeft(scrollLeft);
				scrollParent.scrollTop(scrollTop);
			},

			owner : this,
			disposeWhenNodeIsRemoved : element
		}).extend({'throttle' : 50 });
	
	}
};

// This binding is called through datatable_selection rather than directly.
// This is because knockout calls all bindings defined on a particular element whenever any of their dependencies changes
// meaning that the table was getting un-necessarily redrawn if a row was selected.
// datatable_selection creates a computed variable within its init function, isolating this binding from the other bindings on the element
// (although it might have made more sense to isolate the datable binding as this is more expensive to run). Ah well.
_datatable_selection = {
	init: function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext)
	{
			// Create row selection click handler			
			console.log('Enabling row selection for:'+element);
			var tableElement = $(element);
			$(tableElement).on('click', "tbody tr td", function(eventObject){
				// Get the row index
				if (!tableElement.hasClass('dataTable'))
				{
					return;
				}
				var table = tableElement.dataTable();
				var selected = table.fnGetPosition(this);

				// Update the model's row index.
				// selected contains [row index, column index (visible), column index (all)]
				selected = {'row' : selected[0],  'col' : selected[2]};

				valueAccessor()(selected);
			});
		},

		update : function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext)
		{
			var tableElement = $(element);
			if (!tableElement.hasClass('dataTable'))
			{
				return;
			}
			var table = tableElement.dataTable();

			// Display row selection.
			// First clear selection
			tableElement.find('tbody tr.selected_row').removeClass('selected_row');
			tableElement.find('tbody td.selected_cell').removeClass('selected_cell');

			// Now update the selection.
			var selected = valueAccessor()();
			if (selected !== null && selected !== undefined)
			{
				//Support multiple selections
				var selectedList;
				if( $.isArray(selected))
				{
					selectedList = selected;
				}
				else
				{
					selectedList = [selected];
				}

				var scrollFlag =  selectedList.length == 1;

				$.each(selectedList, function(i,selected){
					

					console.log("Selected row:"+selected.row);
					if (selected.row != null)
					{
						var tr = table.fnGetNodes(selected.row);
						if (tr != null)
						{
							tr = $(tr);
							tr.addClass('selected_row');

							if (selected.col != null)
							{
								tr.find('td:eq('+selected.col+')')
									.addClass('selected_cell');
							}

							if (scrollFlag)
							{
								_scrollRowIntoView(table, selected.row, true);
							}
						}
					}
				});
				
			}			
		}
};

// Binding for handling row selection in datatables
ko.bindingHandlers.datatable_selection = {
    init: function(element, valueAccessor, allBindingsAccessor) {
        var args = arguments;

        _datatable_selection.init.apply(this, args);

        ko.computed({
            read:  function() {
               // Look at datatable binding columns and values to make sure 
               // this binding is called when data changes (datable binding will redraw table
               // losing the selected row, so this needs to be called again to set the css classes again)
        	   ko.utils.unwrapObservable(allBindingsAccessor().datatable.columns);
        	   ko.utils.unwrapObservable(allBindingsAccessor().datatable.values);

               ko.utils.unwrapObservable(valueAccessor());
               _datatable_selection.update.apply(this, args);
            },
            owner: this,
            disposeWhenNodeIsRemoved: element
        }).extend({'throttle' : 100 });
    }        
};


// Binding to show radio buttons using jqueryui buttonset.
// Requires a binding called button_data: with format
//    [ {display : BUTTON_LABEL,
//       model   : MODEL_VALUE_WHEN_SELECTED},
//       ... ]
ko.bindingHandlers.buttonset = {
    init: function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
     	var button_data = allBindingsAccessor().button_data;
     	var modelValue = valueAccessor();

     	//Create button dom elements
     	var jqele = $(element);
     	jqele.uniqueId();
     	var radiogroup = jqele.attr('id');
     	$.each(button_data, function(i, bdat){
     		var label = bdat.display;
     		var model = bdat.model;
     		var iele = $("<input type='radio' name='"+radiogroup+"'>").appendTo(jqele);
     		iele.uniqueId();
 			var lele = $("<label for='"+iele.attr('id')+"'>").appendTo(jqele);
 			lele.append(label);

 			// Add event handle to the label.
 			lele.on('click', function(){
 				modelValue(model);
 			});
     	});

     	// Create the jquery buttons.
     	jqele.buttonset();
    },

	update : function(element, valueAccessor, allBindingsAccessor, viewModel, bindingContext) {
		// Get list of buttons
		var modelValue = ko.utils.unwrapObservable(valueAccessor());
		var button_data = allBindingsAccessor().button_data;
		var jqele = $(element);

		// Get the button list 
		var buttons = jqele.find('input[name="'+jqele.attr('id')+'"]');

		if (modelValue == null)
		{
			buttons.prop('checked', false);
		}
		else
		{
			// Find the selected index
			var bm = {};
			$.each(button_data, function(i,b){
				bm[b.model] = i;
			});

			var bidx = bm[modelValue];
			$(buttons[bidx]).prop('checked', true);
		}

		buttons.button('refresh');
	}
};


ko.bindingHandlers.tablehead = {
    init: function(element, valueAccessor, allBindings, viewModel, bindingContext) {
    	var getColumnIndex = function(e){
    		var dt = $(element).dataTable();
    		var s = dt.fnSettings();
    		var h = s.aoHeader;

    		var cells = [];
    		$.each(h[0], function(idx, item){
    			cells.push(item.cell);
    		});

    		return cells.indexOf(e);	
    	};

		var removeTT = function(ttElement)
    	{
    		ttElement.tooltipster('hide', 
    			function(){
    				ttElement.tooltipster('destroy');
    			});
    	};

		var handlerIn = function(eventObject)
		{
    		var idx = getColumnIndex(this);

    		// Get column name.
			var contents = valueAccessor();
			var columns = ko.utils.unwrapObservable(contents.columns);
			var colname = columns[idx];
    		
			var ttContent = $("<span></span>");
			var plotLink = $("<a href='#'>Plot this column</a>")
			var ttElement = $(this);
			plotLink.click(function(){
				viewModel.plot_model.y_column(colname);
				ttElement.tooltipster('hide');				
				return false;
			});
			ttContent.append(plotLink);

			var isPinned = contents.isPinnedColumn(colname);
			if (isPinned)
			{
				var unPinLink = $("<a href ='#'>Un-Pin</a>")
				unPinLink.css("margin-left", "5px");
				unPinLink.click(function(){
					contents.unPinColumn(colname);
					ttElement.tooltipster('hide');				
					return false;
				});


				ttContent.append(unPinLink);
			}


			ttElement.tooltipster({
					content : ttContent,
					offsetY : -10,
					// trigger : 'custom',
					functionAfter : function(){ttElement.tooltipster('destroy');},
					interactive : true
			});

			ttElement.tooltipster('show');
		};

		var handlerOut = function(eventObject)
		{
			$(this).tooltipster().tooltipster('destroy');
		};

    	// The element seems to get destroyed or unlinked from DOM
    	// registering even handlers to element or even it's .dataTable_wrapper
    	// parent using .on() doesn't work.
    	// Instead extract id attribute, and make a selector that can be used
    	// with .on() higher up the DOM tree (i.e. body node).
    	var selector = $(element).attr("id");
    	selector = "#"+selector+"_wrapper thead th";

    	$('body').on("mouseenter", selector, handlerIn);
    },

    update: function(element, valueAccessor, allBindings, viewModel, bindingContext) {
        // This will be called once when the binding is first applied to an element,
        // and again whenever any observables/computeds that are accessed change
        // Update the DOM element based on the supplied values here.
    }
};