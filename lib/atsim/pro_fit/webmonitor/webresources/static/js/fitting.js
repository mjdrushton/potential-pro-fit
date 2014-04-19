
var model;

$(document).ready(function(){
    model = new ViewModel();

	// Enable jquery-ui-layout plugin
	// and set-up event handlers 
	_setLayout();

	ko.applyBindings(model);
	console.log("Applied bindings");

	
	//Get model to check for updates every two seconds.
	setInterval(function(){
		$.getJSON("/fitting/current_iteration", function(data){
			var iteration = data.current_iteration;
				model.current_iteration(iteration);
		});

		// Get run_status
		$.getJSON("/fitting/run_status", function(data){
			model.run_status(data.runstatus);
			model.title(data.title);
		});
	}, 2000);


	// Set-up plot
	setupPlot(model);
});

var resizeTable = function(tableElement)
{
	tableElement = $(tableElement);
	if (! tableElement.hasClass('dataTable'))
	{
		return;
	}

	//Get the element containing the table
	var e = $(tableElement).parents('.dataTables_wrapper').parent();
	var ep = e.offsetParent();
	var wrapper = e.find('.dataTables_wrapper');
	var scrollbody = e.find('.dataTables_scrollBody');

	// Set the container height
	e.height(
		ep.height() - e.position().top - (e.outerHeight(true)- e.height()));

	//Resize the datable.
	var table = tableElement.dataTable();
	var extraHeight = wrapper.outerHeight(true) - scrollbody.outerHeight(true);
	if (e.children('.dataTables_info'))
	{
		extraHeight = extraHeight + e.find('.dataTables_info').outerHeight(true);
	}
	var newheight =  e.height() - extraHeight;
	var settings = table.fnSettings();
	settings.oScroll.sY = newheight+'px';
	
	//Preserve scroll position on redraw
	var scrollParent = table.scrollParent();
	var scrollTop = scrollParent.scrollTop();
	var scrollLeft = scrollParent.scrollLeft();

	table.fnDraw();

	//Restore scroll position.	
	scrollParent = table.scrollParent();
	scrollParent.scrollLeft(scrollLeft);
	scrollParent.scrollTop(scrollTop);	

};

var _setLayout = function()
{
	// Main splitter panes
	$('body').layout({ 
		applyDefaultStyles: true,
		east__size : 600
	});

	// Nested iteration detail
	$('#data_pane').layout({
		applyDefaultStyles: true 
	});

	// Make sure table resizes correctly
	$('#data_pane .ui-layout-center').resize(function(eobj){
		resizeTable($('#overview_table'));
		resizeTable($('#variables_table'));
		resizeTable($('#evaluators_table'));
		resizeTabElement($('#variables_cfg_tab'));
	});

	var resizeTabElement = function(element)
	{
		var jqele = $(element);
		var op = jqele.offsetParent();
		var jqele_extraheight = jqele.outerHeight(true) - jqele.height();
		var fillHeight = op.height() - jqele.position().top - jqele_extraheight;
		jqele.height(fillHeight);
	};

	
	// Enable tabs for candidate detail pane
	$('#candidate_tabs').tabs({
		heightStyle: "fill"
	});

	// Resize tables on tab change
	$('#data_pane').on('tabsactivate', function(eobj, ui){
		if (ui.newPanel.attr('id') == 'variables_tab')
		{
			resizeTable($('#variables_table'));
		}
		else if (ui.newPanel.attr('id') == 'evaluator_tab')
		{
			resizeTable($('#evaluators_table'));
		}
		resizeTabElement($('#variables_cfg_tab'));
	});
};

var _fetchData = function(url, dataFormatCallback, callBack)
{
	var f= function()
	{
		$.getJSON(url, function(data){
			var formattedData = dataFormatCallback(data);
			callBack(formattedData);
		});
	};
	return f;
};



var _columnDataToDict = function(data)
/** Convert data returned by /fitting/graph/series style JSON calls into
  * list of dicts, where dictionary keys are column labels.
  *
  * data has form:
  * { 'columns' : [label1, label2, ..., labeln],
  *   'values' : [
  * 		[label1_value_1, label2_value_1, ... , labeln_value_1],
  * 		...
  * 		[label1_value_n, label2_value_n, ... , labeln_value_n] ]
  * }
  *
  * @param data Data of form given above
  * @return list of dictionaries
  */
{
	var outrows = []
	var labels = data.columns;
	$.each(data.values, function(i,v){
		var row = {};
		$.each(labels, function(j,l){
			row[l] = v[j];
		});
		outrows.push(row);
	});
	return outrows;
};

var StandardAutoSeries = function(model, plotObj, xaxis, yaxis)
{
	var self = this;

	self.model = model;
	self.plotObj = plotObj;
	self.xaxis = xaxis;
	self.yaxis = yaxis;

	// Set-up series and series updating
	// .. series showing data for every iteration
	self.lineSeriesCurrent = new LineSeries(self.plotObj, self.xaxis, self.yaxis, 'line_series_current');
	self.lineSeriesCurrent.setInterpolate('step-after');
	// ... set-up mouse interaction based on values from this series
	self.mouser = new MouseInteractionRangeManager(self.plotObj, self.lineSeriesCurrent, 0,null, 0,null);

	// ..series that shows points for iterations where the 'best-ever' merit-value improves
	var pointGenerator = new InteractionPointGenerator(self.model, plotObj);
	self.pointSeriesCurrent = new PointSeries(self.plotObj, self.xaxis, self.yaxis, 'series_current', pointGenerator);
	self.pointSeriesBest = new PointSeries(self.plotObj, self.xaxis, self.yaxis, 'series_best', pointGenerator);

	self.dataFormatter = createDataFormatter('iteration_number', 'merit_value');

	self._setCurrentData = function(data)
	{
		// once we have fetched current data, split into two series,
		// one containing all the data (for the line series)
		// the other having the best merit value point filtered out.
		var allData = data;
		var bestData = self.pointSeriesBest.getData();
		var bestIterations = {}; 
		$.each(bestData, function(i, v){
			bestIterations[v[0]] = true;
		});

		var filteredCurrent = [];
		$.each(allData, function(i, v){
			if (! bestIterations[v[0]])
			{
				filteredCurrent.push(v)
			}
		});

		self.lineSeriesCurrent.setData(allData);
		self.pointSeriesCurrent.setData(filteredCurrent);
	};

	// ... JSON callbacks that update the two series we just created. 
	self.updateBestSeries = _fetchData('/fitting/iteration_series/merit_value/running_min/min', 
		self.dataFormatter,
		self.pointSeriesBest.setData);

	self._JSONUpdateCurrentSeries = _fetchData('/fitting/iteration_series/merit_value/all/min', 
		self.dataFormatter,
		self._setCurrentData);

	self._JSONUpdateCurrentSeries();
	self.updateBestSeries();

	// ... subscribe to the knockoutJS model and update when iteration numbers change
	self.model.current_iteration.subscribe(self._JSONUpdateCurrentSeries);
	self.model.best_iteration_summary.iteration_number.subscribe(self.updateBestSeries);
};


var PopulationAutoSeries = function(model, plotObj, xaxis, yaxis)
{
	var self = this;

	self.model = model;
	self.plotObj = plotObj;

	self.xaxis = xaxis;
	self.yaxis = yaxis;

	var pointGen = new ErrorBarPointGenerator(true);
	pointGen.pointGenerator.d3gen = svg_symbolBar(12);

	self.series = new PointSeries(
		self.plotObj,
		self.xaxis, self.yaxis,
		'series_population', 
		// null);
		pointGen);

	// self.standardSeries = new StandardAutoSeries(self.model, self.plotObj, self.xaxis, self.yaxis);
	
	// ... set-up mouse interaction based on values from this series
	self.mouser = new MouseInteractionRangeManager(self.plotObj, self.series, 0,null, 0,null);

	self._formatErrorBarData = function(data)
	{
		var errorBarData = [];
		$.each(_columnDataToDict(data), function(i,row){
			var ebrow = [row.iteration_number,
						 row['merit_value:quartile2'],
						 row['merit_value:min'],
						 row['merit_value:max'],
						 row['merit_value:quartile1'],
						 row['merit_value:quartile3']];
			errorBarData.push(ebrow);
		});
		return errorBarData;
	};

	self._updateSeries = function(errorBarData)
	{
		self.series.setData(errorBarData);
	};

	self.update = function()
	{
		// Get quartiles
		quartileFetch = _fetchData('graph/series/merit_value?stat=quartiles',
			self._formatErrorBarData,
			self._updateSeries);

		// Perform update.
		quartileFetch();
	};

	//TODO: Subscribe for iteration updates

	self.update();
};


var Key = function(plotObj, keyRecords)
/** @param plotObj Plot object to which key should be attached.
  * @param keyRecords Array of objects defining class should have
  *                   properties, 'class' and 'text' that will set 
  *                   CSS class name and key text respectively
  */
{
	var self = this;
	self._plotObj = plotObj;
	self._parent = plotObj.getPlotAreaElement();
	self._pointGenerator = d3.svg.symbol('circle');
	self._gap = 5;
	self._margin = 10;

	self._makeStructure = function()
	{
		self._keyBox = self._parent.append('g');
		self._keyBox.attr('class', 'key');

		self._contents = self._keyBox.append('g')
			.attr('class', 'key_contents');

		self._contents.append('text')
			.attr('class', 'title')
			.text('Legend:');
	};

	self._makeRecord = function(record)
	{
		var item = self._contents.append('g')
			.attr('class', 'legend_item');

		item.append('g')
			.attr('class', 'symbol_group series '+record.class)
			.append('path')
			.attr('class', 'symbol')
			.attr('d', self._pointGenerator);

		item.append('text')
			.attr('class', 'label')
			.text(record.text);
	};

	self._makeRecords = function(records)
	{
		$.each(records, function(i, r){
			self._makeRecord(r);
		});
	};

	self._dolayout = function()
	{
		self._contents.selectAll('.legend_item')
			.each(function(){
				horizontalLayout(d3.select(this), self._gap);
				verticalAlign('m', d3.select(this));
			});

		topToBottomLayout(self._contents, self._gap);

		// Make the margin around the box.
		var w,h;

		w = parseFloat(self._contents.attr('width'))+ self._margin;
		h = parseFloat(self._contents.attr('height'))+ self._margin;
		
		self._keyBox
			.attr('width', w)
			.attr('height', h);

		// Move into corner
		self._keyBox.attr('transform', null);
		snapTo('ne', self._keyBox,
			   'ne', self._parent);

		var trans = self._contents.attr('transform');
		trans = d3.transform(trans);
		trans.translate[0] -= self._margin;
		trans.translate[1] += self._margin;
		self._contents.attr('transform',trans);
	};

	self._makeStructure();
	self._makeRecords(keyRecords);
	self._dolayout()

	$(self._plotObj).on('updatesize', self._dolayout);
};

var InteractionPointGenerator = function(model, plotObj, symbolName)
/** This point generator is used with the point series to support
  *	interaction with the points. At the moment this behaviour is as
	follows:
		* On mouse over, the point is highlighted and grows.
		* On mouse out, the point shrinks back to its original size

*/
{
	var self = this;
	self._plotObj = plotObj;
	self._d3gen = new D3SymbolPointGenerator(symbolName);
	self._model = model;
	
	self.enter = function(selection, series)	
	{
		var newParent = selection.append('g');
		self._d3gen.enter(newParent, series);
		selection.on('mouseover', function(){
			var transSelect = d3.select(this).select('g');
			var startTrans = d3.transform(transSelect.attr('transform'));
			var endTrans = d3.transform(startTrans);
			endTrans.scale = [3,3];

			var data = d3.select(this).datum();
			var coord = d3.transform(d3.select(this).attr('transform')).translate;

			drawCallout(self._plotObj.getPlotAreaElement(), coord[0],coord[1], 12,
				function(content)
				{
					content.append('text')	
						.text("Iteration: "+ data[0]);
					content.append('text')
						.text("Merit: "+formatNumber(data[1]));
					topToBottomLayout(content, 3);
				});

			//Highlight the point immediately on hover.
			transSelect.select('.symbol')
				.classed('highlight', true);

			//Increase size of point
			transSelect
				.transition()
				.duration(100)
				.attrTween('transform', function(d,i,a){
					return d3.interpolateTransform(startTrans, endTrans);
				});
		});

		//Open a new data explorer when the point is clicked on
		selection.on('click', function(){
			var data = d3.select(this).datum();
			var iteration = data[0];
			self._model.overview_table.selected_iteration(iteration);
		});

		selection.on('mouseout', function(){
			var transSelect = d3.select(this).select('g');
			var startTrans = d3.transform(transSelect.attr('transform'));
			var endTrans = d3.transform(startTrans);
			endTrans.scale = [1,1];

			self._plotObj.getPlotAreaElement().selectAll('g.callout')
				.remove();

			// Unhighlight the point.
			transSelect.select('.symbol')
				.classed('highlight', false);

			//Shrink back to original size
			transSelect
				.transition()
				.duration(50)
				.attrTween('transform', function(d,i,a){
					return d3.interpolateTransform(startTrans, endTrans);
				});
		});
	};

	self.update = function(selection, series)
	{
		self._d3gen.update(selection, series);
	};

	self.exit = function(selection, series)
	{
		self._d3gen.exit(selection, series);
	};
};


var setupPlot = function(model)
{
	var plotObj = new Plot('plot');
	var xaxis = new Axis(plotObj, 's', [0,10]);
	xaxis.setLabel('Iteration');
	var yaxis = new Axis(plotObj, 'w', [0,10]);
	yaxis.setLabel('Merit Value');

	var series = new StandardAutoSeries(model, plotObj, xaxis, yaxis);
	var key = new Key(plotObj,
		[
			{class : 'series_current', text : 'Merit'},
			{class : 'series_best',  text : 'Improved merit value'}
		]);
};


svg_symbolBar = function(size)
{
	var halfSize = size / 2.0;
	function f()
	{
		return 'M'+(-halfSize)+',0L'+halfSize+',0';
	};
	return f;
};
