var _DragOffsetInteraction = function(ele)
/** When instances of _DragOffsetInteraction are created,
  *	when ever the mouse is dragged over them they fire
  *	'dragOffset' events. Handlers for these events have the form 
  *
  *		function(event, offset){...}
  *	
  *	where event is jquery event object and offset has x and y
  *	properties giving the mouse offset since the last dragOffset event
  *	occurred.
  *
  *	@param ele D3 selection for which drag events should be created.
  */
{
	var self = this;
	self._dragstart = function(e, ui)
	{
		d3.event.sourceEvent.stopPropagation();
	};

	self._drag = function()
	{
		var offset = {x : d3.event.dx, y: d3.event.dy};
		$(self).trigger('dragOffset', offset);
	};

	drag = d3.behavior.drag()
				.on("dragstart", self._dragstart)
				.on("drag", self._drag);
	ele.call(drag);
};


var MouseInteractionRangeManager = function(plotObj, series, xlo, xhi, ylo, yhi)
/** When created becomes associated with a plot and provides mouse interaction. 
  * Services provided:
  * - Panning by mouse drag
  * - Zooming using mouse-wheel
  * 
  * @param plotObj Plot instance
  * @param series Series instance
  * @param xlo Default axis ranges (@see AutoRangeManager)
  * @param xhi Default axis ranges (@see AutoRangeManager)
  * @param ylo Default axis ranges (@see AutoRangeManager)
  * @param yhi Default axis ranges (@see AutoRangeManager)
  */
{
	var self = this;
	self._series = series;
	self._bg = plotObj.getPlotAreaBackground();
	self._plot = plotObj;
	self._xlo = xlo;
	self._xhi = xhi;
	self._ylo = ylo;
	self._yhi = yhi;
	self._zoomFactor = 0.05;
	self._arm = new AutoRangeManager(series, xlo, xhi, ylo, yhi);

	self._resetButton = self._plot.getPlotAreaElement()
		.append('image')
		.attr('xlink:href', 'resources/images/reset_icon.svg')
		.attr('width', 32)
		.attr('height', 32)
		.attr('x', 5)
		.attr('y', 5);

	
	self._calcNewAxisRange = function(scale, mouseOffset, zoomDelta)
	{
		var factor = zoomDelta < 0 ? 1+self._zoomFactor : 1-self._zoomFactor;
		var pdistToMax = scale.range()[1] - mouseOffset;
		var pdistToMin = mouseOffset - scale.range()[0];

		var newpmax = mouseOffset + (pdistToMax*factor);
		var newpmin = mouseOffset - (pdistToMin*factor);

		var newscale = scale.copy();
		newscale.range([newpmin, newpmax]);
		var lo = newscale.invert(mouseOffset - pdistToMin);
		var hi = newscale.invert(mouseOffset + pdistToMax);
		
		return [lo, hi];
	};

	self._plotAreaZoomHandler = function(e, delta, deltaX, deltaY)
	{
		var domains = self._calcAxisRangeFromMouseWheelEvent(e,delta);
		self._updateAxes(domains[0],domains[1]);
		
		//Prevent window scrolling 
		return false;
	};	

	self._calcAxisRangeFromMouseWheelEvent = function(e, delta)
	{
		var b = self._plot.getPlotAreaBox();
		var xdomain = series.getXAxis().getDomain();
		var ydomain = series.getYAxis().getDomain();

		var offs = $(e.target).offset();

		var mouseX = e.clientX - offs.left;
		var mouseY = e.clientY - offs.top;

		// What do mouseX and mouseY equate to in axis space?
		var xscale = series.getXAxis().getScale();
		var yscale = series.getYAxis().getScale();

		xdomain = self._calcNewAxisRange(xscale, mouseX, delta);
		ydomain = self._calcNewAxisRange(yscale, mouseY, delta);

		xdomain = self._applyBound(xdomain, self._xlo, self._xhi);
		ydomain = self._applyBound(ydomain, self._ylo, self._yhi);

		return [xdomain, ydomain];
	};

	self._axisZoomHandler = function(e, delta)
	{
		var axis = e.data.axis;
		var domains = self._calcAxisRangeFromMouseWheelEvent(e,delta); 
		var otherAxis;
		if ( axis.getSide() == 's' || axis.getSide() == 'n')
		{
			otherAxis = self._series.getYAxis();
			self._updateAxes(domains[0], otherAxis.getScale().domain());
		}
		else
		{
			otherAxis = self._series.getXAxis();
			self._updateAxes(otherAxis.getScale().domain(), domains[1]);
		}

		return false;
	};

	self._applyBound = function(range, lobound, hibound)
	{
		var lo = range[0];
		var hi = range[1];
		if (lobound != null && lo < lobound)
		{
			lo = lobound;
		}

		if (hibound !== null && hi > hibound)
		{
			hi = hibound;
		}
		return [lo,hi];
	};

	self._updateAxes = function(xdomain, ydomain)
	{
		
		self._arm.setRanges(xdomain[0], xdomain[1], ydomain[0], ydomain[1]);
	}

	self._translateRange = function(scale, offset)
	{
		var max = scale.invert(scale.range()[1] + offset);
		var min = scale.invert(scale.range()[0] + offset);
		return [min, max];
	};

	self._calcDragRangesFromEvent = function(e,offset)
	{
		var xscale = series.getXAxis().getScale();
		var yscale = series.getYAxis().getScale();

		var xdomain = self._translateRange(xscale, -offset.x);

		if ( (self._xlo != null && xdomain[0] < self._xlo) || (self._xhi != null && xdomain[1] > self._xhi))
		{
			// We've hit the edge use existing domain.
			xdomain = xscale.domain();
		}

		var ydomain = self._translateRange(yscale, -offset.y);
		if ( (self._ylo != null && ydomain[0] < self._ylo) || (self._yhi != null && ydomain[1] > self._yhi))
		{
			// We've hit the edge use existing domain.
			ydomain = yscale.domain();
		}

		return [xdomain, ydomain];
	};

	self._plotAreaDragHandler = function(e,offset)
	{
		var domains = self._calcDragRangesFromEvent(e,offset);
		self._updateAxes(domains[0], domains[1]);
	};


	self._axisDragHandler = function(e, offset)
	{
		var axis = e.data.axis;
		if (axis.getSide() == 'n' || axis.getSide() == 's')
		{
			offset.y = 0;
		}
		else
		{
			offset.x = 0;
		}
		
		var domains = self._calcDragRangesFromEvent(e,offset);
		self._updateAxes(domains[0], domains[1]);
	};

	self._resetHandler = function(e)
	{
		self._updateAxes([self._xlo, self._xhi], [self._ylo, self._yhi]);
	};

	// Register event handlers
	// ...plot area zoom
	$(self._bg[0]).mousewheel(self._plotAreaZoomHandler);
	// .. axis zoom
	$(self._series.getXAxis().getForegroundRectangle()[0]).bind('mousewheel', {'axis' : self._series.getXAxis()}, self._axisZoomHandler);
	$(self._series.getYAxis().getForegroundRectangle()[0]).bind('mousewheel', {'axis' : self._series.getYAxis()}, self._axisZoomHandler);
	// ... plot area drag
	self._plotAreaDragOffsetEventGenerator = new _DragOffsetInteraction(self._bg);
	$(self._plotAreaDragOffsetEventGenerator).on('dragOffset', self._plotAreaDragHandler);
	// ... axis drag
	self._xAxisPlotAreaDragOffsetEventGenerator = new _DragOffsetInteraction(self._series.getXAxis().getForegroundRectangle());
	$(self._xAxisPlotAreaDragOffsetEventGenerator).bind('dragOffset', {'axis' : self._series.getXAxis()}, self._axisDragHandler);

	self._yAxisPlotAreaDragOffsetEventGenerator = new _DragOffsetInteraction(self._series.getYAxis().getForegroundRectangle());
	$(self._yAxisPlotAreaDragOffsetEventGenerator).bind('dragOffset', {'axis' : self._series.getYAxis()}, self._axisDragHandler);

	// ...view reset
	$(self._resetButton[0]).on('click', self._resetHandler);
};

var AutoRangeManager = function(series, xlo, xhi, ylo, yhi)
/** 
 * Matches the ranges of the axes associated with a Series instance
 * to the range of the data it represents.
 * 
 * @param series Series instance.
 * @param xlo Low value for x-axis, if null then data minimum is used.
 * @param xhi High value for x-axis, if null then data maximum is used.
 * @param ylo Low value for y-axis, if null then data minimum is used.
 * @param yhi High value for y-axis, if null then data maximum is used.
 */
{
	var self = this;
	self._series = series;
	self._xlo = xlo;
	self._xhi = xhi;
	self._ylo = ylo;
	self._yhi = yhi;
	self._data_xrange = null;
	self._data_yrange = null;

	self.setRanges = function(xlo, xhi, ylo, yhi)
	 /** 
	   * Set axis ranges
	   * @param xlo Low value for x-axis, if null then data minimum is used.
 	   * @param xhi High value for x-axis, if null then data maximum is used.
 	   * @param ylo Low value for y-axis, if null then data minimum is used.
       * @param yhi High value for y-axis, if null then data maximum is used.
       */
	{
		self._xlo = xlo;
		self._xhi = xhi;
		self._ylo = ylo;
		self._yhi = yhi;
		self._update();
	};

	self.getRanges = function()
	/**
	  * Get axis ranges.
	  *
	  * @return Ranges [xlo, xhi, ylo, yhi]. See setRanges for definition of values.
	  */
	{
		return [self._xlo, self._xhi, self._ylo, self._yhi];
	};

	self._calcXRange = function()
	{
		var e = self._data_xrange;
		var lo = self._xlo == null ? e[0] : self._xlo;
		var hi = self._xhi == null ? e[1] : self._xhi;
		return [lo, hi];
	};

	self._calcYRange = function()
	{
		var e = self._data_yrange;
		var lo = self._ylo == null ? e[0] : self._ylo;
		var hi = self._yhi == null ? e[1] : self._yhi;
		return [lo, hi];
	};

	self._calcDataExtents = function()
	{
		self._data_xrange = d3.extent(self._series.getData(), function(d){return d[0];});
		self._data_yrange = d3.extent(self._series.getData(), function(d){return d[1];});
	};

	self._update = function()
	{
		self._calcDataExtents();
		var xrange = self._calcXRange();
		var yrange = self._calcYRange();

		self._series.getXAxis().setDomain(xrange);
		self._series.getYAxis().setDomain(yrange);
	};


	self.remove = function()
	/** De-register this auto-scale manager from the series/plot it manipulates.
	 */
	{
		$(self._series).unbind('dataUpdate',self._dataUpdateHandler);
		self._series = null;
	};

	// Register event handler for 'dataUpdate' event.
	self._dataUpdateHandler = function()
	{
		self._update();
	};
	$(self._series).on('dataUpdate', self._dataUpdateHandler);
	self._update();
};


var _createAccessor = function(axis, chosenidx)
{
	var idx;
	if (chosenidx != null)
	{
		idx = chosenidx;
	}
	else
	{
		idx = axis.isHorizontal() ? 0 : 1;
	}

	var accessor = function(d)
	{
		var v = d[idx];
		v = axis.getScale()(v)
		return v;
	};
	return accessor;
}; 

var LineSeries = function(parentPlot, xaxis, yaxis, className)
/** 
 * @param parentPlot Plot instance to which this series belongs
 * @param xaxis Axis instance used by this series for horizontal axis
 * @param yaxis Axis instance used by this series for vertical axis
 * @param className Optional CSS class assigned to the group containing series.
 */
{
	var self = this;

	self._parent = parentPlot;
	self._xaxis = null;
	self._yaxis = null;
	self._data = [];
	self._interpolationMethod = 'linear';
	self._className = className;

	// Create group for this series.
	self._group = self._parent.getPlotAreaElement()
		.append('svg:g')
		.attr('class', self._className ? 'series '+self._className : 'series');


	self.setData = function(data)
	/** 
	  * Set data and re-plot series.
	  *
	  * When called, fires dataUpdate event.
	  *
	  * @param data Data to be plotted as list of [x,y] pairs.
	  */
	{
		self._data = data;
		self._updateLine();
		$(self).trigger("dataUpdate");
	};

	self.getData = function()
	{
		return self._data;
	};

	self.setInterpolate = function(method)
	{
		self._interpolationMethod = method;
		self._updateLine();
	};

	self.getInterpolate = function()
	{
		return self._interpolationMethod;
	};

	self._updateLine = function()
	{
		var s = self._xaxis.getScale().domain();
		if (!(isFinite(s[0]) && isFinite(s[1])))
		{
			return;
		}
		
		s = self._yaxis.getScale().domain();
		if (!(isFinite(s[0]) && isFinite(s[1])))
		{
			return;
		}

		var xpos = _createAccessor(self._xaxis);
		var ypos = _createAccessor(self._yaxis);
		var lineGenerator = d3.svg.line()
			.x(xpos)
			.y(ypos)
			.interpolate(self.getInterpolate());

		var line = self._group.selectAll('path.line')
			.data([self._data])

		line.enter()
			.append('svg:path')
			.attr('class', 'line');

		line.attr('d', lineGenerator);
	};

	self._updatesizeHandler = function()
	{
		self._updateLine();
	};

	self._scaleChangeHandler = function(e, p)
	{
		self._updateLine();
	};

	self.getXAxis = function()
	{
		return self._xaxis;
	};

	self.getYAxis = function()
	{
		return self._yaxis;
	};

	self.setXAxis = function(axis)
	{
		if (self._xaxis)
		{
			$(self._xaxis).unbind('scaleChange', self._scaleChangeHandler);
		}
		self._xaxis = axis;
		$(self._xaxis).on('scaleChange', self._scaleChangeHandler);
	};

	self.setYAxis = function(axis)
	{
		if (self._yaxis)
		{
			$(self._yaxis).unbind('scaleChange', self._scaleChangeHandler);
		}
		self._yaxis = axis;
		$(self._yaxis).on('scaleChange', self._scaleChangeHandler);
	};

	self.setXAxis(xaxis);
	self.setYAxis(yaxis);
};

var D3SymbolPointGenerator = function(symbolName)
/**
 * Symbol generator to be used with PointSeries. 
 * Uses d3.svg.symbol to draw symbol for each data point.
 * 
 * @param symbolName Name of symbol to draw (see documentation d3.svg.symbol for more information)
 */
{
	var self = this;

	self._symbolName = symbolName;
	self.d3gen = d3.svg.symbol(symbolName);

	self.getSymbolName = function(){
		return self._symbolName
	};

	self.enter = function(selection, series)
	{
		selection.append('path')
			.attr('class', 'symbol')
			.attr('d', self.d3gen);
	};

	self.update = function(selection, series)
	{
		var xpos = _createAccessor(series.getXAxis());
		var ypos = _createAccessor(series.getYAxis());
		selection.attr('transform', function(d){
			return 'translate('+xpos(d)+','+ypos(d)+')';			
		});
	};

	self.exit = function(selection, series)
	{
		selection.exit()
			.remove();
	};
};

var ErrorBarPointGenerator = function(makeInner)
/** 
 * PointGenerator for use with PointSeries, accepts data as [x,y,min,max] lists
 * (one per data point). Each point is located at x along the x-axis and had following appearance:
 *  
 * ---   <--- max
 *  |
 *  |
 *  @    <--- y
 *  |
 *  |
 * ---   <--- min
 *
 * Optionally, data points of form [x,y, min, max, innermin, innermax] produce the following appearance:
 *
 * ---   <--- max
 *  |
 *  |
 *  |    <--- innermax
 *   
 *   
 *  @    <--- y
 *   
 *      
 *  |    <--- innermin
 *  |
 *  |
 * ---   <--- min
 * 
 * @param makeInner If false one line created for upper error bar and another for the lower. 
 *					If true then additional lines created y -> innermax and y -> innermin. Otherwise these regions are left blank.
 */	   
{
	var self = this;

	self.pointGenerator = new D3SymbolPointGenerator('circle');
	self._makeInner = makeInner;

	self.enter = function(selection, series)
	{

		//Create the drop lines
		selection.append('line')
			.attr('class', 'high_error_bar outer');

		selection.append('line')
			.attr('class', 'low_error_bar outer');

		
		if (self._makeInner)
		{
			// Create inner quartile lines
			selection.append('line')
				.attr('class', 'high_error_bar inner');

			selection.append('line')
				.attr('class', 'low_error_bar inner');
		}

		//Create the symbol
		self.pointGenerator.enter(selection,series);


	};

	self.update = function(selection, series)
	{
		var xpos = _createAccessor(series.getXAxis());
		var ypos = _createAccessor(series.getYAxis());
		
		var ypos2 = _createAccessor(series.getYAxis(), 2);
		var ypos3 = _createAccessor(series.getYAxis(), 3);

		var ypos4 = _createAccessor(series.getYAxis(), 4);
		var ypos5 = _createAccessor(series.getYAxis(), 5);


		var cp = function(d)
		{
			var m = ypos(d);
			var ly1, ly2, hy1, hy2;

			ly1 = ypos2(d) - m;
			hy2 = ypos3(d) - m;

			ly2 = d.length == 6 ? ypos4(d) - m : 0;
			hy1 = d.length == 6 ? ypos5(d) - m : 0;
			return {'ly1' : ly1, 'hy2' : hy2, 'ly2' : ly2, 'hy1' : hy1};
		};

		selection.select('line.high_error_bar.outer')
			.attr('x1', 0)
			.attr('y1', function(d){return cp(d).hy1;})
			.attr('x2', 0)
			.attr('y2', function(d){return cp(d).hy2;});

		selection.select('line.low_error_bar.outer')
			.attr('x1', 0)
			.attr('y1', function(d){return cp(d).ly1;})
			.attr('x2', 0)
			.attr('y2', function(d){return cp(d).ly2;});

		if (self._makeInner)
		{
			selection.select('line.high_error_bar.inner')
				.attr('x1', 0)
				.attr('y1', 0)
				.attr('x2', 0)
				.attr('y2', function(d){return cp(d).hy1;});

			selection.select('line.low_error_bar.inner')
				.attr('x1', 0)
				.attr('y1', 0)
				.attr('x2', 0)
				.attr('y2', function(d){return cp(d).ly2;});
		}

		self.pointGenerator.update(selection, series);

	};

	self.exit = function(selection, series)
	{
		selection.exit().remove();
	};
};

var PointSeries = function(parentPlot, xaxis, yaxis, className, pointGenerator)
/** 
 * @param parentPlot Plot instance to which this series belongs
 * @param xaxis Axis instance used by this series for horizontal axis
 * @param yaxis Axis instance used by this series for vertical axis
 * @param className Optional CSS class assigned to the group containing series.
 * @param pointGenerator PointGenerator object used to create, update and remove points.
 		By Default D3SymbolPointGenerator is used.
 */
{
	var self = this;

	self._parent = parentPlot;
	self._xaxis = null;
	self._yaxis = null;
	self._data = [];
	self._className = className;

	self._pointGenerator = pointGenerator ? pointGenerator : new D3SymbolPointGenerator('circle');

	// Create group for this series.
	self._group = self._parent.getPlotAreaElement()
		.append('svg:g')
		.attr('class', self._className ? 'series '+self._className : 'series');

	self.setData = function(data)
	/** 
	  * Set data and re-plot series.
	  *
	  * When called, fires dataUpdate event.
	  *
	  * @param data Data to be plotted as list of [x,y] pairs.
	  */
	{
		self._data = data;
		self._updatePoints();
		$(self).trigger("dataUpdate");
	};

	self.getData = function()
	{
		return self._data;
	};

	self._updatePoints = function()
	{
		var selection = self._group
			.selectAll('g.point')
			.data(self._data);

		var groupEnterSelection = selection.enter()
			.append('g')
			.attr('class', 'point');

		self._pointGenerator.enter(groupEnterSelection, self);
		self._pointGenerator.update(selection, self);
		self._pointGenerator.exit(selection, self);
	};	

	self._updatesizeHandler = function()
	{
		self._updatePoints();
	};

	self._scaleChangeHandler = function(e, p)
	{
		self._updatePoints();
	};

	self.getXAxis = function()
	{
		return self._xaxis;
	};

	self.getYAxis = function()
	{
		return self._yaxis;
	};

	self.setXAxis = function(axis)
	{
		if (self._xaxis)
		{
			$(self._xaxis).unbind('scaleChange', self._scaleChangeHandler);
		}
		self._xaxis = axis;
		$(self._xaxis).on('scaleChange', self._scaleChangeHandler);
	};

	self.setYAxis = function(axis)
	{
		if (self._yaxis)
		{
			$(self._yaxis).unbind('scaleChange', self._scaleChangeHandler);
		}
		self._yaxis = axis;
		$(self._yaxis).on('scaleChange', self._scaleChangeHandler);
	};

	self.setXAxis(xaxis);
	self.setYAxis(yaxis);
};

var Axis = function(parentPlot, side, domain)
/**
 *  @param parentPlot Plot instance to which this axis belongs
 *  @param side Determines which side axis belongs to one of (n,e,s,w)
 *  @param domain [min, max] array determining axis limits
 */
{
	var self = this;
	self._side = side.toLowerCase();
	self._parent = parentPlot;
	self._labelOffset = -3;
	self._labelText = '';
	self._domain = domain;
	self._scale = null;
	self._axisGenerator = null;
	self._group = self._parent._svg
		.append('svg:g');

	self._axisGroup = self._group
		.append('svg:g');

	self._label = self._group
		.append('svg:text')
		.attr('class', 'label');

	self._axisfg = self._group
		.append('svg:rect')
		.attr('class', 'axis_bg');

	// Create d3 axis instance
	self._updateScale = function(b)
	/** Creates linear scale used by axis.
		
		Event: this trigger 'scaleChange' event whose extra parameters (i.e. second argument to event handler)
		is dictionary of form:

			{
				oldscale : OLD_SCALE,
				newscale : NEW_SCALE
			}

		Where OLD_SCALE is d3.scale.linear instance as it was before current scale change ocurred.
			  and NEW_SCALE d3.scale.linear instance that will be used to redraw axis.

		@param b Plot area box (with w and h properties)*/
	{
		var v = [b.h, 0];
		var h = [0, b.w];

		// Choose correct range based on whether axis is horizontal or vertical
		var range = { 'n' : h, 's': h,
					  'e' : v, 'w': v}[self._side];
		var oldscale = self._scale;
		self._scale = d3.scale.linear()
			.domain(self._domain)
			.range(range);

		$(self).trigger('scaleChange', {'oldscale' : oldscale, 'newscale' : self._scale});
	};

	self._updateAxisGenerator = function()
	{
		self._axisGenerator = d3.svg.axis()
			.scale(self._scale)
			.orient({'n' : 'top',
					 'w' : 'left',
					 's' : 'bottom',
					 'e' : 'right'}[self._side]);
	};
	
	self._calculateAxisPosition = function(b)
	{
		return { 'n' :  [b.x, b.y],
				 'w' : [b.x, b.y],
				 's' : [b.x, b.y + b.h],
				 'e' : [b.x+b.w, b.y]}[self._side];
	};

	self._updateAxis = function()
	{
		var b = self._parent.getPlotAreaBox();
		self._updateScale(b);
		self._updateAxisGenerator();

		var p = self._calculateAxisPosition(b);

		self._axisGroup
			.attr('transform', 'translate('+p[0]+', '+p[1]+')')
			.call(self._axisGenerator);

	    self._updateLabel(b);
	    var bbox = self._axisGroup[0][0].getBBox();

	    var w,h,x,y;
		if (self._side == 'n' || self._side == 's')
		{
			w = b.w; 
			h = bbox.height;
			x = p[0];
			y = self._side == 'n' ? p[1] - h : p[1];
		}
		else
		{
			w = bbox.width; 
			h = b.h;
			x = self._side == 'w' ? p[0] - w : p[0];
			y = p[1];
		}
		
		self._axisfg
	   		.attr('x', x)
		 	.attr('y', y)
		 	.attr('width', w)
		 	.attr('height', h);
	};

	self._updateLabel = function(b)
	{
		if (! b)
		{
			b = self._parent.getPlotAreaBox();
		}

		var bbox = self._axisGroup[0][0].getBBox();
		var p = self._calculateAxisPosition(b);

		self._label
			.text(self._labelText)
			.attr('style', 'visibility : hidden;');

		if (self._side == 'w')
		{
			self._label.attr('transform', 'rotate(-90.0 )');		
		}
		else if (self._side == 'e')
		{
			self._label.attr('transform', 'rotate(90.0 )');		
		}
		else {
			self._label.attr('transform', null);
		}

		var tbb = self._label[0][0].getBBox();

		var labelPosition = {'n' : [ p[0]+ (b.w/2.0), p[1] - bbox.height +self._labelOffset],
							 'w' : [ p[0] - bbox.width + self._labelOffset, p[1] + b.h/2.0],
							 's' : [ p[0]+ (b.w/2.0), p[1] + tbb.height + bbox.height + self._labelOffset],
							 'e' : [ p[0] +  bbox.width + self._labelOffset, p[1] + b.h/2.0]}[self._side];

		self._label
			.attr('x', labelPosition[0])
			.attr('y', labelPosition[1])
			.attr('text-anchor', 'middle')
			.attr('style' , 'visibility : visible;');

		if (self._side == 'w')
		{
			self._label.attr('transform', 'rotate(-90.0 '+labelPosition[0]+' '+labelPosition[1]+')');		
		}
		else if (self._side == 'e')
		{
			self._label.attr('transform', 'rotate(90.0 '+labelPosition[0]+' '+labelPosition[1]+')');		
		}


	};

	self.updatesize =function()
	{
		self._updateAxis();
	};


	self.setLabel = function(labelText)
	/** Set axis label
	  * @param labelText Text for axis label
	  * @return this Axis instance
	  */
	{
		self._labelText = labelText;
		self._updateLabel();
		return self;
	};

	self.remove = function()
	/** Remove axis from plot. Note: following invocation this object should no longer be used.*/
	{
		self._group.remove();
		$(self._parent).unbind('updatesize', self._updatesizeHandler);
		self._parent = null;
	};

	self.getScale = function()
	{
		return self._scale;
	};

	self.getDomain = function()
	{
		return self._scale.domain();
	};

	self.setDomain = function(domain)
	{
		self._domain = domain;
		self._updateAxis();
	};

	self.getAxisGroup = function()
	/** 
	  * Return the d3 selection representing the SVG group containing 
	  * the axis.
	  * @return d3 selection for SVG axis group
	  */
	{
		return self._group;
	};

	self.getForegroundRectangle = function()
	/**
	  * Return the d3 selection representing SVG rect that is in front of everything, 
	  * this is useful for registerting event handlers
	  * @return SVG rect giving axis background.
	  */
	{
		return self._axisfg;
	};

	self.getSide = function()
	/**
	 * Return a string giving the side of a Plot's plot area that this axis
	 * is attached to.
	 *
	 * @return String one of n,w,s or e
	 */
	{
		return self._side;
	};

	self.isHorizontal = function()
	/** 
	 * @return true if the axis is attached to north or south side, false otherwise.
	 */
	{
		return self._side == 'n' || self._side == 's';
	};

	self.getOrientation = function()
	/** 
	 * @return 'vertical' or 'horizontal' based on axis orientation.
	 */
 	{
 		return self.isHorizontal() ? 'horizontal' : 'vertical';
	}


	self._updateAxis();

	// Add extra classes allowing styles to target axes based on side and orientation.
	self._group
		.attr('class', 'axis axis_'+self.getOrientation()+' axis_side_'+self.getSide());


	// Register for parent updatesize event.
	self._updatesizeHandler = function(){self.updatesize()};
	$(self._parent).on("updatesize",  self._updatesizeHandler);
};

var Plot = function(id)
/**
 *  @param id ID of DOM element  within which plot should be placed
 */
{
	var self = this;
	self._margins = { north : 20,
					east  : 20,
					south : 50,
					west  : 60};


	self._parentContainer = d3.select('#'+id);
	self._svg = self._parentContainer
		.append('svg:svg')
		.attr('class', 'plot');



	// Set-up plot area clipping path
	self._clipID = id+"_plot_area_clip";
	self._plotAreaClipRect = self._svg
		.append('svg:defs')
		.append('svg:clipPath')
		.attr('id', id+"_plot_area_clip")
		.append('rect')
		.attr('x', 0)
		.attr('y', 0);

	// Set-up the plot area
	self._plotArea = self._svg
		.append('svg:g')
		.attr('class', 'plot_area')
		.attr('clip-path', 'url(#'+self._clipID+')');

	self._plotAreaBackground = self._plotArea
		.append('svg:rect')
		.attr('x', 0)
		.attr('y', 0)
		.attr('class', 'plot_area_background');


	self._plotAreaFrame = self._svg
		.append('svg:rect')
		.attr('class', 'plot_area_frame')
		.attr('x', 0)
		.attr('y', 0);

	

	self.getPlotAreaElement = function()
	/** @return d3 element reperesenting plot area */
	{
		return self._svg.select('g.plot_area');
	};

	self.getPlotAreaBackground = function()
	/** @return d3 selection for rect providing plot-area background */
	{
		return self._plotAreaBackground;
	};

	self._calculateSVGBox = function()
	{
		var svgEle = $(self._svg[0]);
		return { w : svgEle.width(),
				 h : svgEle.height()};
	};

	self.getPlotAreaBox = function()
	{
		var svgBox = self._calculateSVGBox();
		var outdict =  { x : self._margins.west,
				 y : self._margins.north,
				 w : svgBox.w - (self._margins.west + self._margins.east),
				 h : svgBox.h - (self._margins.north + self._margins.south)};

		if (outdict.w < 1) outdict.w = 1;
		if (outdict.h < 1) outdict.h = 1;
		if (outdict.x > outdict.w) outdict.x = outdict.w;
		if (outdict.y > outdict.h) outdict.y = outdict.h;
		return outdict;
	};

	self._updatePlotArea = function()
	{
		var box = self.getPlotAreaBox();
		var trans = 'translate('+box.x+', '+box.y+')';
		var setBox = function(d)
		{
			d.attr('width', box.w)
			.attr('height', box.h);
		};

		self._plotArea.attr('transform', trans).call(setBox);
		self._plotAreaFrame.attr('transform', trans).call(setBox);

		self._plotAreaBackground.call(setBox);
		self._plotAreaClipRect.call(setBox);
	};

	self._updateSVGSize = function()
	{
		var w = $(self._parentContainer[0]).width();
		var h = $(self._parentContainer[0]).height();

		$(self._svg[0]).width(w);
		$(self._svg[0]).height(h);
	};

	self.updatesize = function()
	{
		self._updateSVGSize();
		self._updatePlotArea();
		$(self).trigger("updatesize");
	};
	

	self.updatesize();

	$(self._parentContainer[0]).resize(function()
	{
		self.updatesize();
	});
};

var createDataFormatter = function(xkey, ykey) 
{
	var f = function(data)
	{
		var xi, yi;
		$.each(data['columns'], function(i, v){
			if (v == xkey)
			{
				xi = i;
			}

			if (v == ykey)
			{
				yi = i;
			}
		});

		var outdat = [];

		$.each(data['values'], function(i, row){
			outdat.push([row[xi], row[yi]]);
		});

		return outdat;
	};
	return f;
};
