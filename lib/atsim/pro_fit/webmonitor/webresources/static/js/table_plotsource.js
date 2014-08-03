var OverviewTableSeries = function(model, plotObj, xaxis, yaxis)
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

	self._xcol = 'iteration_number';
	self._ycol = 'merit_value';
	self._runningMinCol = 'it:is_running_min';

	var _setCurrentData = function()
	{
		var data = self.model.overview_table;

		if (!data.enabled())
		{
			self.lineSeriesCurrent.setData([]);
			self.pointSeriesCurrent.setData([]);
			self.pointSeriesBest.setData([]);	
			return;
		}

		var all = [];
		var best = [];
		var all_minus_best = [];

		var xcolidx = $.inArray(self._xcol, data.columns());
		var ycolidx = $.inArray(self._ycol, data.columns());
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

		// Update the three PlotSeries with their data.
		self.lineSeriesCurrent.setData(all);
		self.pointSeriesCurrent.setData(all_minus_best);
		self.pointSeriesBest.setData(best);
	};

	// ... subscribe to the knockoutJS model and update when iteration numbers change
	model.overview_table.values.subscribe(_setCurrentData);
};