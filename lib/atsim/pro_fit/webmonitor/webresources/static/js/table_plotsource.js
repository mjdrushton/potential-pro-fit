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

	// Subscribe to data updates.
	ko.computed(function(){
		var plot_model = self.model.plot_model;
		var all = plot_model.all_data();
		var best = plot_model.best_data();
		var all_minus_best = plot_model.all_minus_best();

		self.lineSeriesCurrent.setData(all);
		self.pointSeriesCurrent.setData(all_minus_best);
		self.pointSeriesBest.setData(best);
	});

};