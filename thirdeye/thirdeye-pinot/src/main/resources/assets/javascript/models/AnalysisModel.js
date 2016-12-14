function AnalysisModel() {
  this.metric;
  this.metricId;
  this.timeRange;
  this.granularity;
  this.dimensions;
  this.filters;

  this.currentStart = moment().subtract(4, 'days');;
  this.currentEnd = moment();
  this.baselineStart= moment().subtract(10, 'days');
  this.baselineEnd = moment().subtract(6, 'days');
}

AnalysisModel.prototype = {
  init: function (params) {
  },

  update: function (params) {
    console.log("Logging hash params");
    console.log(params);
    if (params.metricId) {
      this.metricId = params.metricId;
    }
    if (params.timeRange) {
      this.timeRange = params.timeRange;
    }
    if (params.granularity) {
      this.granularity = params.granularity;
    }
    if (params.dimensions) {
      this.dimensions = params.dimensions;
    }
    if (params.filters) {
      this.filters = params.filters;
    }
    if (params.currentStart) {
      this.currentStart = params.currentStart;
    }
    if (params.currentEnd) {
      this.currentEnd = params.currentEnd;
    }
    if (params.baselineStart) {
      this.baselineStart = params.baselineStart;
    }
    if (params.baselineEnd) {
      this.baselineEnd = params.baselineEnd;
    }
  }
}
