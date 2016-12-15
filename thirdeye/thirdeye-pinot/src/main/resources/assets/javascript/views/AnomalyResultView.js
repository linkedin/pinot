function AnomalyResultView(anomalyResultModel) {

  // model
  this.anomalyResultModel = anomalyResultModel;
  this.metricSearchConfig = {
      theme : "bootstrap",
      placeholder : "Search for Metric",
      ajax : {
        url : '/data/autocomplete/metric',
        minimumInputLength : 3,
        delay : 250,
        allowClear: true,
        data : function(params) {
          var query = {
            name : params.term,
            page : params.page
          }
          // Query paramters will be ?name=[term]&page=[page]
          return query;
        },
        processResults : function(data) {
          var results = [];
          mode = $('#anomalies-search-mode').val();
            $.each(data, function(index, item) {
              results.push({
                id : item.id,
                text : item.alias
              });
            });
          return {
            results : results
          };
        }
      }
    };
  this.dashboardSearchConfig = {
      theme : "bootstrap",
      placeholder : "Search for Dashboard",
      ajax : {
        url : '/data/autocomplete/dashboard',
        minimumInputLength : 3,
        delay : 250,
        allowClear: true,
        data : function(params) {
          var query = {
            name : params.term,
            page : params.page
          }
          // Query paramters will be ?name=[term]&page=[page]
          return query;
        },
        processResults : function(data) {
          var results = [];
          mode = $('#anomalies-search-mode').val();
            $.each(data, function(index, item) {
              results.push({
                id : item.id,
                text : item.name
              });
            });
          return {
            results : results
          };
        }
      }
    };

  this.anomalySearchConfig = {
      theme : "bootstrap",
      placeholder : "Search for anomaly ID",
      tags: true
    };

  this.timeRangeConfig = {
    startDate : this.anomalyResultModel.startDate,
    endDate : this.anomalyResultModel.endDate,
    dateLimit : {
      days : 60
    },
    showDropdowns : true,
    showWeekNumbers : true,
    timePicker : true,
    timePickerIncrement : 60,
    timePicker12Hour : true,
    ranges : {
      'Last 24 Hours' : [ moment(), moment() ],
      'Yesterday' : [ moment().subtract(1, 'days'), moment().subtract(1, 'days') ],
      'Last 7 Days' : [ moment().subtract(6, 'days'), moment() ],
      'Last 30 Days' : [ moment().subtract(29, 'days'), moment() ],
      'This Month' : [ moment().startOf('month'), moment().endOf('month') ],
      'Last Month' : [ moment().subtract(1, 'month').startOf('month'), moment().subtract(1, 'month').endOf('month') ]
    },
    buttonClasses : [ 'btn', 'btn-sm' ],
    applyClass : 'btn-primary',
    cancelClass : 'btn-default'
  };

  this.spinner = new Spinner();

  // Compile HTML template
  var anomalies_template = $("#anomalies-template").html();
  this.anomalies_template_compiled = Handlebars.compile(anomalies_template);
  $("#anomalies-place-holder").html(this.anomalies_template_compiled);

  var anomaly_results_template = $("#anomaly-results-template").html();
  this.anomaly_results_template_compiled = Handlebars.compile(anomaly_results_template);

  // events
  // this.metricChangeEvent = new Event(this);
  // this.hideDataRangePickerEvent = new Event(this);
  this.applyButtonEvent = new Event(this);
  this.rootCauseAnalysisButtonClickEvent = new Event(this);
  this.showDetailsLinkClickEvent = new Event(this);
  this.anomalyFeedbackSelectEvent = new Event(this);

}

AnomalyResultView.prototype = {
  init : function() {
    $('#anomalies-search-mode').select2({
      minimumResultsForSearch : -1,
      theme : "bootstrap"
    }).on("change", function(e) {
      console.log('On change of search mode');
      console.log(e);
      self.showSearchBarBasedOnMode();
    });

    this.setupSearchBar();

    // TIME RANGE SELECTION
    this.timeRangeConfig.startDate = this.anomalyResultModel.startDate;
    this.timeRangeConfig.endDate = this.anomalyResultModel.endDate;
    function cb(start, end) {
      $('#anomalies-time-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh a'));
    }
    $('#anomalies-time-range').daterangepicker(this.timeRangeConfig, cb);
    cb(this.timeRangeConfig.startDate, this.timeRangeConfig.endDate);


    // APPLY BUTTON
    this.setupListenerOnApplyButton();
  },

  render : function() {

    var anomalies = this.anomalyResultModel.getAnomaliesList();

    var anomaly_results_template_compiled_with_results = this.anomaly_results_template_compiled(anomalies);
    $("#anomaly-results-place-holder").html(anomaly_results_template_compiled_with_results);
    this.renderAnomaliesTab(anomalies);
    self = this;

    this.showSearchBarBasedOnMode();



    // this.setupListenerOnDateRangePicker();

    // FUNCTION DROPDOWN
    var functions = this.anomalyResultModel.getAnomalyFunctions();
    var anomalyFunctionSelector = $('#anomaly-function-dropdown');
    $.each(functions, function(val, text) {
      anomalyFunctionSelector.append($('<option></option>').val(val).html(text));
    });

    this.spinner.stop();


  },
  showSearchBarBasedOnMode : function() {
    var mode = $('#anomalies-search-mode').val();
    $('#anomalies-search-dashboard-container').hide();
    $('#anomalies-search-anomaly-container').hide()
    $('#anomalies-search-metrics-container').hide();
    if (mode == 'metric') {
      console.log('showing metric');
      $('#anomalies-search-metrics-container').show();
    } else if (mode == 'dashboard') {
      console.log('showing dashboard');
      $('#anomalies-search-dashboard-container').show();
    } else if (mode == 'id') {
      $('#anomalies-search-anomaly-container').show()
    }
  },
  setupSearchBar : function() {
    $('#anomalies-search-metrics-input').select2(this.metricSearchConfig).on("select2:select", function(e) {
    });
    $('#anomalies-search-dashboard-input').select2(this.dashboardSearchConfig).on("select2:select", function(e) {
    });
    $('#anomalies-search-anomaly-input').select2(this.anomalySearchConfig).on("select2:select", function(e) {
    });

  },
  renderAnomaliesTab : function(anomalies) {
    for (var idx = 0; idx < anomalies.length; idx++) {
      var anomaly = anomalies[idx];
      console.log(anomaly);

      var currentRange = anomaly.currentStart + " - " + anomaly.currentEnd;
      var baselineRange = anomaly.baselineStart + " - " + anomaly.baselineEnd;
      $("#current-range-" + idx).html(currentRange);
      $("#baseline-range-" + idx).html(baselineRange);

      var date = [ 'date' ].concat(anomaly.dates);
      var currentValues = [ 'current' ].concat(anomaly.currentValues);
      var baselineValues = [ 'baseline' ].concat(anomaly.baselineValues);
      var chartColumns = [ date, currentValues, baselineValues ];

      var regionStart = moment(anomaly.anomalyRegionStart, constants.TIMESERIES_DATE_FORMAT).format(constants.DETAILS_DATE_FORMAT);
      var regionEnd = moment(anomaly.anomalyRegionEnd, constants.TIMESERIES_DATE_FORMAT).format(constants.DETAILS_DATE_FORMAT);
      $("#region-" + idx).html(regionStart + " - " + regionEnd)

      var current = anomaly.current;
      var baseline = anomaly.baseline;
      $("#current-value-" + idx).html(current);
      $("#baseline-value-" + idx).html(baseline);

      var dimension = anomaly.anomalyFunctionDimension;
      $("#dimension-" + idx).html(dimension)

      if (anomaly.anomalyFeedback) {
        $("#anomaly-feedback-" + idx + " select").val(anomaly.anomalyFeedback);
      }

      // CHART GENERATION
      var chart = c3.generate({
        bindto : '#anomaly-chart-' + idx,
        data : {
          x : 'date',
          xFormat : '%Y-%m-%d %H:%M',
          columns : chartColumns,
          type : 'spline'
        },
        legend : {
          show : false,
          position : 'top'
        },
        axis : {
          y : {
            show : true
          },
          x : {
            type : 'timeseries',
            show : true
          }
        },
        regions : [ {
          axis : 'x',
          start : anomaly.anomalyRegionStart,
          end : anomaly.anomalyRegionEnd,
          class: 'anomaly-region',
          tick : {
            format : '%m %d %Y'
          }
        } ]
      });

      this.setupListenersOnAnomaly(idx, anomaly);
    }

  },

  dataEventHandler : function(e) {
    var currentTargetId = e.currentTarget.id;
    if (currentTargetId.startsWith('root-cause-analysis-button-')) {
      this.rootCauseAnalysisButtonClickEvent.notify(e.data);
    } else if (currentTargetId.startsWith('show-details-')) {
      this.showDetailsLinkClickEvent.notify(e.data);
    } else if (currentTargetId.startsWith('anomaly-feedback-')) {
      var option = $("#" + currentTargetId + " option:selected").text();
      e.data['feedback'] = option;
      this.anomalyFeedbackSelectEvent.notify(e.data);
    }
  },
  setupListenerOnApplyButton : function() {
    var self = this;
    $('#apply-button').click(function() {
      var mode = $('#anomalies-search-mode').val();
      var metricIds = [];
      var dashboardId = null;
      var anomalyIds = [];
      if (mode == 'metric') {
        metricIds = $('#anomalies-search-metrics-input').val();
      } else if (mode = 'dashboard') {
        dashboardId = $('#anomalies-search-dashboard-input').val();
      } else if (mode = 'id') {
        anomalyIds = $('#anomalies-search-anomaly-input').val();
      }

      var functionName = $('#anomaly-function-dropdown').val();
      var startDate = $('#anomalies-time-range').data('daterangepicker').startDate;
      var endDate = $('#anomalies-time-range').data('daterangepicker').endDate;

      var anomaliesParams = {
        mode : mode,
        metricIds : metricIds,
        dashboardId : dashboardId,
        anomalyIds : anomalyIds,
        startDate : startDate,
        endDate : endDate,
        functionName : functionName
      }
      var target = document.getElementById('anomaly-results-place-holder');
      var opts = {
          lines: 13 // The number of lines to draw
        , length: 28 // The length of each line
        , width: 14 // The line thickness
        , radius: 42 // The radius of the inner circle
        , scale: 1 // Scales overall size of the spinner
        , corners: 1 // Corner roundness (0..1)
        , color: '#000' // #rgb or #rrggbb or array of colors
        , opacity: 0.25 // Opacity of the lines
        , rotate: 0 // The rotation offset
        , direction: 1 // 1: clockwise, -1: counterclockwise
        , speed: 1 // Rounds per second
        , trail: 60 // Afterglow percentage
        , fps: 20 // Frames per second when using setTimeout() as a fallback for CSS
        , zIndex: 2e9 // The z-index (defaults to 2000000000)
        , className: 'spinner' // The CSS class to assign to the spinner
        , top: '50%' // Top position relative to parent
        , left: '50%' // Left position relative to parent
        , shadow: false // Whether to render a shadow
        , hwaccel: false // Whether to use hardware acceleration
        , position: 'absolute' // Element positioning
        }
//      target.appendChild(self.spinner.spin().el);
      self.spinner = new Spinner(opts).spin(target);
      self.applyButtonEvent.notify(anomaliesParams);
    })
  },
  setupListenersOnAnomaly : function(idx, anomaly) {
    var rootCauseAnalysisParams = {
      metric : anomaly.metric,
      rangeStart : anomaly.currentStart,
      rangeEnd : anomaly.currentEnd,
      dimension : anomaly.anomalyFunctionDimension
    }
    $('#root-cause-analysis-button-' + idx).click(rootCauseAnalysisParams, this.dataEventHandler.bind(this));
    var showDetailsParams = {
      anomalyId : anomaly.anomalyId,
      metric : anomaly.metric,
      rangeStart : anomaly.currentStart,
      rangeEnd : anomaly.currentEnd,
      dimension : anomaly.anomalyFunctionDimension
    }
    $('#show-details-' + idx).click(showDetailsParams, this.dataEventHandler.bind(this));
    var anomalyFeedbackParams = {
      idx : idx,
      anomalyId : anomaly.anomalyId
    }
    $('#anomaly-feedback-' + idx).change(anomalyFeedbackParams, this.dataEventHandler.bind(this));
  }


};

