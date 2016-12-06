function DashboardView(dashboardModel) {
  this.dashboardModel = dashboardModel;
  this.tabClickEvent = new Event(this);
}

DashboardView.prototype = {
  init: function () {
    this.result_dashboard_template_compiled = dashboard_template_compiled({});
    $("#dashboard-place-holder").html(this.result_dashboard_template_compiled);

    $('#dashboard-tabs a').click(function (e) {
      e.preventDefault();
      $(this).tab('show');
    });

    var self = this;
    var tabSelectionEventHandler = function (e) {
      var targetTab = $(e.target).attr('href');
      // var previousTab = $(e.relatedTarget).attr('href');
      console.log("target tab:" + targetTab);
      // console.log("previous tab:" + previousTab);
      self.tabClickEvent.notify(targetTab);
    };
    $('#dashboard-tabs a[data-toggle="tab"]').on('shown.bs.tab', tabSelectionEventHandler);
    $('#dashboard-tabs a:first').click();
  },

  render: function () {
    renderDashboardTab();
  }

};

function renderDashboardTab() {

  // DASHBOARD SELECTION
  var countries = [ {
    value : 'Andorra',
    data : 'AD'
  }, {
    value : 'Zimbabwe',
    data : 'ZZ'
  } ];
  $('#dashboard-input').autocomplete({
    lookup : countries,
    onSelect : function(suggestion) {
      alert('You selected: ' + suggestion.value + ', ' + suggestion.data);
    }
  });

  // TIME RANGE SELECTION
  var start = moment().subtract(1, 'days');
  var end = moment();

  function dashboard_range_cb(start, end) {
    $('#dashboard-time-range span').addClass("time-range").html(start.format('MMM D, ') + start.format('hh:mm a') + '  &mdash;  ' + end.format('MMM D, ') + end.format('hh:mm a'));
  }

  $('#dashboard-time-range').daterangepicker({
    startDate : start,
    endDate : end,
    dateLimit : {
      days : 60
    },
    showDropdowns : true,
    showWeekNumbers : true,
    timePicker : true,
    timePickerIncrement : 5,
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
  }, dashboard_range_cb);

  dashboard_range_cb(start, end);
}
