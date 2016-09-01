 anomaliesDisplayData = "";
 timeseriesDisplayData = "";

function getAnomalies(tab) {

    //Creating request url
    var baselineStart = moment(parseInt(hash.currentStart)).add(-7, 'days')
    var baselineEnd = moment(parseInt(hash.currentEnd)).add(-7, 'days')
    var aggTimeGranularity = (window.datasetConfig.dataGranularity) ? window.datasetConfig.dataGranularity : "HOURS";
    var dataset = hash.dataset;
    var compareMode = "WoW";
    var currentStart = hash.currentStart;
    var currentEnd = hash.currentEnd;
    var metrics = hash.metrics;

    var timeSeriesUrl = "/dashboard/data/tabular?dataset=" + dataset + "&compareMode=" + compareMode //
        + "&currentStart=" + currentStart + "&currentEnd=" + currentEnd  //
        + "&baselineStart=" + baselineStart + "&baselineEnd=" + baselineEnd   //
        + "&aggTimeGranularity=" + aggTimeGranularity + "&metrics=" + metrics;

    var currentStartISO = moment(parseInt(currentStart)).toISOString();
    var currentEndISO = moment(parseInt(currentEnd)).toISOString();

    var urlParams = "dataset=" + hash.dataset + "&startTimeIso=" + currentStartISO + "&endTimeIso=" + currentEndISO + "&metric=" + hash.metrics;
        urlParams += hash.filter ? "&filters=" + hash.filters : "";
        urlParams += hash.hasOwnProperty("anomalyFunctionId")  ?   "&id=" + hash.anomalyFunctionId : "";

    var anomaliesUrl = "/dashboard/anomalies/view?" + urlParams;

    //AJAX for data
    getData(anomaliesUrl).done(function (anomalyData) {
        anomaliesDisplayData = anomalyData;
        getData(timeSeriesUrl).done(function (timeSeriesData) {
            timeseriesDisplayData = timeSeriesData;
            //Error handling when data is falsy (empty, undefined or null)
            if (!timeSeriesData) {
                $("#" + tab + "-chart-area-error").empty();
                var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' });
                warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: metric timeseries data =' + timeSeriesData  }));
                $("#" + tab + "-chart-area-error").append(warning);
                $("#" + tab + "-chart-area-error").show();
                return
            } else {
                $("#" + tab + "-chart-area-error").hide();
                $("#" + tab + "-display-chart-section").empty();
            }
            var placeholder= "linechart-placeholder"
            renderAnomalyLineChart(timeSeriesData, anomalyData, tab, placeholder);
            renderAnomalyTable(anomalyData, tab);

            //anomalyFunctionId is only present in hash when anomaly
            // function run adhoc was requested on self service tab
            //needs to be removed to be able to view other functions in later queries on the anomalies view
            delete hash.anomalyFunctionId
        });
    });
}

function renderAnomalyLineChart(timeSeriesData, anomalyData, tab, placeholder) {

    //$("#" + tab + "-display-chart-section").empty();
    /* Handelbars template for time series legend */
    var result_metric_time_series_section = HandleBarsTemplates.template_metric_time_series_section(timeSeriesData);
    $("#" + tab + "-display-chart-section").append(result_metric_time_series_section);
    drawAnomalyTimeSeries(timeSeriesData, anomalyData, tab, placeholder);
}

var anomalyLineChart;
function drawAnomalyTimeSeries(timeSeriesData, anomalyData, tab, placeholder) {

    var currentView = $("#" + tab + "-display-chart-section");

    //Unbind previous eventListeners
    currentView.off("click")
    currentView.off("change")

    var aggTimeGranularity = (window.datasetConfig.dataGranularity) ? window.datasetConfig.dataGranularity : "HOURS";
    var dateTimeFormat = "%I:%M %p";
    if (aggTimeGranularity == "DAYS" ) {
        dateTimeFormat = "%m-%d";
    }else if(timeSeriesData.summary.baselineEnd - timeSeriesData.summary.baselineStart > 86400000 ){
        dateTimeFormat = "%m-%d %I %p";
    }

    var lineChartPlaceholder = $("#"+ placeholder, currentView)[0];
    // Metric(s)
    var metrics = timeSeriesData["metrics"];
    var lineChartData = {};
    var xTicksBaseline = [];
    var xTicksCurrent = [];
    var colors = {};
    var chartTypes = {};
    var axes = {};
    var regions = [];

    for (var t = 0, len = timeSeriesData["timeBuckets"].length; t < len; t++) {
        var timeBucket = timeSeriesData["timeBuckets"][t]["currentStart"];
        var currentEnd = timeSeriesData["timeBuckets"][t]["currentEnd"];
        xTicksBaseline.push(timeBucket);
        xTicksCurrent.push(timeBucket);
    }

    console.log("anomalyData");
    console.log(anomalyData);
    for (var i = 0; i < anomalyData.length; i++) {
        var anomaly = anomalyData[i];

        var anomalyStart = anomaly.startTime;
        var anomalyEnd = anomaly.endTime;
        var anomayID = "anomaly-id-" + anomaly.id;
        regions.push({'axis': 'x', 'start': anomalyStart, 'end': anomalyEnd, 'class': 'regionX ' + anomayID });
    }
    lineChartData["time"] = xTicksCurrent;

    var colorArray;
    if (metrics.length < 10) {
        colorArray = d3.scale.category10().range();
    } else if (metrics.length < 20) {
        colorArray = d3.scale.category20().range();
    } else {
        colorArray = colorScale(metrics.length)
    }

    for (var i = 0, mlen = metrics.length; i < mlen; i++) {
        var metricBaselineData = [];
        var metricCurrentData = [];
        var deltaPercentageData = [];
        for (var t = 0, len = timeSeriesData["timeBuckets"].length; t < len; t++) {
            var baselineValue = timeSeriesData["data"][metrics[i]]["responseData"][t][0];
            var currentValue = timeSeriesData["data"][metrics[i]]["responseData"][t][1];
            var deltaPercentage = parseInt(timeSeriesData["data"][metrics[i]]["responseData"][t][2] * 100);
            metricBaselineData.push(baselineValue);
            metricCurrentData.push(currentValue);
            deltaPercentageData.push(deltaPercentage);
        }
        lineChartData[metrics[i] + "-baseline"] = metricBaselineData;
        lineChartData[metrics[i] + "-current"] = metricCurrentData;

        colors[metrics[i] + "-baseline"] = colorArray[i];
        colors[metrics[i] + "-current"] = colorArray[i];

    }

    anomalyLineChart = c3.generate({
        bindto: lineChartPlaceholder,
        padding: {
            top: 0,
            right: 100,
            bottom: 20,
            left: 100
        },
        data: {
            x: 'time',
            json: lineChartData,
            type: 'spline',
            colors: colors
        },
        axis: {
            x: {
                type: 'timeseries',
                tick: {
                    count:11,
                    width:84,
                    multiline: true,
                    format: dateTimeFormat
                }
            },
            y: {
                tick: {
                    //format integers with comma-grouping for thousands
                    format: d3.format(",.1 ")
                }
            }
        },
        regions: regions,
        legend: {
            show: false
        },
        grid: {
            x: {
                show: false
            },
            y: {
                show: false
            }
        },
        point: {
            show: false
        }
    });

    anomalyLineChart.hide();
    var numAnomalies = anomalyData.length
    var regionColors;

    if (anomalyData.length > 0 && anomalyData[0]["regionColor"]) {

        regionColors = [];
        regionColors.push( anomalyData[0]["regionColor"] )

    } else if(parseInt(numAnomalies) < 10) {
        regionColors = d3.scale.category10().range();

    } else if (numAnomalies < 20) {
        regionColors = d3.scale.category20().range();
    } else {
        regionColors = colorScale(numAnomalies);
    }


    //paint the anomaly regions based on anomaly id
    for (var i = 0; i < numAnomalies; i++) {
        var anomalyId = "anomaly-id-" + anomalyData[i]["id"];
        d3.select("." + anomalyId + " rect")
            .style("fill", regionColors[i])
    }


    attach_TimeSeries_EventListeners(currentView)

} //end of drawAnomalyTimeSeries

function renderAnomalyTable(data, tab) {
    //Error handling when data is falsy (empty, undefined or null)
    if (!data) {
        $("#" + tab + "-chart-area-error").empty()
        var warning = $('<div></div>', { class: 'uk-alert uk-alert-warning' })
        warning.append($('<p></p>', { html: 'Something went wrong. Please try and reload the page. Error: anomalies data =' + data  }))
        $("#" + tab + "-chart-area-error").append(warning)
        $("#" + tab + "-chart-area-error").show()
        return
    }

    /* Handelbars template for table */
    var result_anomalies_template = HandleBarsTemplates.template_anomalies(data);
    $("#" + tab + "-display-chart-section").append(result_anomalies_template);

    /** Create Datatables instance of the anomalies table **/
    $("#anomalies-table").DataTable();

    attach_AnomalyTable_EventListeners()

}


 function attach_TimeSeries_EventListeners(currentView){

     //Unbind previously attached eventlisteners
     currentView.off("click");
     currentView.off("change");

     // Clicking the checkbox of the timeseries legend will redraw the timeseries
     // with the selected elements
     currentView.on("click", '.time-series-metric-checkbox', function () {
         anomalyTimeSeriesCheckbox(this);
     });

     //Select all / deselect all metrics option
     currentView.on("click", ".time-series-metric-select-all-checkbox", function () {
         anomalyTimeSelectAllCheckbox(this);
     });


     //licking a checkbox in the table toggles the region of that timerange on the timeseries chart
     currentView.on("change", ".anomaly-table-checkbox input", function () {
         toggleAnomalyTimeRange(this);
     });

     //Preselect first metric on load
     $($(".time-series-metric-checkbox", currentView)[0]).click();


     function anomalyTimeSeriesCheckbox(target) {
         var checkbox = target;
         var checkboxObj = $(checkbox);
         metricName = checkboxObj.val();
         if (checkboxObj.is(':checked')) {
             //Show metric's lines on timeseries
             anomalyLineChart.show(metricName + "-current");
             anomalyLineChart.show(metricName + "-baseline");

             //show related ranges on timeserie and related rows in the tabular display
    //            $(".anomaly-table-checkbox input").each(function () {
    //
    //                if ($(this).attr("data-value") == metricName) {
    //                    var tableRow = $(this).closest("tr");
    //                    tableRow.show()
    //                    //check the related input boxes
    //                    $("input", tableRow).attr('checked', 'checked');
    //                    $("input", tableRow).prop('checked', true);
    //                    //show the related timeranges
    //                    var anomalyId = "anomaly-id-" + $(this).attr("id");
    //                    $("." + anomalyId).show();
    //                }
    //            })

         } else {
             //Hide metric's lines on timeseries
             anomalyLineChart.hide(metricName + "-current");
             anomalyLineChart.hide(metricName + "-baseline");

             //hide related ranges on timeserie and related rows in the tabular display
    //            $(".anomaly-table-checkbox input").each(function () {
    //
    //                if ($(this).attr("data-value") == metricName) {
    //                    $(this).closest("tr").hide();
    //                    var anomalyId = "anomaly-id-" + $(this).attr("id");
    //                    $("." + anomalyId).hide();
    //                }
    //            })
         }
     }

     function anomalyTimeSelectAllCheckbox(target) {
         //if select all is checked
         if ($(target).is(':checked')) {
             //trigger click on each unchecked checkbox
             $(".time-series-metric-checkbox", currentView).each(function (index, checkbox) {
                 if (!$(checkbox).is(':checked')) {
                     $(checkbox).click();
                 }
             })
         } else {
             //trigger click on each checked checkbox
             $(".time-series-metric-checkbox", currentView).each(function (index, checkbox) {
                 if ($(checkbox).is(':checked')) {
                     $(checkbox).click();
                 }
             })
         }
     }

     function toggleAnomalyTimeRange(target) {
         var anomalyId = ".anomaly-id-" + $(target).attr("id");
         if ($(target).is(':checked')) {
             $(anomalyId).show();
         } else {
             $(anomalyId).hide();
         }
     }

     //Show the first line on the timeseries
     var firstLegendLabel = $($(".time-series-metric-checkbox")[0])
     if( !firstLegendLabel.is(':checked')) {
         firstLegendLabel.click();
     }
 }

 function attach_AnomalyTable_EventListeners(){

     //Unbind previously attached eventlisteners
     $("#anomalies-table").off("click");
     $("#anomalies-table").off("hide.uk.dropdown");


     //Select all checkbox selects the checkboxes in all rows
     $("#anomalies-table").on("click", ".select-all-checkbox", function () {

         var currentTable = $(this).closest("table");

         if ($(this).is(':checked')) {
             $("input[type='checkbox']", currentTable).attr('checked', 'checked');
             $("input[type='checkbox']", currentTable).prop('checked', true);
             $("input[type='checkbox']", currentTable).change();

         } else {
             $("input[type='checkbox']", currentTable).removeAttr('checked');
             $("input[type='checkbox']", currentTable).prop('checked', false);
             $("input[type='checkbox']", currentTable).change();

         }
     })

     $("#anomalies-table").on("click", ".view-chart-link", function () {
         updateChartForSingleAnomaly(this);
     });

     //Clicking a checkbox in the table takes user to related heatmap chart
     $("#anomalies-table").on("click", ".heatmap-link", function () {
         showHeatMapOfAnomaly(this);
     });

     //Clicking the feedback option will trigger the ajax - post
     $("#anomalies-table").on("click", ".feedback-list", function () {
         $(this).next("textarea").show();
     });


     $('.feedback-dropdown[data-uk-dropdown]').on('hide.uk.dropdown', function(){

         submitAnomalyFeedback(this);
     });

     /** Compare/Tabular view and dashboard view heat-map-cell click switches the view to compare/heat-map
      * focusing on the timerange of the cell or in case of cumulative values it query the cumulative timerange **/
     function showHeatMapOfAnomaly(target) {

         var $target = $(target);
         hash.view = "compare";
         hash.aggTimeGranularity = "aggregateAll";

         var currentStartUTC = $target.attr("data-start-utc-millis");
         var currentEndUTC = $target.attr("data-end-utc-millis");

         //Using WoW for anomaly baseline
         var baselineStartUTC = moment(parseInt(currentStartUTC)).add(-7, 'days').valueOf();
         var baselineEndUTC = moment(parseInt(currentEndUTC)).add(-7, 'days').valueOf();

         hash.baselineStart = baselineStartUTC;
         hash.baselineEnd = baselineEndUTC;
         hash.currentStart = currentStartUTC;
         hash.currentEnd = currentEndUTC;
         delete hash.dashboard;
         metrics = [];
         var metricName = $target.attr("data-metric");
         metrics.push(metricName);
         hash.metrics = metrics.toString();

         //update hash will trigger window.onhashchange event:
         // update the form area and trigger the ajax call
         window.location.hash = encodeHashParameters(hash);
     }

     function submitAnomalyFeedback(target) {
         var $target = $(target);
         var selector = $(".selected-feedback", $target);
         var feedbackType = selector.attr("value");
         var anomalyId = selector.attr("data-anomaly-id");
         var comment = $(".feedback-comment", $target).val();

         //Remove control characters
         comment = comment.replace(/[\x00-\x1F\x7F-\x9F]/g, "")
         if(feedbackType){

             var data = '{ "feedbackType": "' + feedbackType + '","comment": "'+ comment +'"}';
             var url = "/dashboard/anomaly-merged-result/feedback/" + anomalyId;

             //post anomaly feedback
             submitData(url, data).done(function () {
                 $(selector).addClass("green-background");
             }).fail(function(){
                 $(selector).addClass("red-background");
             })
         }
     }

     // TODO: requires refactoring !!

     function updateChartForSingleAnomaly(target) {


         var button = $(target);
         var dimension = button.attr("data-explore-dimensions");
         var value = button.attr("data-dimension-value");
         var startTime = button.attr("data-start-time");
         var endTime = button.attr("data-end-time");
         var anomalyId = button.attr("data-anomaly-id");
         var row = button.closest('tr')
         var colorRGB = $(".color-box", row).css("background-color");
         var colorHEX = rgbToHex(colorRGB);
         var baselineStart = moment(parseInt(hash.currentStart)).add(-7, 'days')
         var baselineEnd = moment(parseInt(hash.currentEnd)).add(-7, 'days')
         var aggTimeGranularity = (window.datasetConfig.dataGranularity) ? window.datasetConfig.dataGranularity : "HOURS";
         var dataset = hash.dataset;
         var compareMode = "WoW";
         var currentStart = hash.currentStart;
         var currentEnd = hash.currentEnd;
         var metrics = hash.metrics;

         var filter = "{}";
         if(dimension && value && value != 'ALL') {
             filter = '{"'+dimension+'":["'+value+'"]}';
         }

         var timeSeriesUrl = "/dashboard/data/tabular?dataset=" + dataset + "&compareMode=" + compareMode //
             + "&currentStart=" + currentStart + "&currentEnd=" + currentEnd  //
             + "&baselineStart=" + baselineStart + "&baselineEnd=" + baselineEnd   //
             + "&aggTimeGranularity=" + aggTimeGranularity + "&metrics=" + metrics+ "&filters=" + filter;
         var tab = hash.view;

         getDataCustomCallback(timeSeriesUrl,tab ).done(function (timeSeriesData) {
             //Error handling when data is falsy (empty, undefined or null)
             if (!timeSeriesData) {
                 // do nothing
                 return
             } else {
                 $("#" + tab + "-chart-area-error").hide();
             }
             var placeholder = "linechart-placeholder";
             var anomalyRegionData = [];
             console.log('color')
             console.log(colorHEX)
             anomalyRegionData.push({startTime: parseInt(startTime), endTime: parseInt(endTime), id: anomalyId, regionColor: colorHEX});
             drawAnomalyTimeSeries(timeSeriesData, anomalyRegionData, tab, placeholder);

         });
     }

     //Set initial view:
     $(".select-all-checkbox").click();
     $($(".anomaly-table-checkbox")[0]).click();

 }























