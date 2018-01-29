/**
 * Handles alert form creation settings
 * @module self-serve/create/controller
 * @exports create
 */
import fetch from 'fetch';
import moment from 'moment';
import _ from 'lodash';
import Controller from '@ember/controller';
import { computed } from '@ember/object';
import { task, timeout } from 'ember-concurrency';
import { checkStatus, buildDateEod, postProps } from 'thirdeye-frontend/helpers/utils';

export default Controller.extend({

  /**
   * Initialized alert creation page settings
   */
  isValidated: false,
  isMetricSelected: false,
  isFormDisabled: false,
  isMetricDataInvalid: false,
  isCreateAlertSuccess: false,
  isCreateGroupSuccess: false,
  isCreateAlertError: false,
  isSelectMetricError: false,
  isReplayStatusError: false,
  isMetricDataLoading: false,
  isReplayStatusPending: true,
  isReplayStatusSuccess: false,
  isReplayStarted: false,
  isGroupNameDuplicate: false,
  isAlertNameDuplicate: false,
  isFetchingDimensions: false,
  isDimensionFetchDone: false,
  isProcessingForm: false,
  isEmailError: false,
  isDuplicateEmail: false,
  showGraphLegend: false,
  redirectToAlertPage: true,
  metricGranularityOptions: [],
  topDimensions: [],
  originalDimensions: [],
  bsAlertBannerType: 'success',
  graphEmailLinkProps: '',
  replayStatusClass: 'te-form__banner--pending',
  legendText: {
    dotted: {
      text: 'WoW'
    },
    solid: {
      text: 'Observed'
    }
  },

  /**
   * Component property initial settings
   */
  filters: {},
  graphConfig: {},
  selectedFilters: JSON.stringify({}),
  selectedWeeklyEffect: true,

  /**
   * Object to cover basic ield 'presence' validation
   */
  requiredFields: [
    'selectedMetricOption',
    'selectedPattern',
    'alertFunctionName',
    'selectedAppName'
  ],

  /**
   * Array to define alerts table columns for selected config group
   */
  alertsTableColumns: [
    {
      propertyName: 'id',
      title: 'Id',
      className: 'te-form__table-index'
    },
    {
      propertyName: 'name',
      title: 'Alert Name'
    },
    {
      propertyName: 'metric',
      title: 'Alert Metric'
    },
    {
      propertyName: 'type',
      title: 'Alert Type'
    }
  ],

  /**
   * Options for patterns of interest field. These may eventually load from the backend.
   */
  patternsOfInterest: ['Up and Down', 'Up only', 'Down only'],

  /**
   * Mapping user readable pattern and sensitivity to DB values
   */
  optionMap: {
    pattern: {
      'Up and Down': 'UP,DOWN',
      'Up only': 'UP',
      'Down only': 'DOWN'
    },
    sensitivity: {
      'Robust (Low)': 'LOW',
      'Medium': 'MEDIUM',
      'Sensitive (High)': 'HIGH'
    },
    severity: {
      'Percentage of Change': 'weight',
      'Absolute Value of Change': 'deviation'
    }
  },

  /**
   * Severity display options (power-select) and values
   * @type {Object}
   */
  tuneSeverityOptions: computed(
    'optionMap.severity',
    function() {
      const severityOptions = Object.keys(this.get('optionMap.severity'));
      return severityOptions;
    }
  ),

  /**
   * Conditionally display '%' based on selected severity option
   * @type {String}
   */
  sensitivityUnits: computed('selectedSeverityOption', function() {
    const chosenSeverity = this.get('selectedSeverityOption');
    const isNotPercent = chosenSeverity && chosenSeverity.includes('Absolute');
    return isNotPercent ? '' : '%';
  }),

  /**
   * Builds the new autotune filter from custom tuning options
   * @type {String}
   */
  alertFilterObj: computed(
    'selectedSeverityOption',
    'customPercentChange',
    'customMttdChange',
    'selectedPattern',
    function() {
      const {
        severity: severityMap,
        pattern: patternMap
      } = this.getProperties('optionMap').optionMap;

      const {
        selectedPattern,
        customMttdChange,
        customPercentChange,
        selectedSeverityOption: selectedSeverity
      } = this.getProperties('selectedPattern', 'customMttdChange', 'customPercentChange', 'selectedSeverityOption');

      const requiredProps = ['customMttdChange', 'customPercentChange', 'selectedSeverityOption'];
      const isCustomFilterPossible = requiredProps.every(val => Ember.isPresent(this.get(val)));
      const filterObj = {
        pattern: patternMap[selectedPattern],
        isCustom: isCustomFilterPossible
      };

      if (isCustomFilterPossible) {
        const mttdVal = Number(customMttdChange).toFixed(2);
        const severityThresholdVal = (Number(customPercentChange)/100).toFixed(2);
        Object.assign(filterObj, {
          features: `window_size_in_hour,${severityMap[selectedSeverity]}`,
          mttd: `window_size_in_hour=${mttdVal};${severityMap[selectedSeverity]}=${severityThresholdVal}`
        });
      }

      return filterObj;
    }
  ),

  /**
   * All selected dimensions to be loaded into graph
   * @returns {Array}
   */
  selectedDimensions: computed(
    'topDimensions',
    'topDimensions.@each.isSelected',
    function() {
      return this.get('topDimensions').filterBy('isSelected');
    }
  ),

  /**
   * Application name field options loaded from our model.
   */
  allApplicationNames: Ember.computed.reads('model.allAppNames'),

  /**
   * The list of all existing alert configuration groups.
   */
  allAlertsConfigGroups: Ember.computed.reads('model.allConfigGroups'),

  /**
   * Handler for search by function name - using ember concurrency (task)
   * @method searchMetricsList
   * @param {metric} String - portion of metric name used in typeahead
   * @return {Promise}
   */
  searchMetricsList: task(function* (metric) {
    yield timeout(600);
    const url = `/data/autocomplete/metric?name=${metric}`;
    return fetch(url).then(checkStatus);
  }),

  /**
   * Determines if a metric should be filtered out
   * @method isMetricGraphable
   * @param {Object} metric
   * @returns {Boolean}
   */
  isMetricGraphable(metric) {
    return metric
    && metric.subDimensionContributionMap['All'].currentValues
    && metric.subDimensionContributionMap['All'].currentValues.reduce((total, val) => {
      return total + val;
    }, 0);
  },

  /**
   * Fetches an alert function record by Id.
   * Use case: show me the names of all functions monitored by a given alert group.
   * @method fetchFunctionById
   * @param {Number} functionId - Id for the selected alert function
   * @return {Promise}
   */
  fetchFunctionById(functionId) {
    const url = `/onboard/function/${functionId}`;
    return fetch(url).then(checkStatus);
  },

  /**
   * Fetches an alert function record by name.
   * Use case: when user names an alert, make sure no duplicate already exists.
   * @method fetchAnomalyByName
   * @param {String} functionName - name of alert or function
   * @return {Promise}
   */
  fetchAnomalyByName(functionName) {
    const url = `/data/autocomplete/functionByName?name=${functionName}`;
    return fetch(url).then(checkStatus);
  },

  /**
   * Fetches all essential metric properties by metric Id.
   * This is the data we will feed to the graph generating component.
   * Note: these requests can fail silently and any empty response will fall back on defaults.
   * @method fetchMetricData
   * @param {Number} metricId - Id for the selected metric
   * @return {Ember.RSVP.promise}
   */
  fetchMetricData(metricId) {
    const promiseHash = {
      maxTime: fetch(`/data/maxDataTime/metricId/${metricId}`).then(res => checkStatus(res, 'get', true)),
      granularities: fetch(`/data/agg/granularity/metric/${metricId}`).then(res => checkStatus(res, 'get', true)),
      filters: fetch(`/data/autocomplete/filters/metric/${metricId}`).then(res => checkStatus(res, 'get', true)),
      dimensions: fetch(`/data/autocomplete/dimensions/metric/${metricId}`).then(res => checkStatus(res, 'get', true))
    };
    return Ember.RSVP.hash(promiseHash);
  },

  /**
   * Fetches the time series data required to display the anomaly detection graph for the current metric.
   * @method fetchAnomalyGraphData
   * @param {Object} config - key metric properties to graph
   * @return {Promise} Returns time-series data for the metric
   */
  fetchAnomalyGraphData(config) {
    const {
      id,
      dimension,
      currentStart,
      currentEnd,
      baselineStart,
      baselineEnd,
      granularity,
      filters
    } = config;

    const url = `/timeseries/compare/${id}/${currentStart}/${currentEnd}/${baselineStart}/${baselineEnd}?dimension=${dimension}&granularity=${granularity}&filters=${encodeURIComponent(filters)}`;
    return fetch(url).then(checkStatus);
  },

  /**
   * Loads time-series data into the anomaly-graph component.
   * Note: 'MINUTE' granularity loads 1 week of data. Otherwise, it loads 1 month.
   * @method triggerGraphFromMetric
   * @param {Number} metricId - Id of selected metric to graph
   * @return {undefined}
   */
  triggerGraphFromMetric(metricId) {
    const id = metricId.id;
    const maxDimensionSize = 5;
    const maxTime = this.get('maxTime');
    const selectedDimension = this.get('selectedDimension');
    const filters = this.get('selectedFilters') || '';
    const dimension = selectedDimension || 'All';
    const currentEnd = moment(maxTime).isValid()
      ? moment(maxTime).valueOf()
      : buildDateEod(1, 'day').valueOf();
    const currentStart = moment(currentEnd).subtract(1, 'months').valueOf();
    const baselineStart = moment(currentStart).subtract(1, 'week').valueOf();
    const baselineEnd = moment(currentEnd).subtract(1, 'week');
    const granularity = this.get('selectedGranularity') || this.get('granularities.firstObject') || '';
    const isMinutely = granularity.toLowerCase().includes('minute');
    const graphConfig = {
      id,
      dimension,
      currentStart,
      currentEnd,
      baselineStart,
      baselineEnd,
      granularity,
      filters
    };

    // Reduce data volume by narrowing graph window to 2 weeks for minute granularity
    if (isMinutely) {
      graphConfig.currentStart = moment(currentEnd).subtract(2, 'week').valueOf();
    }

    // Update graph, and related fields
    this.setProperties({
      graphConfig: graphConfig,
      selectedGranularity: granularity,
      isFilterSelectDisabled: Ember.isEmpty(filters)
    });

    // Fetch new graph metric data
    this.fetchAnomalyGraphData(graphConfig).then(metricData => {
      if (!this.isMetricGraphable(metricData)) {
        // Metric has no data. not graphing
        this.setProperties({
          isMetricDataInvalid: true,
          isMetricDataLoading: false
        });
      } else {
        // Dimensions are selected. Compile, rank, and send them to the graph.
        if(selectedDimension) {
          this.getTopDimensions(metricData, graphConfig, maxDimensionSize, selectedDimension)
            .then(orderedDimensions => {
              this.setProperties({
                isMetricSelected: true,
                isFetchingDimensions: false,
                isDimensionFetchDone: true,
                isMetricDataLoading: false,
                topDimensions: orderedDimensions
              });
            })
            .catch(() => {
              this.set('isMetricDataLoading', false);
            });
        }
        // Metric has data. now sending new data to graph.
        this.setProperties({
          isMetricSelected: true,
          isMetricDataLoading: false,
          showGraphLegend: Ember.isPresent(selectedDimension),
          selectedMetric: Object.assign(metricData, { color: 'blue' })
        });
      }
    }).catch((error) => {
      // The request failed. No graph to render.
      this.clearAll();
      this.setProperties({
        isMetricDataLoading: false,
        selectMetricErrMsg: error
      });
    });
  },

  /**
   * If a dimension has been selected, the metric data object will contain subdimensions.
   * This method calls for dimension ranking by metric, filters for the selected dimension,
   * and returns a sorted list of graph-ready dimension objects.
   * @method getTopDimensions
   * @param {Object} data - the graphable metric data returned from fetchAnomalyGraphData()
   * @param {Object} config - the graph configuration object
   * @param {Number} maxSize - number of sub-dimensions to display on graph
   * @param {String} selectedDimension - the user-selected dimension to graph
   * @return {undefined}
   */
  getTopDimensions(data, config, maxSize, selectedDimension) {
    const url = `/rootcause/query?framework=relatedDimensions&anomalyStart=${config.currentStart}&anomalyEnd=${config.currentEnd}&baselineStart=${config.baselineStart}&baselineEnd=${config.baselineEnd}&analysisStart=${config.currentStart}&analysisEnd=${config.currentEnd}&urns=thirdeye:metric:${config.id}&filters=${encodeURIComponent(config.filters)}`;
    const colors = ['orange', 'teal', 'purple', 'red', 'green', 'pink'];
    const dimensionObj = data.subDimensionContributionMap || {};
    let dimensionList = [];
    let topDimensions = [];
    let topDimensionLabels = [];
    let filteredDimensions = [];
    let colorIndex = 0;

    return new Ember.RSVP.Promise((resolve) => {
      fetch(url).then(checkStatus)
        .then((scoredDimensions) => {
          // Select scored dimensions belonging the selected one
          filteredDimensions =  _.filter(scoredDimensions, function(dimension) {
            return dimension.label.split('=')[0] === selectedDimension;
          });
          // Prep a sorted list of labels for our dimension's top contributing sub-dimensions
          topDimensions = filteredDimensions.sortBy('score').reverse().slice(0, maxSize);
          topDimensionLabels = [...new Set(topDimensions.map(key => key.label.split('=')[1]))];
          // Build the array of subdimension objects for the selected dimension
          for (let subDimension of topDimensionLabels) {
            if (subDimension && dimensionObj[subDimension]) {
              dimensionList.push({
                name: subDimension,
                color: colors[colorIndex],
                baselineValues: dimensionObj[subDimension].baselineValues,
                currentValues: dimensionObj[subDimension].currentValues,
                isSelected: true
              });
              colorIndex++;
            }
          }
          // Return sorted list of dimension objects
          resolve(dimensionList);
        });
    });
  },

  /**
   * Enriches the list of functions by Id, adding the properties we may want to display.
   * We are preparing to display the alerts that belong to the currently selected config group.
   * @method prepareFunctions
   * @param {Object} configGroup - the currently selected alert config group
   * @param {Object} newId - conditional param to help us tag any function that was "just added"
   * @return {Ember.RSVP.Promise} A new list of functions (alerts)
   */
  prepareFunctions(configGroup, newId = 0) {
    const newFunctionList = [];
    const existingFunctionList = configGroup.emailConfig ? configGroup.emailConfig.functionIds : [];
    let cnt = 0;

    // Build object for each function(alert) to display in results table
    return new Ember.RSVP.Promise((resolve) => {
      for (var functionId of existingFunctionList) {
        this.fetchFunctionById(functionId).then(functionData => {
          newFunctionList.push({
            number: cnt + 1,
            id: functionData.id,
            name: functionData.functionName,
            metric: functionData.metric + '::' + functionData.collection,
            type: functionData.type,
            active: functionData.isActive,
            isNewId: functionData.id === newId
          });
          cnt ++;
          if (existingFunctionList.length === cnt) {
            if (newId) {
              newFunctionList.reverse();
            }
            resolve(newFunctionList);
          }
        });
      }
    });
  },

  /**
   * If these two conditions are true, we assume the user wants to edit an existing alert group
   * @method isAlertGroupEditModeActive
   * @return {Boolean}
   */
  isAlertGroupEditModeActive: computed(
    'selectedConfigGroup',
    'newConfigGroupName',
    function() {
      return this.get('selectedConfigGroup') && Ember.isNone(this.get('newConfigGroupName'));
    }
  ),

  /**
   * Determines cases in which the filter field should be disabled
   * @method isFilterSelectDisabled
   * @return {Boolean}
   */
  isFilterSelectDisabled: computed(
    'filters',
    'isMetricSelected',
    function() {
      return (!this.get('isMetricSelected') || Ember.isEmpty(this.get('filters')));
    }
  ),

  /**
   * Determines cases in which the granularity field should be disabled
   * @method isGranularitySelectDisabled
   * @return {Boolean}
   */
  isGranularitySelectDisabled: computed(
    'granularities',
    'isMetricSelected',
    function() {
      return (!this.get('isMetricSelected') || Ember.isEmpty(this.get('granularities')));
    }
  ),

  /**
   * Enables the submit button when all required fields are filled
   * @method isSubmitDisabled
   * @param {Number} metricId - Id of selected metric to graph
   * @return {Boolean} PreventSubmit
   */
  isSubmitDisabled: computed(
    'selectedMetricOption',
    'selectedPattern',
    'selectedWeeklyEffect',
    'alertFunctionName',
    'selectedAppName',
    'selectedConfigGroup',
    'newConfigGroupName',
    'alertGroupNewRecipient',
    'isAlertNameDuplicate',
    'isGroupNameDuplicate',
    'isProcessingForm',
    function() {
      let isDisabled = false;
      const {
        requiredFields,
        isProcessingForm,
        newConfigGroupName,
        isAlertNameDuplicate,
        isGroupNameDuplicate,
        alertGroupNewRecipient,
        selectedConfigGroup: groupRecipients,
      } = this.getProperties(
        'requiredFields',
        'isProcessingForm',
        'newConfigGroupName',
        'isAlertNameDuplicate',
        'isGroupNameDuplicate',
        'alertGroupNewRecipient',
        'selectedConfigGroup'
      );
      const hasRecipients = _.has(groupRecipients, 'recipients');
      // Any missing required field values?
      for (var field of requiredFields) {
        if (Ember.isBlank(this.get(field))) {
          isDisabled = true;
        }
      }
      // Enable submit if either of these field values are present
      if (Ember.isBlank(groupRecipients) && Ember.isBlank(newConfigGroupName)) {
        isDisabled = true;
      }
      // Duplicate alert Name or group name
      if (isAlertNameDuplicate || isGroupNameDuplicate) {
        isDisabled = true;
      }
      // For alert group email recipients, require presence only if group recipients is empty
      if (Ember.isBlank(alertGroupNewRecipient) && !hasRecipients) {
        isDisabled = true;
      }
      // Disable after submit clicked
      if (isProcessingForm) {
        isDisabled = true;
      }
      return isDisabled;
    }
  ),

  /**
   * Double-check new email array for errors.
   * @method isEmailValid
   * @param {Array} emailArr - array of new emails entered by user
   * @return {Boolean} whether errors were found
   */
  isEmailValid(emailArr) {
    const emailRegex = /^.{3,}@linkedin.com$/;
    let isValid = true;

    for (var email of emailArr) {
      if (!emailRegex.test(email)) {
        isValid = false;
      }
    }

    return isValid;
  },

  /**
   * Check for missing email address
   * @method isEmailPresent
   * @param {Array} emailArr - array of new emails entered by user
   * @return {Boolean}
   */
  isEmailPresent(emailArr) {
    let isPresent = true;

    if (this.get('selectedConfigGroup') || this.get('newConfigGroupName')) {
      isPresent = Ember.isPresent(this.get('selectedGroupRecipients')) || Ember.isPresent(emailArr);
    }

    return isPresent;
  },

  /**
   * Filter all existing alert groups down to only those that are active and belong to the
   * currently selected application team.
   * @method filteredConfigGroups
   * @param {Object} selectedApplication - user-selected application object
   * @return {Array} activeGroups - filtered list of groups that are active
   */
  filteredConfigGroups: computed(
    'selectedApplication',
    function() {
      const appName = this.get('selectedApplication');
      const activeGroups = this.get('allAlertsConfigGroups').filterBy('active');
      const groupsWithAppName = activeGroups.filter(group => Ember.isPresent(group.application));

      if (Ember.isPresent(appName)) {
        return groupsWithAppName.filter(group => group.application.toLowerCase().includes(appName));
      } else {
        return activeGroups;
      }
    }
  ),

  /**
   * Sets the message text over the graph placeholder before data is loaded
   * @method graphMessageText
   * @return {String} the appropriate graph placeholder text
   */
  graphMessageText: computed(
    'isMetricDataInvalid',
    function() {
      const defaultMsg = 'Once a metric is selected, the metric replay graph will show here';
      const invalidMsg = 'Sorry, metric has no current data';
      return this.get('isMetricDataInvalid') ? invalidMsg : defaultMsg;
    }
  ),

  /**
   * Preps a mailto link containing the currently selected metric name
   * @method graphMailtoLink
   * @return {String} the URI-encoded mailto link
   */
  graphMailtoLink: computed(
    'selectedMetricOption',
    function() {
      const selectedMetric = this.get('selectedMetricOption');
      const fullMetricName = `${selectedMetric.dataset}::${selectedMetric.name}`;
      const recipient = 'ask_thirdeye@linkedin.com';
      const subject = 'TE Self-Serve Create Alert Metric Issue';
      const body = `TE Team, please look into a possible inconsistency issue with [ ${fullMetricName} ]`;
      const mailtoString = `mailto:${recipient}?subject=${encodeURIComponent(subject)}&body=${encodeURIComponent(body)}`;
      return mailtoString;
    }
  ),

  /**
   * Returns the appropriate subtitle for selected config group monitored alerts
   * @method selectedConfigGroupSubtitle
   * @return {String} title of expandable section for selected config group
   */
  selectedConfigGroupSubtitle: computed(
    'selectedConfigGroup',
    function () {
      return `Alerts Monitored by: ${this.get('selectedConfigGroup.name')}`;
    }
  ),

  /**
   * Builds the new alert settings to be sent to the alert creation task manager
   * @type {Object}
   */
  onboardFunctionPayload: computed(
    'alertFunctionName',
    'selectedMetricOption',
    'selectedDimension',
    'selectedFilters',
    'selectedPattern',
    'selectedGranularity',
    'selectedWeeklyEffect',
    'selectedConfigGroup',
    'newConfigGroupName',
    'alertGroupNewRecipient',
    'selectedApplication',
    'alertFilterObj',
    function() {
      const {
        alertFunctionName: functionName,
        selectedMetricOption,
        selectedDimension,
        selectedFilters,
        selectedPattern,
        selectedGranularity,
        selectedWeeklyEffect,
        selectedConfigGroup,
        newConfigGroupName,
        alertGroupNewRecipient,
        selectedApplication,
        alertFilterObj
      } = this.getProperties(
        'alertFunctionName',
        'selectedMetricOption',
        'selectedDimension',
        'selectedFilters',
        'selectedPattern',
        'selectedGranularity',
        'selectedWeeklyEffect',
        'selectedConfigGroup',
        'newConfigGroupName',
        'alertGroupNewRecipient',
        'selectedApplication',
        'alertFilterObj'
      );

      const jobName = `${functionName}:${selectedMetricOption.id}`;
      const newAlertObj = {
        functionName,
        collection: selectedMetricOption.dataset,
        metric: selectedMetricOption.name,
        alertRecipients: alertGroupNewRecipient,
        dataGranularity: selectedGranularity,
        pattern: alertFilterObj.pattern,
        application: selectedApplication
      };

      // Prepare config group property for new alert object and add it
      const isGroupExisting = selectedConfigGroup && Ember.isNone(newConfigGroupName);
      const subscriptionGroupKey = isGroupExisting ? 'alertId' : 'alertName';
      const subscriptionGroupValue = isGroupExisting ? selectedConfigGroup.id.toString() : newConfigGroupName;
      newAlertObj[subscriptionGroupKey] = subscriptionGroupValue;

      // Do we have custom sensitivity settings to add?
      if (alertFilterObj.isCustom) {
        Object.assign(newAlertObj, { features: alertFilterObj.features, mttd: alertFilterObj.mttd });
      }

      // Add filters property if present
      if (selectedFilters.length > 2) {
        Object.assign(newAlertObj, { filters: encodeURIComponent(selectedFilters) });
      }

      // Add dimensions if present
      if (selectedDimension) {
        Object.assign(newAlertObj, { exploreDimensions: selectedDimension });
      }

      return {
        jobName,
        payload: JSON.stringify(newAlertObj)
      };

    }
  ),

  /**
   * Reset the form... clear all important fields
   * @method clearAll
   * @return {undefined}
   */
  clearAll() {
    this.setProperties({
      isFetchingDimensions: false,
      isDimensionFetchDone: false,
      isEmailError: false,
      isEmptyEmail: false,
      isFormDisabled: false,
      isMetricSelected: false,
      isMetricDataInvalid: false,
      isSelectMetricError: false,
      selectedMetricOption: null,
      selectedPattern: null,
      selectedGranularity: null,
      selectedWeeklyEffect: true,
      selectedDimension: null,
      alertFunctionName: null,
      selectedAppName: null,
      selectedConfigGroup: null,
      newConfigGroupName: null,
      alertGroupNewRecipient: null,
      selectedGroupRecipients: null,
      isCreateAlertSuccess: null,
      isCreateAlertError: false,
      isProcessingForm: false,
      isCreateGroupSuccess: false,
      isReplayStatusSuccess: false,
      isReplayStarted: false,
      isReplayStatusError: false,
      isGroupNameDuplicate: false,
      isAlertNameDuplicate: false,
      graphEmailLinkProps: '',
      bsAlertBannerType: 'success',
      selectedFilters: JSON.stringify({}),
      replayStatusClass: 'te-form__banner--pending'
    });
    this.send('refreshModel');
  },

  /**
   * Actions for create alert form view
   */
  actions: {


    /**
     * Handles the primary metric selection in the alert creation
     */
    onPrimaryMetricToggle() {
      return;
    },

    /**
     * When a metric is selected, fetch its props, and send them to the graph builder
     * @method onSelectMetric
     * @param {Object} selectedObj - The selected metric
     * @return {undefined}
     */
    onSelectMetric(selectedObj) {
      this.clearAll();
      this.setProperties({
        isMetricDataLoading: true,
        topDimensions: [],
        selectedMetricOption: selectedObj
      });
      this.fetchMetricData(selectedObj.id)
        .then((hash) => {
          this.setProperties(hash);
          this.setProperties({
            metricGranularityOptions: hash.granularities,
            originalDimensions: hash.dimensions
          });
          this.triggerGraphFromMetric(selectedObj);
        })
        .catch((err) => {
          this.setProperties({
            isSelectMetricError: true,
            selectMetricErrMsg: err
          });
        });
    },

    /**
     * When a filter is selected, fetch new anomaly graph data based on that filter
     * and trigger a new graph load. Also filter dimension names already selected as filters.
     * @method onSelectFilter
     * @param {Object} selectedFilters - The selected filters to apply
     * @return {undefined}
     */
    onSelectFilter(selectedFilters) {
      const selectedFilterObj = JSON.parse(selectedFilters);
      const dimensionNameSet = new Set(this.get('originalDimensions'));
      const filterNames = Object.keys(JSON.parse(selectedFilters));
      let isSelectedDimensionEqualToSelectedFilter = false;

      this.set('graphConfig.filters', selectedFilters);
      // Remove selected filters from dimension options only if filter has single entity
      for (var key of filterNames) {
        if (selectedFilterObj[key].length === 1) {
          dimensionNameSet.delete(key);
          if (key === this.get('selectedDimension')) {
            isSelectedDimensionEqualToSelectedFilter = true;
          }
        }
      }
      // Update dimension options and loader
      this.setProperties({
        dimensions: [...dimensionNameSet],
        isMetricDataLoading: true
      });
      // Do not allow selected dimension to match selected filter
      if (isSelectedDimensionEqualToSelectedFilter) {
        this.set('selectedDimension', 'All');
      }
      // Fetch new graph data with selected filters
      this.triggerGraphFromMetric(this.get('selectedMetricOption'));
    },

    /**
     * When a dimension is selected, fetch new anomaly graph data based on that dimension
     * and trigger a new graph load, showing the top contributing subdimensions.
     * @method onSelectFilter
     * @param {Object} selectedDimension - The selected dimension to apply
     * @return {undefined}
     */
    onSelectDimension(selectedDimension) {
      this.setProperties({
        selectedDimension,
        isMetricDataLoading: true,
        isFetchingDimensions: true,
        isDimensionFetchDone: false
      });
      this.triggerGraphFromMetric(this.get('selectedMetricOption'));
    },

    /**
     * Set our selected granularity. Trigger graph reload.
     * @method onSelectGranularity
     * @param {Object} selectedObj - The selected granularity option
     * @return {undefined}
     */
    onSelectGranularity(selectedObj) {
      this.setProperties({
        selectedGranularity: selectedObj,
        isMetricDataLoading: true
      });
      this.triggerGraphFromMetric(this.get('selectedMetricOption'));
    },

    /**
     * Set our selected application name
     * @method onSelectAppName
     * @param {Object} selectedObj - The selected app name option
     * @return {undefined}
     */
    onSelectAppName(selectedObj) {
      this.setProperties({
        selectedAppName: selectedObj,
        selectedApplication: selectedObj.application
      });
    },

    /**
     * Set our selected alert configuration group. If one is selected, display editable fields
     * for that group and display the list of functions that belong to that group.
     * @method onSelectConfigGroup
     * @param {Object} selectedObj - The selected config group option
     * @return {undefined}
     */
    onSelectConfigGroup(selectedObj) {
      const emails = selectedObj.recipients || '';
      this.setProperties({
        selectedConfigGroup: selectedObj,
        newConfigGroupName: null,
        isEmptyEmail: Ember.isEmpty(emails),
        selectedGroupRecipients: emails.split(',').filter(e => String(e).trim()).join(', ')
      });
      this.prepareFunctions(selectedObj).then(functionData => {
        this.set('selectedGroupFunctions', functionData);
      });
    },

    /**
     * Make sure alert name does not already exist in the system
     * @method validateAlertName
     * @param {String} name - The new alert name
     * @return {undefined}
     */
    validateAlertName(name) {
      let isDuplicateName = false;
      this.fetchAnomalyByName(name).then(anomaly => {
        for (var resultObj of anomaly) {
          if (resultObj.functionName === name) {
            isDuplicateName = true;
          }
        }
        this.set('isAlertNameDuplicate', isDuplicateName);
      });
    },

    /**
     * Reset selected group list if user chooses to create a new group
     * @method validateNewGroupName
     * @param {String} name - User-provided alert group name
     * @return {undefined}
     */
    validateNewGroupName(name) {
      this.set('isGroupNameDuplicate', false);
      // return early if name is empty
      if (!name || !name.trim().length) { return; }
      const nameExists = this.get('allAlertsConfigGroups')
        .map(group => group.name)
        .includes(name);

      // set error message and return early if group name exists
      if (nameExists) {
        this.set('isGroupNameDuplicate', true);
        return;
      }

      this.setProperties({
        newConfigGroupName: name,
        selectedConfigGroup: null,
        selectedGroupRecipients: null,
        isEmptyEmail: Ember.isEmpty(this.get('alertGroupNewRecipient'))
      });
    },

    /**
     * Verify that email address does not already exist in alert group. If it does, remove it and alert user.
     * @method validateAlertEmail
     * @param {String} emailInput - Comma-separated list of new emails to add to the config group.
     * @return {undefined}
     */
    validateAlertEmail(emailInput) {
      const newEmailArr = emailInput.replace(/\s+/g, '').split(',');
      let existingEmailArr = this.get('selectedGroupRecipients');
      let cleanEmailArr = [];
      let badEmailArr = [];
      let isDuplicateEmail = false;

      // Release submit button error state
      this.setProperties({
        isEmailError: false,
        isProcessingForm: false,
        isEditedConfigGroup: true,
        isEmptyEmail: Ember.isPresent(this.get('newConfigGroupName')) && !emailInput.length
      });

      // Check for duplicates
      if (emailInput.trim() && existingEmailArr) {
        existingEmailArr = existingEmailArr.replace(/\s+/g, '').split(',');
        for (var email of newEmailArr) {
          if (email.length && existingEmailArr.includes(email)) {
            isDuplicateEmail = true;
            badEmailArr.push(email);
          } else {
            cleanEmailArr.push(email);
          }
        }
        this.setProperties({
          isDuplicateEmail,
          duplicateEmails: badEmailArr.join()
        });
      }
    },

    /**
     * Reset the form... clear all important fields
     * @method clearAll
     * @return {undefined}
     */
    onResetForm() {
      this.clearAll();
    },

    /**
     * Enable reaction to dimension toggling in graph legend component
     * @method onSelection
     * @return {undefined}
     */
    onSelection(selectedDimension) {
      const { isSelected } = selectedDimension;
      Ember.set(selectedDimension, 'isSelected', !isSelected);
    },

    /**
     * Check for email errors before triggering onboarding job
     * @method onSubmit
     * @return {undefined}
     */
    onSubmit() {
      const {
        isDuplicateEmail,
        onboardFunctionPayload,
        alertGroupNewRecipient: newEmails,
      } = this.getProperties('isDuplicateEmail', 'onboardFunctionPayload', 'alertGroupNewRecipient');
      const newEmailsArr = newEmails ? newEmails.replace(/ /g, '').split(',') : [];
      const isEmailError = !this.isEmailValid(newEmailsArr);

      this.setProperties({
        isEmailError,
        isProcessingForm: true,
        isEmptyEmail: !this.isEmailPresent(newEmailsArr)
      });

      // Exit quietly (showing warning) in the event of error
      if (isEmailError || isDuplicateEmail) { return; }

      // Begin onboarding tasks
      this.send('triggerOnboardingJob', onboardFunctionPayload);
    }
  }
});
