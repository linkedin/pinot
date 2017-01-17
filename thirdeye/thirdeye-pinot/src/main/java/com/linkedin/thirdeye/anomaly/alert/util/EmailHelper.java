package com.linkedin.thirdeye.anomaly.alert.util;

import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;

import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewHandler;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewRequest;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.jfree.chart.JFreeChart;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonRequest;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonResponse;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.email.AnomalyGraphGenerator;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import static com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper.FILTER_TYPE_KEY;

public abstract class EmailHelper {

  private static final Logger LOG = LoggerFactory.getLogger(EmailHelper.class);
  private static final String PNG = ".png";

  private static final String EMAIL_REPORT_CHART_PREFIX = "email_report_chart_";

  private static final long HOUR_MILLIS = TimeUnit.HOURS.toMillis(1);
  private static final long DAY_MILLIS = TimeUnit.DAYS.toMillis(1);
  private static final long WEEK_MILLIS = TimeUnit.DAYS.toMillis(7);

  private static final int MINIMUM_GRAPH_WINDOW_HOURS = 24;
  private static final int MINIMUM_GRAPH_WINDOW_DAYS = 7;

  public static final String EMAIL_ADDRESS_SEPARATOR = ",";

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final LoadingCache<String, Long> collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getCollectionMaxDataTimeCache();
  private static final QueryCache queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();

  private static final DatasetConfigManager datasetConfigManager = DAO_REGISTRY.getDatasetConfigDAO();

  private EmailHelper() {

  }

  public static String writeTimeSeriesChart(final EmailConfigurationDTO config,
      TimeOnTimeComparisonHandler timeOnTimeComparisonHandler, final DateTime now,
      final DateTime then, final String collection,
      final Map<RawAnomalyResultDTO, String> anomaliesWithLabels) throws JobExecutionException {
    try {
      int windowSize = config.getWindowSize();
      TimeUnit windowUnit = config.getWindowUnit();
      long windowMillis = windowUnit.toMillis(windowSize);

      // TODO provide a way for email reports to specify desired graph granularity.
      DatasetConfigManager datasetConfigDAO = DAO_REGISTRY.getDatasetConfigDAO();
      DatasetConfigDTO datasetConfig = datasetConfigDAO.findByDataset(collection);
      TimeSpec timespec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
      TimeGranularity dataGranularity = timespec.getDataGranularity();

      TimeOnTimeComparisonResponse chartData =
          getData(timeOnTimeComparisonHandler, config, then, now, WEEK_MILLIS, dataGranularity);
      AnomalyGraphGenerator anomalyGraphGenerator = AnomalyGraphGenerator.getInstance();
      JFreeChart chart = anomalyGraphGenerator
          .createChart(chartData, dataGranularity, windowMillis, anomaliesWithLabels);
      String chartFilePath = EMAIL_REPORT_CHART_PREFIX + config.getId() + PNG;
      LOG.info("Writing chart to {}", chartFilePath);
      anomalyGraphGenerator.writeChartToFile(chart, chartFilePath);
      return chartFilePath;
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  /**
   * Generate and send request to retrieve chart data. If the request window is too small, the graph
   * data retrieved has default window sizes based on time granularity and ending at the defined
   * endpoint: <br/> <ul> <li>DAYS: 7</li> <li>HOURS: 24</li> </ul>
   *
   * @param bucketGranularity
   *
   * @throws JobExecutionException
   */
  public static TimeOnTimeComparisonResponse getData(
      TimeOnTimeComparisonHandler timeOnTimeComparisonHandler, EmailConfigurationDTO config,
      DateTime start, final DateTime end, long baselinePeriodMillis,
      TimeGranularity bucketGranularity) throws JobExecutionException {
    start = calculateGraphDataStart(start, end, bucketGranularity);
    try {
      TimeOnTimeComparisonRequest comparisonRequest = new TimeOnTimeComparisonRequest();
      comparisonRequest.setCollectionName(config.getCollection());
      comparisonRequest.setBaselineStart(start.minus(baselinePeriodMillis));
      comparisonRequest.setBaselineEnd(end.minus(baselinePeriodMillis));
      comparisonRequest.setCurrentStart(start);
      comparisonRequest.setCurrentEnd(end);
      comparisonRequest.setEndDateInclusive(true);

      List<MetricExpression> metricExpressions = new ArrayList<>();
      MetricExpression expression = new MetricExpression(config.getMetric());
      metricExpressions.add(expression);
      comparisonRequest.setMetricExpressions(metricExpressions);
      comparisonRequest.setAggregationTimeGranularity(bucketGranularity);
      LOG.debug("Starting...");
      TimeOnTimeComparisonResponse response = timeOnTimeComparisonHandler.handle(comparisonRequest);
      LOG.debug("Done!");
      return response;
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  public static DateTime calculateGraphDataStart(DateTime start, DateTime end,
      TimeGranularity bucketGranularity) {
    TimeUnit unit = bucketGranularity.getUnit();
    long minUnits;
    if (TimeUnit.DAYS.equals(unit)) {
      minUnits = MINIMUM_GRAPH_WINDOW_DAYS;
    } else if (TimeUnit.HOURS.equals(unit)) {
      minUnits = MINIMUM_GRAPH_WINDOW_HOURS;
    } else {
      // no need to do calculation, return input start;
      return start;
    }
    long currentUnits = unit.convert(end.getMillis() - start.getMillis(), TimeUnit.MILLISECONDS);
    if (currentUnits < minUnits) {
      LOG.info("Overriding config window size {} {} with minimum default of {}", currentUnits, unit,
          minUnits);
      start = end.minus(unit.toMillis(minUnits));
    }
    return start;
  }

  public static void sendEmailWithTextBody(HtmlEmail email, SmtpConfiguration smtpConfigutation, String subject,
      String textBody, String fromAddress, String toAddress) throws EmailException {
    email.setTextMsg(textBody);
    sendEmail(smtpConfigutation, email, subject, fromAddress, toAddress);
  }

  public static void sendEmailWithHtml(HtmlEmail email, SmtpConfiguration smtpConfiguration, String subject,
      String htmlBody, String fromAddress, String toAddress) throws EmailException {
    email.setHtmlMsg(htmlBody);
    sendEmail(smtpConfiguration, email, subject, fromAddress, toAddress);
  }

  /** Sends email according to the provided config. */
  private static void sendEmail(SmtpConfiguration config, HtmlEmail email, String subject,
      String fromAddress, String toAddress) throws EmailException {
    if (config != null) {
      email.setSubject(subject);
      LOG.info("Sending email to {}", toAddress);
      email.setHostName(config.getSmtpHost());
      email.setSmtpPort(config.getSmtpPort());
      if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
        email.setAuthenticator(
            new DefaultAuthenticator(config.getSmtpUser(), config.getSmtpPassword()));
        email.setSSLOnConnect(true);
      }
      email.setFrom(fromAddress);
      for (String address : toAddress.split(EMAIL_ADDRESS_SEPARATOR)) {
        email.addTo(address);
      }
      email.send();
      LOG.info("Email sent with subject [{}] to address [{}]!", subject, toAddress);
    } else {
      LOG.error("No email config provided for email with subject [{}]!", subject);
    }
  }

  public static ContributorViewResponse getContributorData(String collection, String metric, List<String> dimensions)
      throws Exception {

    ContributorViewRequest request = new ContributorViewRequest();
    request.setCollection(collection);

    List<MetricExpression> metricExpressions =
        Utils.convertToMetricExpressions(metric, MetricAggFunction.SUM, collection);
    request.setMetricExpressions(metricExpressions);
    long currentEnd = System.currentTimeMillis();
    long maxDataTime = collectionMaxDataTimeCache.get(collection);
    if (currentEnd > maxDataTime) {
      long delta = currentEnd - maxDataTime;
      currentEnd = currentEnd - delta;
    }

    // align to nearest hour
    currentEnd = currentEnd - (currentEnd % HOUR_MILLIS);

    String aggTimeGranularity = "HOURS";
    long currentStart = currentEnd - DAY_MILLIS;

    DatasetConfigDTO datasetConfigDTO = datasetConfigManager.findByDataset(collection);
    if (datasetConfigDTO.getTimeUnit().toString().equals("DAYS")) {
      aggTimeGranularity = datasetConfigDTO.getTimeUnit().toString();
      currentEnd = currentEnd - (currentEnd % DAY_MILLIS);
      currentStart = currentEnd - WEEK_MILLIS;
    }

    long baselineStart = currentStart - WEEK_MILLIS ;
    long baselineEnd = currentEnd - WEEK_MILLIS;

    String timeZone = datasetConfigDTO.getTimezone();
    request.setBaselineStart(new DateTime(baselineStart, DateTimeZone.forID(timeZone)));
    request.setBaselineEnd(new DateTime(baselineEnd, DateTimeZone.forID(timeZone)));
    request.setCurrentStart(new DateTime(currentStart, DateTimeZone.forID(timeZone)));
    request.setCurrentEnd(new DateTime(currentEnd, DateTimeZone.forID(timeZone)));
    request.setTimeGranularity(Utils.getAggregationTimeGranularity(aggTimeGranularity, collection));
    request.setGroupByDimensions(dimensions);
    ContributorViewHandler handler = new ContributorViewHandler(queryCache);
    ContributorViewResponse response = handler.process(request);
    return response;
  }

  /**
   *
   * @param results
   * @return
   */
  public static List<MergedAnomalyResultDTO> applyFiltrationRule(List<MergedAnomalyResultDTO> results) {
    if (results.size() == 0) {
      return results;
    }
    // Function ID to Alert Filter
    Map<Long, AlertFilter> functionAlertFilter = new HashMap<>();

    List<MergedAnomalyResultDTO> qualifiedAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO result : results) {
      // Lazy initiates alert filter for anomalies of the same anomaly function
      AnomalyFunctionBean anomalyFunctionSpec = result.getFunction();
      long functionId = anomalyFunctionSpec.getId();
      AlertFilter alertFilter = functionAlertFilter.get(functionId);
      if (alertFilter == null) {
        // Get filtration rule from anomaly function configuration
        alertFilter = initiateAlertFilter(anomalyFunctionSpec);
        functionAlertFilter.put(functionId, alertFilter);
        LOG.info("Using filter {} for anomaly function {} (dataset: {}, metric: {})", alertFilter,
            functionId, anomalyFunctionSpec.getCollection(), anomalyFunctionSpec.getMetric());
      }
      if (alertFilter.isQualified(result)) {
        qualifiedAnomalies.add(result);
      }
    }
    LOG.info(
        "Found [{}] anomalies qualified to alert after applying filtration rule on [{}] anomalies",
        qualifiedAnomalies.size(), results.size());
    return qualifiedAnomalies;
  }

  /**
   * Initiates an alert filter for the given anomaly function.
   *
   * @param anomalyFunctionSpec the anomaly function that contains the alert filter spec.
   *
   * @return the alert filter specified by the alert filter spec or a dummy filter if the function
   * does not have an alert filter spec or this method fails to initiates an alert filter from the
   * spec.
   */
  static AlertFilter initiateAlertFilter(AnomalyFunctionBean anomalyFunctionSpec) {
    Map<String, String> alertFilterInfo = anomalyFunctionSpec.getAlertFilter();
    if (alertFilterInfo == null) {
      alertFilterInfo = Collections.emptyMap();
    }
    AlertFilter alertFilter;
    if (alertFilterInfo.containsKey(FILTER_TYPE_KEY)) {
      AlertFilterType type = AlertFilterType.valueOf(alertFilterInfo.get(FILTER_TYPE_KEY).toUpperCase());
      alertFilter = AlertFilterHelper.getAlertFilter(type);
      alertFilter.setParameters(alertFilterInfo);
    } else {
      // Every anomaly triggers an alert by default
      alertFilter = AlertFilterHelper.getAlertFilter(AlertFilterType.DUMMY);
    }
    return alertFilter;
  }
}
