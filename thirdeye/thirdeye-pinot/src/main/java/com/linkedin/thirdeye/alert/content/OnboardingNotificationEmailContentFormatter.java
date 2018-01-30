package com.linkedin.thirdeye.alert.content;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.dashboard.resources.DetectionJobResource;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OnboardingNotificationEmailContentFormatter extends BaseEmailContentFormatter {
  private static final Logger LOG = LoggerFactory.getLogger(OnboardingNotificationEmailContentFormatter.class);
  public static final String EMAIL_TEMPLATE = "emailTemplate";
  public static final String DEFAULT_EMAIL_TEMPLATE = "onboard-notification-email-template.ftl";
  public static final String DEFAULT_NULL_STRING_VALUE = "N/A";
  public static final String ALERT_FILTER_PATTERN_KEY = DetectionJobResource.AUTOTUNE_PATTERN_KEY;
  public static final String ALERT_CONFIG_NAME = "alertConfigName";
  public static final int DEFAULT_ONBOARDING_REPLAY_DAYS = 30;

  public OnboardingNotificationEmailContentFormatter() {
  }

  @Override
  public void init(Properties properties, EmailContentFormatterConfiguration configuration) {
    super.init(properties, configuration);
    this.emailTemplate = properties.getProperty(EMAIL_TEMPLATE, DEFAULT_EMAIL_TEMPLATE);
  }
  /**
   * The actual function that convert anomalies into parameter map

   * @param templateData
   * @param anomalies
   */
  @Override
  protected void updateTemplateDataByAnomalyResults(Map<String, Object> templateData,
      Collection<AnomalyResult> anomalies, EmailContentFormatterContext context) {
    AnomalyFunctionDTO anomalyFunctionSpec = null;
    for (AnomalyResult anomalyResult : anomalies) {
      if (!(anomalyResult instanceof MergedAnomalyResultDTO)) {
        throw new IllegalArgumentException("Input anomalies should be instance of MergedAnomalyResultDTO");
      }
      if (anomalyFunctionSpec == null) {
        anomalyFunctionSpec = ((MergedAnomalyResultDTO) anomalyResult).getFunction();
      } else if (anomalyFunctionSpec.getId() != ((MergedAnomalyResultDTO) anomalyResult).getFunction().getId()) {
        throw new IllegalArgumentException("Input anomalies should be generated by the same anomaly function");
      }
    }

    // calculate times in between
    int onboardingReplayDays = DEFAULT_ONBOARDING_REPLAY_DAYS;
    if (context.getStart() != null && context.getEnd() != null) {
      onboardingReplayDays = Days.daysBetween(context.getStart(), context.getEnd()).getDays();
    }

    templateData.put("functionName", anomalyFunctionSpec.getFunctionName());
    templateData.put("functionId", anomalyFunctionSpec.getId());
    templateData.put("metrics", anomalyFunctionSpec.getMetric());
    templateData.put("filters", returnValueOrDefault(anomalyFunctionSpec.getFilters(), DEFAULT_NULL_STRING_VALUE));
    templateData.put("dimensionDrillDown", returnValueOrDefault(anomalyFunctionSpec.getExploreDimensions(), DEFAULT_NULL_STRING_VALUE));
    templateData.put("repalyDays", Integer.toString(onboardingReplayDays));
    String alertPattern = DEFAULT_NULL_STRING_VALUE;
    Map<String, String> alertFilter = anomalyFunctionSpec.getAlertFilter();
    if (alertFilter != null && alertFilter.containsKey(ALERT_FILTER_PATTERN_KEY)) {
      alertPattern = alertFilter.get(ALERT_FILTER_PATTERN_KEY);
    }
    AlertConfigDTO alertConfig = null;
    templateData.put("alertPattern", alertPattern);
    if (templateData.containsKey(ALERT_CONFIG_NAME)) {
      String configName = (String) templateData.get(ALERT_CONFIG_NAME);
      alertConfig = DAORegistry.getInstance().getAlertConfigDAO().findWhereNameEquals(configName);
    }
    if (alertConfig == null) {
      alertConfig = new AlertConfigDTO();
    }
    templateData.put("application", returnValueOrDefault(alertConfig.getApplication(), DEFAULT_NULL_STRING_VALUE));
    templateData.put("recipients", returnValueOrDefault(alertConfig.getRecipients(), DEFAULT_NULL_STRING_VALUE));
  }

  private String returnValueOrDefault(String value, String defaultValue) {
    return StringUtils.isEmpty(value) ? defaultValue: value;
  }
}
