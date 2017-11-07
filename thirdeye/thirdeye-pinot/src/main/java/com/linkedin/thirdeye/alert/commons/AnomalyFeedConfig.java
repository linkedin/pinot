package com.linkedin.thirdeye.alert.commons;

import com.linkedin.thirdeye.alert.fetcher.BaseAnomalyFetcher;
import com.linkedin.thirdeye.datalayer.util.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class AnomalyFeedConfig {
  private String anomalyFeedType;
  private AnomalySource anomalySourceType;
  private String anomalySource;
  private Long alertSnapshotId;
  private List<AnomalyFetcherConfig> anomalyFetcherConfigs;
  private List<Map<String, String>> alertFilterConfigs;

  public String getAnomalyFeedType() {
    return anomalyFeedType;
  }

  public void setAnomalyFeedType(String anomalyFeedType) {
    this.anomalyFeedType = anomalyFeedType;
  }

  public AnomalySource getAnomalySourceType() {
    return anomalySourceType;
  }

  public void setAnomalySourceType(AnomalySource anomalySourceType) {
    this.anomalySourceType = anomalySourceType;
  }

  public String getAnomalySource() {
    return anomalySource;
  }

  public void setAnomalySource(String anomalySource) {
    this.anomalySource = anomalySource;
  }

  public List<AnomalyFetcherConfig> getAnomalyFetcherConfigs() {
    if (anomalyFetcherConfigs == null) {
      anomalyFetcherConfigs = Collections.EMPTY_LIST;
    }
    for (AnomalyFetcherConfig anomalyFetcherConfig : anomalyFetcherConfigs) {
      Properties properties = StringUtils.decodeCompactedProperties(anomalyFetcherConfig.getProperties());
      anomalyFetcherConfig.setAnomalySourceType(anomalySourceType);
      anomalyFetcherConfig.setAnomalySource(anomalySource);
      anomalyFetcherConfig.setProperties(StringUtils.encodeCompactedProperties(properties));
    }
    return anomalyFetcherConfigs;
  }

  public void setAnomalyFetcherConfigs(List<AnomalyFetcherConfig> anomalyFetcherConfigs) {
    this.anomalyFetcherConfigs = anomalyFetcherConfigs;
  }

  public List<Map<String, String>> getAlertFilterConfigs() {
    if (alertFilterConfigs == null) {
      alertFilterConfigs = Collections.EMPTY_LIST;
    }
    return alertFilterConfigs;
  }

  public void setAlertFilterConfigs(List<Map<String, String>> alertFilterConfigs) {
    this.alertFilterConfigs = alertFilterConfigs;
  }

  public Long getAlertSnapshotId() {
    return alertSnapshotId;
  }

  public void setAlertSnapshotId(Long alertSnapshotId) {
    this.alertSnapshotId = alertSnapshotId;
  }
}
