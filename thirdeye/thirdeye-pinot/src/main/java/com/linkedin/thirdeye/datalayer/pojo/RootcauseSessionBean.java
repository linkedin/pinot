package com.linkedin.thirdeye.datalayer.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Objects;
import java.util.Set;


/**
 * RootcauseSessionBean holds information for stored rootcause investigation reports. Supports backpointers to previous
 * versions.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class RootcauseSessionBean extends AbstractBean {
  private String name;
  private String text;
  private String owner;
  private String compareMode;
  private String granularity;
  private Long previousId;
  private Long anomalyRangeStart;
  private Long anomalyRangeEnd;
  private Long analysisRangeStart;
  private Long analysisRangeEnd;
  private Long created;
  private Long sessionVersion;
  private Set<String> contextUrns;
  private Set<String> selectedUrns;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getText() {
    return text;
  }

  public void setText(String text) {
    this.text = text;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public String getCompareMode() {
    return compareMode;
  }

  public void setCompareMode(String compareMode) {
    this.compareMode = compareMode;
  }

  public String getGranularity() {
    return granularity;
  }

  public void setGranularity(String granularity) {
    this.granularity = granularity;
  }

  public Long getPreviousId() {
    return previousId;
  }

  public void setPreviousId(Long previousId) {
    this.previousId = previousId;
  }

  public Long getAnomalyRangeStart() {
    return anomalyRangeStart;
  }

  public void setAnomalyRangeStart(Long anomalyRangeStart) {
    this.anomalyRangeStart = anomalyRangeStart;
  }

  public Long getAnomalyRangeEnd() {
    return anomalyRangeEnd;
  }

  public void setAnomalyRangeEnd(Long anomalyRangeEnd) {
    this.anomalyRangeEnd = anomalyRangeEnd;
  }

  public Long getAnalysisRangeStart() {
    return analysisRangeStart;
  }

  public void setAnalysisRangeStart(Long analysisRangeStart) {
    this.analysisRangeStart = analysisRangeStart;
  }

  public Long getAnalysisRangeEnd() {
    return analysisRangeEnd;
  }

  public void setAnalysisRangeEnd(Long analysisRangeEnd) {
    this.analysisRangeEnd = analysisRangeEnd;
  }

  public Long getCreated() {
    return created;
  }

  public void setCreated(Long created) {
    this.created = created;
  }

  public Long getSessionVersion() {
    return sessionVersion;
  }

  public void setSessionVersion(Long sessionVersion) {
    this.sessionVersion = sessionVersion;
  }

  public Set<String> getContextUrns() {
    return contextUrns;
  }

  public void setContextUrns(Set<String> contextUrns) {
    this.contextUrns = contextUrns;
  }

  public Set<String> getSelectedUrns() {
    return selectedUrns;
  }

  public void setSelectedUrns(Set<String> selectedUrns) {
    this.selectedUrns = selectedUrns;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RootcauseSessionBean)) {
      return false;
    }
    RootcauseSessionBean that = (RootcauseSessionBean) o;
    return Objects.equals(name, that.name) && Objects.equals(text, that.text) && Objects.equals(owner, that.owner)
        && Objects.equals(compareMode, that.compareMode) && Objects.equals(granularity, that.granularity)
        && Objects.equals(previousId, that.previousId) && Objects.equals(anomalyRangeStart, that.anomalyRangeStart)
        && Objects.equals(anomalyRangeEnd, that.anomalyRangeEnd) && Objects.equals(analysisRangeStart,
        that.analysisRangeStart) && Objects.equals(analysisRangeEnd, that.analysisRangeEnd) && Objects.equals(created,
        that.created) && Objects.equals(sessionVersion, that.sessionVersion) && Objects.equals(contextUrns,
        that.contextUrns) && Objects.equals(selectedUrns, that.selectedUrns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, text, owner, compareMode, granularity, previousId, anomalyRangeStart, anomalyRangeEnd,
        analysisRangeStart, analysisRangeEnd, created, sessionVersion, contextUrns, selectedUrns);
  }
}
