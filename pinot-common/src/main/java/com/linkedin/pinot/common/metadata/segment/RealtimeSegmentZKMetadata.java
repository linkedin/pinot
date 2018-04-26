/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metadata.segment;

import java.util.Map;

import org.apache.helix.ZNRecord;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import static com.linkedin.pinot.common.utils.EqualityUtils.isEqual;
import static com.linkedin.pinot.common.utils.EqualityUtils.hashCodeOf;
import static com.linkedin.pinot.common.utils.EqualityUtils.isSameReference;
import static com.linkedin.pinot.common.utils.EqualityUtils.isNullOrNotSameClass;


public class RealtimeSegmentZKMetadata extends SegmentZKMetadata {

  private static final PeriodFormatter PERIOD_FORMATTER = new PeriodFormatterBuilder()
      .appendHours().appendSuffix("h")
      .appendMinutes().appendSuffix("m")
      .appendSeconds().appendSuffix("s")
      .toFormatter();

  private Status _status = null;
  private int _sizeThresholdToFlushSegment = -1;
  private String _timeThresholdToFlushSegment = null; // store as period string for readability

  public RealtimeSegmentZKMetadata() {
    setSegmentType(SegmentType.REALTIME);
  }

  public RealtimeSegmentZKMetadata(ZNRecord znRecord) {
    super(znRecord);
    setSegmentType(SegmentType.REALTIME);
    _status = Status.valueOf(znRecord.getSimpleField(CommonConstants.Segment.Realtime.STATUS));
    _sizeThresholdToFlushSegment = znRecord.getIntField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, -1);
    String flushThresholdTime = znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_TIME);
    if (flushThresholdTime != null && !flushThresholdTime.equals(NULL)) {
      _timeThresholdToFlushSegment = znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_TIME);
    }

  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    String newline = "\n";
    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newline);
    result.append("  " + super.getClass().getName() + " : " + super.toString());
    result.append(newline);
    result.append("  " + CommonConstants.Segment.Realtime.STATUS + " : " + _status);
    result.append(newline);
    result.append("}");
    return result.toString();
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = super.toZNRecord();
    znRecord.setSimpleField(CommonConstants.Segment.Realtime.STATUS, _status.toString());
    znRecord.setLongField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, _sizeThresholdToFlushSegment);
    znRecord.setSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_TIME, _timeThresholdToFlushSegment);
    return znRecord;
  }

  @Override
  public boolean equals(Object segmentMetadata) {
    if (isSameReference(this, segmentMetadata)) {
      return true;
    }

    if (isNullOrNotSameClass(this, segmentMetadata)) {
      return false;
    }

    RealtimeSegmentZKMetadata metadata = (RealtimeSegmentZKMetadata) segmentMetadata;
    return super.equals(metadata) &&
        isEqual(_status, metadata._status) &&
        isEqual(_sizeThresholdToFlushSegment, metadata._sizeThresholdToFlushSegment) &&
        isEqual(_timeThresholdToFlushSegment, metadata._timeThresholdToFlushSegment);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = hashCodeOf(result, _status);
    result = hashCodeOf(result, _sizeThresholdToFlushSegment);
    result = hashCodeOf(result, _timeThresholdToFlushSegment);
    return result;
  }

  @Override
  public Map<String, String> toMap() {
    Map<String, String> configMap = super.toMap();
    configMap.put(CommonConstants.Segment.Realtime.STATUS, _status.toString());
    configMap.put(CommonConstants.Segment.SEGMENT_TYPE, SegmentType.REALTIME.toString());
    configMap.put(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE, Integer.toString(_sizeThresholdToFlushSegment));
    configMap.put(CommonConstants.Segment.FLUSH_THRESHOLD_TIME, _timeThresholdToFlushSegment);
    return configMap;
  }

  public Status getStatus() {
    return _status;
  }

  public void setStatus(Status status) {
    _status = status;
  }

  public void setSizeThresholdToFlushSegment(int sizeThresholdToFlushSegment) {
    _sizeThresholdToFlushSegment = sizeThresholdToFlushSegment;
  }

  public int getSizeThresholdToFlushSegment() {
    return _sizeThresholdToFlushSegment;
  }

  /**
   * Converts the period string to millis before returning
   * @return time threshold value in millis
   */
  public Long getTimeThresholdToFlushSegmentMillis() {
    return convertToMillis(_timeThresholdToFlushSegment);
  }

  /**
   * Converts the millis value of threshold into a readable period string before setting
   * @param timeThresholdToFlushSegmentMillis
   */
  public void setTimeThresholdToFlushSegmentMillis(Long timeThresholdToFlushSegmentMillis) {
    _timeThresholdToFlushSegment = convertToPeriod(timeThresholdToFlushSegmentMillis);
  }

  private Long convertToMillis(String timeStr) {
    if (timeStr == null) {
      return null;
    }
    try {
      Period p = PERIOD_FORMATTER.parsePeriod(timeStr);
      return p.toStandardDuration().getStandardSeconds() * 1000L;
    } catch (Exception e) {
      throw new RuntimeException("Invalid time spec '" + timeStr + "' (Valid examples: '3h', '4h30m')", e);
    }
  }

  private String convertToPeriod(Long millis) {
    Period p = new Period(new Duration(millis));
    return PERIOD_FORMATTER.print(p);
  }
}
