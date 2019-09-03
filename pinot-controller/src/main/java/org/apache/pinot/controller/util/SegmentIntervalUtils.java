/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.utils.time.TimeUtils;
import org.joda.time.Duration;
import org.joda.time.Interval;


/**
 * Helper methods for segment interval validations
 */
public class SegmentIntervalUtils {

  /**
   * Checks if the given segment metadata time interval is valid
   */
  public static boolean isValidInterval(Interval timeInterval) {
    return timeInterval != null && TimeUtils.timeValueInValidRange(timeInterval.getStartMillis()) && TimeUtils
        .timeValueInValidRange(timeInterval.getEndMillis());
  }

  /**
   * We only want to check missing segments if the table has at least 2 segments and a time column
   */
  public static boolean eligibleForMissingSegmentCheck(int numSegments,
      SegmentsValidationAndRetentionConfig validationConfig) {
    return numSegments >= 2 && StringUtils.isNotEmpty(validationConfig.getTimeColumnName());
  }

  /**
   * We only want to check intervals if the table has a time column
   */
  public static boolean eligibleForSegmentIntervalCheck(SegmentsValidationAndRetentionConfig validationConfig) {
    return StringUtils.isNotEmpty(validationConfig.getTimeColumnName());
  }

  /**
   * Converts push frequency into duration. For invalid or less than 'hourly' push frequency, treats it as 'daily'.
   */
  public static Duration convertToDuration(String pushFrequency) {
    if ("hourly".equalsIgnoreCase(pushFrequency)) {
      return Duration.standardHours(1L);
    }
    if ("daily".equalsIgnoreCase(pushFrequency)) {
      return Duration.standardDays(1L);
    }
    if ("weekly".equalsIgnoreCase(pushFrequency)) {
      return Duration.standardDays(7L);
    }
    if ("monthly".equalsIgnoreCase(pushFrequency)) {
      return Duration.standardDays(30L);
    }
    return Duration.standardDays(1L);
  }
}
