/*
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

package org.apache.pinot.thirdeye.detection.alert;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.constant.AnomalyResultSource;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.DataProvider;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.MapUtils;

import static org.apache.pinot.thirdeye.detection.alert.scheme.DetectionEmailAlerter.*;


public abstract class StatefulDetectionAlertFilter extends DetectionAlertFilter {

  public static final String PROP_TO = "to";
  public static final String PROP_CC = "cc";
  public static final String PROP_BCC = "bcc";
  public static final String PROP_RECIPIENTS = "recipients";

  public StatefulDetectionAlertFilter(DataProvider provider, DetectionAlertConfigDTO config, long endTime) {
    super(provider, config, endTime);
  }

  @Override
  public DetectionAlertFilterResult run() throws Exception {
    return this.run(this.config.getVectorClocks(), this.getAnomalyMinId());
  }

  protected abstract DetectionAlertFilterResult run(Map<Long, Long> vectorClocks, long highWaterMark);

  protected final Set<MergedAnomalyResultDTO> filter(Map<Long, Long> vectorClocks, final long minId) {
    // retrieve all candidate anomalies
    Set<MergedAnomalyResultDTO> allAnomalies = new HashSet<>();
    for (Long detectionId : vectorClocks.keySet()) {
      long startTime = vectorClocks.get(detectionId);

      AnomalySlice slice = new AnomalySlice()
          .withDetectionId(detectionId)
          .withStart(startTime)
          .withEnd(this.endTime);
      Collection<MergedAnomalyResultDTO> candidates;
      candidates = this.provider.fetchAnomalies(Collections.singletonList(slice)).get(slice);

      Collection<MergedAnomalyResultDTO> anomalies =
          Collections2.filter(candidates, new Predicate<MergedAnomalyResultDTO>() {
            @Override
            public boolean apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
              return mergedAnomalyResultDTO != null
                  && !mergedAnomalyResultDTO.isChild()
                  && !AlertUtils.hasFeedback(mergedAnomalyResultDTO)
                  && (mergedAnomalyResultDTO.getId() == null || mergedAnomalyResultDTO.getId() >= minId)
                  && mergedAnomalyResultDTO.getAnomalyResultSource().equals(AnomalyResultSource.DEFAULT_ANOMALY_DETECTION);
            }
          });

      allAnomalies.addAll(anomalies);
    }
    return allAnomalies;
  }

  protected final Map<Long, Long> makeVectorClocks(Collection<Long> detectionConfigIds) {
    Map<Long, Long> clocks = new HashMap<>();

    for (Long id : detectionConfigIds) {
      clocks.put(id, MapUtils.getLong(this.config.getVectorClocks(), id, 0L));
    }

    return clocks;
  }

  private long getAnomalyMinId() {
    if (this.config.getHighWaterMark() != null) {
      return this.config.getHighWaterMark();
    }
    return 0;
  }

  protected Set<String> cleanupRecipients(Set<String> recipient) {
    Set<String> filteredRecipients = new HashSet<>();
    if (recipient != null) {
      filteredRecipients.addAll(recipient);
      filteredRecipients = filteredRecipients.stream().map(String::trim).collect(Collectors.toSet());
      filteredRecipients.removeIf(rec -> rec == null || "".equals(rec));
    }
    return filteredRecipients;
  }

  /**
   * Extracts the alert schemes from config and also merges (overrides)
   * recipients explicitly defined outside the scope of alert schemes.
   */
  protected Map<String, Object> generateAlertSchemeProps(DetectionAlertConfigDTO config,
      Set<String> to, Set<String> cc, Set<String> bcc) {
    Map<String, Object> notificationSchemeProps = new HashMap<>();

    // Make a copy of the current alert schemes
    if (config.getAlertSchemes() != null) {
      for (Map.Entry<Object, Object> alertSchemeEntry : ConfigUtils.getMap(config.getAlertSchemes()).entrySet()) {
        notificationSchemeProps.put(alertSchemeEntry.getKey().toString(), ConfigUtils.getMap(alertSchemeEntry.getValue()));
      }
    }

    // Override the email alert scheme
    Map<String, Object> recipients = new HashMap<>();
    recipients.put(PROP_TO, cleanupRecipients(to));
    recipients.put(PROP_CC, cleanupRecipients(cc));
    recipients.put(PROP_BCC, cleanupRecipients(bcc));
    Map<String, Object> recipientsHolder = new HashMap<>();
    recipientsHolder.put(PROP_RECIPIENTS, recipients);
    ((Map<String, Object>) notificationSchemeProps.get(PROP_EMAIL_SCHEME)).putAll(recipientsHolder);

    return notificationSchemeProps;
  }
}
