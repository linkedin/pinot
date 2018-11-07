/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.detection.components;

import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detection.wrapper.DetectionUtils;
import com.linkedin.thirdeye.detection.spec.ThresholdRuleDetectorSpec;
import com.linkedin.thirdeye.detection.spi.components.AnomalyDetector;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.Collections;
import java.util.List;
import org.joda.time.Interval;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


public class ThresholdRuleDetector implements AnomalyDetector<ThresholdRuleDetectorSpec> {
  private final String COL_TOO_HIGH = "tooHigh";
  private final String COL_TOO_LOW = "tooLow";
  private final String COL_ANOMALY = "anomaly";

  private double min;
  private double max;
  private MetricSlice slice;
  private Long configId;
  private Long endTime;
  private MetricEntity me;
  @Override
  public InputDataSpec getInputDataSpec(Interval interval, String metricUrn, long configId) {
    this.me = MetricEntity.fromURN(metricUrn);
    this.configId = configId;
    this.endTime = interval.getEndMillis();
    this.slice = MetricSlice.from(me.getId(), interval.getStartMillis(), endTime, me.getFilters());

    return new InputDataSpec()
        .withTimeseriesSlices(Collections.singletonList(this.slice));
  }

  @Override
  public List<MergedAnomalyResultDTO> runDetection(InputData data) {
    DataFrame df = data.getTimeseries().get(this.slice);

    // defaults
    df.addSeries(COL_TOO_HIGH, BooleanSeries.fillValues(df.size(), false));
    df.addSeries(COL_TOO_LOW, BooleanSeries.fillValues(df.size(), false));

    // max
    if (!Double.isNaN(this.max)) {
      df.addSeries(COL_TOO_HIGH, df.getDoubles(COL_VALUE).gt(this.max));
    }

    // min
    if (!Double.isNaN(this.min)) {
      df.addSeries(COL_TOO_LOW, df.getDoubles(COL_VALUE).lt(this.min));
    }

    df.mapInPlace(BooleanSeries.HAS_TRUE, COL_ANOMALY, COL_TOO_HIGH, COL_TOO_LOW);

    return DetectionUtils.makeAnomalies(this.slice, df, COL_ANOMALY, this.endTime);
  }

  @Override
  public void init(ThresholdRuleDetectorSpec spec) {
    this.min = spec.getMin();
    this.max = spec.getMax();
  }





}
