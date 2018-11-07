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

package com.linkedin.thirdeye.detection.tune;

import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.spi.model.InputData;
import com.linkedin.thirdeye.detection.spi.model.InputDataSpec;
import com.linkedin.thirdeye.detection.wrapper.DetectionUtils;

/**
 * High level training module interface. Should implement this interface whenever possible.
 */
public abstract class StaticStageTrainingModule implements StageTrainingModule {
  public static final String PROP_YAML_CONFIG = "yamlConfig";
  static final String PROP_CLASS_NAME = "className";

  public TrainingResult fit(DataProvider provider) {
    return this.fit(DetectionUtils.getDataForSpec(provider, this.getInputDataSpec()));
  }

  abstract TrainingResult fit(InputData data);

  abstract InputDataSpec getInputDataSpec();
}
