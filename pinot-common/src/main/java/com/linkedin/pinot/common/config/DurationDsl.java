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

package com.linkedin.pinot.common.config;

import java.util.concurrent.TimeUnit;


/**
 * DSL for durations, which turns "5 days" into a duration.
 */
public class DurationDsl implements SingleKeyDsl<Duration> {
  @Override
  public Duration parse(String text) {
    if (text == null || text.isEmpty()) {
      return null;
    }

    String[] parts = text.split(" ");
    final String unit = parts[1].toUpperCase();
    final String unitCount = parts[0];
    return new Duration(TimeUnit.valueOf(unit), Integer.parseInt(unitCount));
  }

  @Override
  public String unparse(Duration value) {
    return value.getUnitCount() + " " + value.getUnit();
  }
}
