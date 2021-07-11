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
package org.apache.pinot.tools.utils;

import com.google.common.base.Joiner;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.utils.JsonUtils;

public class OutputFormatUtils {
  private static final Joiner CSV_JOINER = Joiner.on(',').skipNulls();
  private enum OutputFormat {
    JSON, CSV
  }

  private OutputFormatUtils() {
  }

  private static String convertResultTableToCsv(ResultTable resultTable) {
    StringBuilder sb = new StringBuilder();
    sb.append(CSV_JOINER.join(resultTable.getDataSchema().getColumnNames()));
    sb.append("\n");
    for (Object[] rows : resultTable.getRows()) {
      sb.append(CSV_JOINER.join(rows));
      sb.append("\n");
    }
    return sb.toString();
  }

  public static void saveResponse(
      BrokerResponseNative brokerResponse,
      String outputPath,
      String outputFormat) throws IOException, IllegalArgumentException {
    OutputFormat format = outputFormat == null ? OutputFormat.JSON :
        OutputFormat.valueOf(outputFormat.toUpperCase());
    String outputContent;
    switch (format) {
      case CSV: outputContent = convertResultTableToCsv(brokerResponse.getResultTable()); break;
      case JSON:
      default: outputContent = JsonUtils.objectToString(brokerResponse.getResultTable()); break;
    }
    Files.write(new File(outputPath).toPath(), outputContent.getBytes(StandardCharsets.UTF_8));
  }
}