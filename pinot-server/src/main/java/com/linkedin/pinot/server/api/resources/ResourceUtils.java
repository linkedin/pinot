/**
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
package com.linkedin.pinot.server.api.resources;

import java.io.IOException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Util class for server APIs.
 */
public class ResourceUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUtils.class);

  private ResourceUtils() {
  }

  public static String convertToJsonString(Object object) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
    } catch (IOException e) {
      LOGGER.error("Failed to convert json into string: ", e);
      throw new WebApplicationException("Failed to convert json into string.", Response.Status.INTERNAL_SERVER_ERROR);
    }
  }
}
