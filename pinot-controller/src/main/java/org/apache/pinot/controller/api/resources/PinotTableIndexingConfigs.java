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
package org.apache.pinot.controller.api.resources;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.TABLE_TAG)
@Path("/")
public class PinotTableIndexingConfigs {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableIndexingConfigs.class);

  @Inject
  PinotHelixResourceManager pinotHelixResourceManager;

  @Deprecated
  @PUT
  @Path("/tables/{tableName}/indexingConfigs")
  @ApiOperation(value = "Update table indexing configuration")
  @Produces(MediaType.APPLICATION_JSON)
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 404, message = "Table not found"), @ApiResponse(code = 500, message = "Server error updating configuration")})
  public SuccessResponse updateIndexingConfig(
      @ApiParam(value = "Table name (without type)", required = true) @PathParam("tableName") String tableName,
      String body) {
    try {
      TableConfig tableConfig = TableConfig.fromJsonString(body);
      pinotHelixResourceManager.updateIndexingConfigFor(tableConfig.getTableName(), tableConfig.getTableType(),
          tableConfig.getIndexingConfig());
      return new SuccessResponse("Updated indexing config for table " + tableName);
    } catch (IOException e) {
      String errStr = "Error converting request to table config for table: " + tableName;
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST, e);
    } catch (Exception e) {
      String errStr = "Failed to update indexing config for table: " + tableName;
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
  }
}
