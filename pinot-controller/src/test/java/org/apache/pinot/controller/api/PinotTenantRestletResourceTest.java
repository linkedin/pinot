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
package org.apache.pinot.controller.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.controller.ControllerTestUtils.*;
import static org.testng.Assert.*;


public class PinotTenantRestletResourceTest {
  private static final int NUM_BROKER_INSTANCES = 1;
  private static final int NUM_SERVER_INSTANCES = 3;
  private static final String TABLE_NAME = "pinotTenantRestletResourceTestTable_OFFLINE";

  @BeforeClass
  public void setUp()
      throws Exception {
    addFakeBrokerInstancesToAutoJoinHelixCluster(NUM_BROKER_INSTANCES, true);
    addFakeServerInstancesToAutoJoinHelixCluster(NUM_SERVER_INSTANCES, true);
  }

  @Test
  public void testTableListForTenant()
      throws Exception {
    // Get current table count
    String listTablesUrl = getControllerRequestURLBuilder().forTablesFromTenant(TagNameUtils.DEFAULT_TENANT_NAME);
    JsonNode tableList = JsonUtils.stringToJsonNode(sendGetRequest(listTablesUrl));
    int currentTableCount = tableList.get("tables").size();

    // Add a table
    sendPostRequest(getControllerRequestURLBuilder().forTableCreate(),
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build().toJsonString());

    // There should be 1 table on the tenant
    tableList = JsonUtils.stringToJsonNode(sendGetRequest(listTablesUrl));
    JsonNode tables = tableList.get("tables");
    assertEquals(tables.size(), currentTableCount+1);

    // Check to make sure that test table exists.
    boolean found = false;
    for (int i = 0; !found && i < tables.size(); i++) {
      found = tables.get(i).asText().equals(TABLE_NAME);
    }

    assertTrue(found);
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
  }
}
