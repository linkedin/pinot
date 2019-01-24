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
package org.apache.pinot.integration.tests;

import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.broker.broker.BrokerAdminApiApplication;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests that the controller, broker and server admin consoles return the expected pages.
 */
public class AdminConsoleIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdminConsoleIntegrationTest.class);

  @BeforeClass
  public void setUp() throws Exception {
    // Start an empty Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();
  }

  /**
   * Tests resposnes to /api and /help.
   */
  @Test
  public void testApiHelp() throws Exception {
    // test controller
    String response = sendGetRequest(_controllerBaseApiUrl + "/help");
    String expected =
        IOUtils.toString(ControllerAdminApiApplication.class.getClassLoader().getResourceAsStream("api/index.html"),
            "UTF-8").replace("\n", "");
    Assert.assertEquals(response, expected);
    // help and api map to the same content
    response = sendGetRequest(_controllerBaseApiUrl + "/api");
    Assert.assertEquals(response, expected);


    // test broker
    response = sendGetRequest(_brokerBaseApiUrl + "/help");
    expected =
        IOUtils.toString(BrokerAdminApiApplication.class.getClassLoader().getResourceAsStream("api/index.html"),
            "UTF-8").replace("\n", "");
    Assert.assertEquals(response, expected);
    // help and api map to the same content
    response = sendGetRequest(_brokerBaseApiUrl + "/api");
    Assert.assertEquals(response, expected);

    String serverBaseApiUrl = "http://localhost:" + CommonConstants.Server.DEFAULT_ADMIN_API_PORT;
    // test server
    response = sendGetRequest( serverBaseApiUrl + "/help");
    expected =
        IOUtils.toString(BrokerAdminApiApplication.class.getClassLoader().getResourceAsStream("api/index.html"),
            "UTF-8").replace("\n", "");
    Assert.assertEquals(response, expected);

    // help and api map to the same content
    response = sendGetRequest(serverBaseApiUrl + "/api");
    Assert.assertEquals(response, expected);
  }

  @Test
  public void testQuery() throws Exception {
    // test controller query console
    String response = sendGetRequest(_controllerBaseApiUrl + "/query");
    String expected =
        IOUtils.toString(ControllerAdminApiApplication.class.getClassLoader().getResourceAsStream("static/query/index.html"),
            "UTF-8").replace("\n", "");
    Assert.assertEquals(response, expected);
  }
}


