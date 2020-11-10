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

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.ConvertToRawIndexTask;
import org.apache.pinot.core.common.MinionConstants.MergeRollupTask;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test that extends HybridClusterIntegrationTest and add Minions into the cluster to convert 3 metric
 * columns' index into raw index for OFFLINE segments.
 */
public class MinionTasksIntegrationTest extends HybridClusterIntegrationTest {
  private static final String COLUMNS_TO_CONVERT = "ActualElapsedTime,ArrDelay,DepDelay,CRSDepTime";

  private PinotHelixTaskResourceManager _helixTaskResourceManager;
  private PinotTaskManager _taskManager;

  @Nullable
  @Override
  protected List<String> getNoDictionaryColumns() {
    return null;
  }

  // NOTE: Only allow converting raw index for v1 segment
  @Override
  protected String getSegmentVersion() {
    return SegmentVersion.v1.name();
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    // The parent setUp() sets up Zookeeper, Kafka, controller, broker and servers
    super.setUp();

    startMinion(null, null);
    _helixTaskResourceManager = _controllerStarter.getHelixTaskResourceManager();
    _taskManager = _controllerStarter.getTaskManager();
  }

  @Test(priority = 1)
  public void testConvertToRawIndexTask()
      throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Configure ConvertToRawIndexTask
    TableConfig tableConfig = getOfflineTableConfig();
    Map<String, Map<String, String>> taskConfigs = new HashMap<>();
    Map<String, String> convertToRawIndexTaskConfigs = new HashMap<>();
    convertToRawIndexTaskConfigs.put(MinionConstants.TABLE_MAX_NUM_TASKS_KEY, "5");
    convertToRawIndexTaskConfigs.put(ConvertToRawIndexTask.COLUMNS_TO_CONVERT_KEY, COLUMNS_TO_CONVERT);
    taskConfigs.put(ConvertToRawIndexTask.TASK_TYPE, convertToRawIndexTaskConfigs);
    tableConfig.setTaskConfig(new TableTaskConfig(taskConfigs));
    updateTableConfig(tableConfig);

    File testDataDir = new File(CommonConstants.Server.DEFAULT_INSTANCE_DATA_DIR + "-0", offlineTableName);
    if (!testDataDir.isDirectory()) {
      testDataDir = new File(CommonConstants.Server.DEFAULT_INSTANCE_DATA_DIR + "-1", offlineTableName);
    }
    Assert.assertTrue(testDataDir.isDirectory());
    File tableDataDir = testDataDir;

    // Check that all columns have dictionary
    File[] indexDirs = tableDataDir.listFiles();
    Assert.assertNotNull(indexDirs);
    for (File indexDir : indexDirs) {
      SegmentMetadata segmentMetadata = new SegmentMetadataImpl(indexDir);
      for (String columnName : segmentMetadata.getSchema().getColumnNames()) {
        Assert.assertTrue(segmentMetadata.hasDictionary(columnName));
      }
    }

    // Should create the task queues and generate a ConvertToRawIndexTask task with 5 child tasks
    Assert.assertTrue(_taskManager.scheduleTasks().containsKey(ConvertToRawIndexTask.TASK_TYPE));
    Assert.assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(ConvertToRawIndexTask.TASK_TYPE)));

    // Should generate one more ConvertToRawIndexTask task with 3 child tasks
    Assert.assertTrue(_taskManager.scheduleTasks().containsKey(ConvertToRawIndexTask.TASK_TYPE));

    // Wait at most 600 seconds for all tasks COMPLETED and new segments refreshed
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _helixTaskResourceManager.getTaskStates(ConvertToRawIndexTask.TASK_TYPE).values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }

      // Check segment ZK metadata
      for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : _helixResourceManager
          .getOfflineSegmentMetadata(offlineTableName)) {
        Map<String, String> customMap = offlineSegmentZKMetadata.getCustomMap();
        if (customMap == null || customMap.size() != 1 || !customMap
            .containsKey(ConvertToRawIndexTask.TASK_TYPE + MinionConstants.TASK_TIME_SUFFIX)) {
          return false;
        }
      }

      // Check segment metadata
      File[] indexDirs1 = tableDataDir.listFiles();
      Assert.assertNotNull(indexDirs1);
      for (File indexDir : indexDirs1) {
        SegmentMetadata segmentMetadata;

        // Segment metadata file might not exist if the segment is refreshing
        try {
          segmentMetadata = new SegmentMetadataImpl(indexDir);
        } catch (Exception e) {
          return false;
        }

        // The columns in COLUMNS_TO_CONVERT should have raw index
        List<String> rawIndexColumns = Arrays.asList(StringUtils.split(COLUMNS_TO_CONVERT, ','));
        for (String columnName : segmentMetadata.getSchema().getColumnNames()) {
          if (rawIndexColumns.contains(columnName)) {
            if (segmentMetadata.hasDictionary(columnName)) {
              return false;
            }
          } else {
            if (!segmentMetadata.hasDictionary(columnName)) {
              return false;
            }
          }
        }
      }

      return true;
    }, 600_000L, "Failed to get all tasks COMPLETED and new segments refreshed");
  }

  @Test(priority = 1)
  public void testPinotHelixResourceManagerAPIs() {
    // Instance APIs
    Assert.assertEquals(_helixResourceManager.getAllInstances().size(), 5);
    Assert.assertEquals(_helixResourceManager.getOnlineInstanceList().size(), 5);
    Assert.assertEquals(_helixResourceManager.getOnlineUnTaggedBrokerInstanceList().size(), 0);
    Assert.assertEquals(_helixResourceManager.getOnlineUnTaggedServerInstanceList().size(), 0);

    // Table APIs
    String rawTableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(rawTableName);
    List<String> tableNames = _helixResourceManager.getAllTables();
    Assert.assertEquals(tableNames.size(), 2);
    Assert.assertTrue(tableNames.contains(offlineTableName));
    Assert.assertTrue(tableNames.contains(realtimeTableName));
    Assert.assertEquals(_helixResourceManager.getAllRawTables(), Collections.singletonList(rawTableName));
    Assert.assertEquals(_helixResourceManager.getAllRealtimeTables(), Collections.singletonList(realtimeTableName));

    // Tenant APIs
    Assert.assertEquals(_helixResourceManager.getAllBrokerTenantNames(), Collections.singleton("TestTenant"));
    Assert.assertEquals(_helixResourceManager.getAllServerTenantNames(), Collections.singleton("TestTenant"));
  }

  @Test(priority = 2)
  public void testMergeRollup()
      throws Exception {
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());

    // Configure MergeRollupTask
    TableConfig tableConfig = getOfflineTableConfig();
    Map<String, Map<String, String>> taskConfigs = new HashMap<>();
    Map<String, String> mergeRollupConfigs = new HashMap<>();
    mergeRollupConfigs.put(MinionConstants.TABLE_MAX_NUM_TASKS_KEY, "5");
    mergeRollupConfigs.put(MergeRollupTask.MAX_NUM_SEGMENTS_PER_TASK_KEY, "3");
    taskConfigs.put(MergeRollupTask.TASK_TYPE, mergeRollupConfigs);
    tableConfig.setTaskConfig(new TableTaskConfig(taskConfigs));
    updateTableConfig(tableConfig);

    // Schedule merge task
    Assert.assertTrue(_taskManager.scheduleTasks().containsKey(MinionConstants.MergeRollupTask.TASK_TYPE));
    Assert.assertTrue(_helixTaskResourceManager.getTaskQueues()
        .contains(PinotHelixTaskResourceManager.getHelixJobQueueName(MinionConstants.MergeRollupTask.TASK_TYPE)));

    // Wait at most 600 seconds for all tasks COMPLETED
    waitForMergeTaskToComplete(offlineTableName);

    // Check with the queries
    super.testHardcodedSqlQueries();
    super.testHardcodedQueries();
  }

  private void waitForMergeTaskToComplete(String offlineTableName) {
    TestUtils.waitForCondition(input -> {
      // Check task state
      for (TaskState taskState : _helixTaskResourceManager.getTaskStates(MinionConstants.MergeRollupTask.TASK_TYPE)
          .values()) {
        if (taskState != TaskState.COMPLETED) {
          return false;
        }
      }

      // Check segment ZK metadata
      SegmentLineage segmentLineage = _taskManager.getClusterInfoAccessor().getSegmentLineage(offlineTableName);
      for (String entryId : segmentLineage.getLineageEntryIds()) {
        LineageEntry lineageEntry = segmentLineage.getLineageEntry(entryId);
        if (lineageEntry.getState() != LineageEntryState.COMPLETED) {
          return false;
        }
      }
      return true;
    }, 600_000L, "Failed to get all tasks COMPLETED and new segments refreshed");
  }

  @Test(enabled = false)
  public void testSegmentListApi() {
  }

  @Test(enabled = false)
  public void testBrokerDebugOutput() {
  }

  @Test(enabled = false)
  public void testBrokerDebugRoutingTableSQL() {
  }

  @Test(enabled = false)
  public void testBrokerResponseMetadata() {
  }

  @Test(enabled = false)
  public void testDictionaryBasedQueries() {
  }

  @Test(enabled = false)
  public void testGeneratedQueriesWithMultiValues() {
  }

  @Test(enabled = false)
  public void testGeneratedQueriesWithoutMultiValues() {
  }

  @Test(enabled = false)
  public void testHardcodedQueries() {
  }

  @Test(enabled = false)
  public void testHardcodedSqlQueries() {
  }

  @Test(enabled = false)
  public void testInstanceShutdown() {
  }

  @Test(enabled = false)
  public void testQueriesFromQueryFile() {
  }

  @Test(enabled = false)
  public void testQueryExceptions() {
  }

  @Test(enabled = false)
  public void testReload() {
  }

  @Test(enabled = false)
  public void testSqlQueriesFromQueryFile() {
  }

  @Test(enabled = false)
  public void testVirtualColumnQueries() {
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopMinion();

    super.tearDown();
  }
}
