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
package com.linkedin.pinot.controller.helix.core.relocation;

import com.google.common.base.Function;
import com.google.common.collect.MinMaxPriorityQueue;
import com.linkedin.pinot.common.config.RealtimeTagConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manager to relocate completed segments from consuming servers to completed servers
 * Segment relocation will be done by this manager, instead of directly moving segments to completed servers on completion,
 * so that we don't get segment downtime when a segment is in transition
 *
 * We only relocate segments for realtime tables, and only if tenant config indicates that relocation is required
 * A segment will be relocated, one replica at a time, once all of its replicas are in ONLINE state and on consuming servers
 */
public class RealtimeSegmentRelocationManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentRelocationManager.class);
  private final ScheduledExecutorService _executorService;
  private final PinotHelixResourceManager _pinotHelixResourceManager;
  private final HelixManager _helixManager;
  private final HelixAdmin _helixAdmin;
  private final long _runFrequencyMinutes;

  public RealtimeSegmentRelocationManager(PinotHelixResourceManager pinotHelixResourceManager, ControllerConf config) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
    _helixManager = pinotHelixResourceManager.getHelixZkManager();
    _helixAdmin = pinotHelixResourceManager.getHelixAdmin();
    _runFrequencyMinutes = config.getRelocationManagerFrequencyInMinutes();

    _executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName("PinotRelocationManagerExecutorService");
        return thread;
      }
    });
  }

  public void start() {
    LOGGER.info("Starting relocation manager");

    _executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          runRelocation();
        } catch (Exception e) {
          LOGGER.warn("Caught exception while running relocation manager", e);
        }
      }
    }, 5, _runFrequencyMinutes, TimeUnit.MINUTES);
  }

  public void stop() {
    _executorService.shutdown();
  }

  /**
   * Check all tables. Perform relocation of segments if table is realtime and relocation is required
   */
  public void runRelocation() {
    if (!_pinotHelixResourceManager.isLeader()) {
      LOGGER.info("Skipping realtime segment relocation, not leader!");
      return;
    }

    LOGGER.info("Starting relocation of realtime segments");
    List<String> allTableNames = _pinotHelixResourceManager.getAllTables();

    for (String tableNameWithType : allTableNames) {

      TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
      if (tableType.equals(TableType.REALTIME)) {
        LOGGER.info("Starting relocation of segments for table: {}", tableNameWithType);

        TableConfig tableConfig = _pinotHelixResourceManager.getRealtimeTableConfig(tableNameWithType);

        RealtimeTagConfig realtimeTagConfig = new RealtimeTagConfig(tableConfig, _helixManager);
        if (!realtimeTagConfig.isRelocateCompletedSegments()) {
          LOGGER.info("Skipping relocation of segments for {}", tableNameWithType);
          return;
        }

        IdealState idealState = HelixHelper.getTableIdealState(_helixManager, tableNameWithType);
        if (!idealState.isEnabled()) {
          LOGGER.info("Skipping relocation of segments for {} since ideal state is disabled", tableNameWithType);
          return;
        }

        Map<String, Map<String, String>> segmentNameToInstancesMap = relocateSegments(realtimeTagConfig, idealState);
        if (!segmentNameToInstancesMap.isEmpty()) {
          updateIdealState(tableNameWithType, segmentNameToInstancesMap);
        }
      }
    }
    LOGGER.info("Realtime segment relocation completed");
  }

  /**
   * Given a realtime tag config and an ideal state, relocate the segments
   * which are completed but still hanging around on consuming servers, one replica at a time
   * @param  realtimeTagConfig
   * @param idealState
   */
  protected Map<String, Map<String, String>> relocateSegments(RealtimeTagConfig realtimeTagConfig,
      IdealState idealState) {
    Map<String, Map<String, String>> segmentNameToInstancesMap;

    List<String> consumingServers = _helixAdmin.getInstancesInClusterWithTag(_helixManager.getClusterName(),
        realtimeTagConfig.getConsumingServerTag());
    if (consumingServers.isEmpty()) {
      throw new IllegalStateException(
          "Found no realtime consuming servers with tag " + realtimeTagConfig.getConsumingServerTag());
    }
    List<String> completedServers = _helixAdmin.getInstancesInClusterWithTag(_helixManager.getClusterName(),
        realtimeTagConfig.getCompletedServerTag());
    if (completedServers.isEmpty()) {
      throw new IllegalStateException(
          "Found no realtime completed servers with tag " + realtimeTagConfig.getCompletedServerTag());
    }

    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();

    // create map of completed server name to num segments it holds.
    // This will help us decide which completed server to choose for replacing a consuming server
    Map<String, Integer> completedServerToNumSegments = new HashMap<>(completedServers.size());
    for (String server : completedServers) {
      completedServerToNumSegments.put(server, 0);
    }
    for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
      for (String instance : entry.getValue().keySet()) {
        if (completedServers.contains(instance)) {
          completedServerToNumSegments.put(instance, completedServerToNumSegments.get(instance) + 1);
        }
      }
    }
    MinMaxPriorityQueue<Map.Entry<String, Integer>> completedServersQueue =
        MinMaxPriorityQueue.orderedBy(new Comparator<Map.Entry<String, Integer>>() {
          @Override
          public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
            return Integer.compare(entry1.getValue(), entry2.getValue());
          }
        }).maximumSize(completedServers.size()).create();
    completedServersQueue.addAll(completedServerToNumSegments.entrySet());

    // get new mapping for segments that need relocation
    segmentNameToInstancesMap = createNewSegmentToInstanceStateMap(mapFields, consumingServers, completedServersQueue);

    return segmentNameToInstancesMap;
  }

  /**
   * Given an ideal state mapping, list of consuming serves and completed servers,
   * create a mapping for those segments that should relocate a replica from consuming to completed server
   * @param mapFields
   * @param consumingServers
   * @param completedServersQueue
   * @return
   */
  private Map<String, Map<String, String>> createNewSegmentToInstanceStateMap(
      Map<String, Map<String, String>> mapFields, List<String> consumingServers,
      MinMaxPriorityQueue<Map.Entry<String, Integer>> completedServersQueue) {

    Map<String, Map<String, String>> segmentNameToInstancesMap = new HashMap<>();
    // TODO: we are scanning the entire segments list every time. This is unnecessary because only the latest segments will need relocation
    // Can we do something to avoid this?
    // 1. Time boundary: scan only last day whereas runFrequency = hourly
    // 2. For each partition, scan in descending order, and stop when the first segment not needing relocation is found
    for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {

      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();

      Map<String, String> newInstanceStateMap =
          createNewInstanceStateMap(instanceStateMap, consumingServers, completedServersQueue);
      if (MapUtils.isNotEmpty(newInstanceStateMap)) {
        segmentNameToInstancesMap.put(segmentName, newInstanceStateMap);
      }
    }
    return segmentNameToInstancesMap;
  }

  /**
   * Given the instanceStateMap and a list of consuming and completed servers for a realtime resource,
   * creates a new instanceStateMap, where one replica's instance is replaced from a consuming server to a completed server
   * @param instanceStateMap
   * @param consumingServers
   * @param completedServersQueue
   * @return
   */
  private Map<String, String> createNewInstanceStateMap(Map<String, String> instanceStateMap,
      List<String> consumingServers, MinMaxPriorityQueue<Map.Entry<String, Integer>> completedServersQueue) {

    Map<String, String> newInstanceStateMap = null;

    // proceed only if all segments are ONLINE, and at least 1 server is from consuming list
    for (String state : instanceStateMap.values()) {
      if (!state.equals(PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE)) {
        return newInstanceStateMap;
      }
    }

    for (String instance : instanceStateMap.keySet()) {
      if (consumingServers.contains(instance)) {
        // Decide best strategy to pick completed server.
        // 1. pick random from list of completed servers
        // 2. pick completed server with minimum segments, based on ideal state of this resource
        // 3. pick completed server with minimum segment, based on ideal state of all resources in this tenant
        // 4. use SegmentAssignmentStrategy

        // TODO: Using 2 for now. We should use 4. However the current interface and implementations cannot be used as is.
        // We should refactor the SegmentAssignmentStrategy interface suitably and reuse it here

        Map.Entry<String, Integer> completedInstance = completedServersQueue.pollFirst();

        newInstanceStateMap = new HashMap<>(instanceStateMap.size());
        newInstanceStateMap.putAll(instanceStateMap);
        newInstanceStateMap.remove(instance);
        newInstanceStateMap.put(completedInstance.getKey(),
            PinotHelixSegmentOnlineOfflineStateModelGenerator.ONLINE_STATE);

        completedInstance.setValue(completedInstance.getValue() + 1);
        completedServersQueue.add(completedInstance);
        break;
      }
    }
    return newInstanceStateMap;
  }

  /**
   * Updates the ideal state of given tablename with the new segment to instance state map
   * @param tableNameWithType
   * @param segmentNameToInstanceMap
   */
  private void updateIdealState(final String tableNameWithType,
      final Map<String, Map<String, String>> segmentNameToInstanceMap) {
    HelixHelper.updateIdealState(_helixManager, tableNameWithType, new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        for (Map.Entry<String, Map<String, String>> entry : segmentNameToInstanceMap.entrySet()) {
          idealState.setInstanceStateMap(entry.getKey(), entry.getValue());
        }
        return idealState;
      }
    }, RetryPolicies.exponentialBackoffRetryPolicy(5, 1000, 2.0f));
  }
}
