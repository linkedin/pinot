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

package com.linkedin.pinot.common.partition;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.linkedin.pinot.common.config.RealtimeTagConfig;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;


/**
 * Class to generate partitions assignment based on num partitions in ideal state, num tagged instances and num replicas
 */
public class PartitionAssignmentGenerator {

  private HelixManager _helixManager;

  public PartitionAssignmentGenerator(HelixManager helixManager) {
    _helixManager = helixManager;
  }

  /**
   * Gets partition assignment of a table by reading the segment assignment in ideal state
   */
  public PartitionAssignment getPartitionAssignmentFromIdealState(TableConfig tableConfig, IdealState idealState) {
    String tableNameWithType = tableConfig.getTableName();

    // read all segments
    Map<String, Map<String, String>> mapFields = idealState.getRecord().getMapFields();

    // get latest segment in each partition
    Map<String, LLCSegmentName> partitionIdToLatestSegment = new HashMap<>();
    for (Map.Entry<String, Map<String, String>> entry : mapFields.entrySet()) {
      String segmentName = entry.getKey();
      if (LLCSegmentName.isLowLevelConsumerSegmentName(segmentName)) {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
        String partitionId = String.valueOf(llcSegmentName.getPartitionId());
        LLCSegmentName latestSegment = partitionIdToLatestSegment.get(partitionId);
        if (latestSegment == null || llcSegmentName.getSequenceNumber() > latestSegment.getSequenceNumber()) {
          partitionIdToLatestSegment.put(partitionId, llcSegmentName);
        }
      }
    }

    // extract partition assignment from the latest segments
    PartitionAssignment partitionAssignment = new PartitionAssignment(tableNameWithType);
    for (Map.Entry<String, LLCSegmentName> entry : partitionIdToLatestSegment.entrySet()) {
      String segmentName = entry.getValue().getSegmentName();
      Map<String, String> instanceStateMap = mapFields.get(segmentName);
      partitionAssignment.addPartition(entry.getKey(), Lists.newArrayList(instanceStateMap.keySet()));
    }
    return partitionAssignment;
  }

  /**
   * Generates partition assignment for given table, using tagged hosts and num partitions
   */
  public PartitionAssignment generatePartitionAssignment(TableConfig tableConfig, int numPartitions) {

    List<String> partitions = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      partitions.add(String.valueOf(i));
    }

    String tableNameWithType = tableConfig.getTableName();
    int numReplicas = tableConfig.getValidationConfig().getReplicasPerPartitionNumber();
    RealtimeTagConfig realtimeTagConfig = getRealtimeTagConfig(tableConfig);

    List<String> consumingTaggedInstances = getConsumingTaggedInstances(realtimeTagConfig);
    if (consumingTaggedInstances.size() < numReplicas) {
      throw new IllegalStateException(
          "Not enough consuming instances tagged. Must be atleast equal to numReplicas:" + numReplicas);
    }
    int maxConsumingServers = realtimeTagConfig.getMaxConsumingServers();
    int numInstancesToUse = Math.min(maxConsumingServers, consumingTaggedInstances.size());

    /**
     * TODO: We will use only uniform assignment for now
     * This will be refactored as AssignmentStrategy interface and implementations UniformAssignment, BalancedAssignment etc
     * {@link PartitionAssignmentGenerator} and AssignmentStrategy interface will together replace
     * StreamPartitionAssignmentGenerator and StreamPartitionAssignmentStrategy
     */
    return uniformAssignment(tableNameWithType, partitions, numReplicas, consumingTaggedInstances, numInstancesToUse);
  }

  /**
   * Uniformly sprays the partitions and replicas across given list of instances
   * Picks starting point based on table hash value. This ensures that we will always pick the same starting point,
   * and return consistent assignment across calls
   * @param allInstances
   * @param partitions
   * @param numReplicas
   * @return
   */
  private PartitionAssignment uniformAssignment(String tableName, List<String> partitions, int numReplicas,
      List<String> allInstances, int numInstancesToUse) {

    PartitionAssignment partitionAssignment = new PartitionAssignment(tableName);

    Collections.sort(allInstances);
    List<String> instancesToUse = new ArrayList<>();
    if (allInstances.size() <= numInstancesToUse) {
      instancesToUse.addAll(allInstances);
    } else {
      int hashedServerId = Math.abs(EqualityUtils.hashCodeOf(tableName)) % allInstances.size();
      for (int i = 0; i < numInstancesToUse; i++) {
        instancesToUse.add(allInstances.get(hashedServerId++));
        if (hashedServerId == allInstances.size()) {
          hashedServerId = 0;
        }
      }
    }

    // pick a hashed start again, so that even in cases where we use all servers, we don't overload the servers at the start of the list
    int hashedStartingServer = Math.abs(EqualityUtils.hashCodeOf(tableName)) % numInstancesToUse;
    for (String partition : partitions) {
      List<String> instances = new ArrayList<>(numReplicas);
      for (int r = 0; r < numReplicas; r++) {
        instances.add(instancesToUse.get(hashedStartingServer++));
        if (hashedStartingServer == numInstancesToUse) {
          hashedStartingServer = 0;
        }
      }
      partitionAssignment.addPartition(partition, instances);
    }
    return partitionAssignment;
  }

  @VisibleForTesting
  protected List<String> getConsumingTaggedInstances(RealtimeTagConfig realtimeTagConfig) {
    String consumingServerTag = realtimeTagConfig.getConsumingServerTag();
    List<String> consumingTaggedInstances = _helixManager.getClusterManagmentTool()
        .getInstancesInClusterWithTag(_helixManager.getClusterName(), consumingServerTag);
    if (consumingTaggedInstances.isEmpty()) {
      throw new IllegalStateException("No instances found with tag " + consumingServerTag);
    }
    return consumingTaggedInstances;
  }

  @VisibleForTesting
  protected RealtimeTagConfig getRealtimeTagConfig(TableConfig tableConfig) {
    return new RealtimeTagConfig(tableConfig, _helixManager);
  }
}
