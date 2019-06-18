/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package org.apache.pinot.thirdeye.detection.health;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.TaskBean;


/**
 * The detection task status for a detection config
 */
public class DetectionTaskStatus {
  // the task success rate for the detection config
  @JsonProperty
  private final double taskSuccessRate;

  // the health status for the detection tasks
  @JsonProperty
  private final HealthStatus healthStatus;

  // the list of tasks for the detection config
  @JsonProperty
  private final List<TaskDTO> tasks;

  private static final double TASK_SUCCESS_RATE_BAD_THRESHOLD = 0.5;
  private static final double TASK_SUCCESS_RATE_MODERATE_THRESHOLD = 0.8;

  public DetectionTaskStatus(double taskSuccessRate, HealthStatus healthStatus, List<TaskDTO> tasks) {
    this.taskSuccessRate = taskSuccessRate;
    this.healthStatus = healthStatus;
    this.tasks = tasks;
  }

  public double getTaskSuccessRate() {
    return taskSuccessRate;
  }

  public HealthStatus getHealthStatus() {
    return healthStatus;
  }

  public List<TaskDTO> getTasks() {
    return tasks;
  }

  public static DetectionTaskStatus fromTasks(List<TaskDTO> tasks) {

    double taskSuccessRate = Double.NaN;
    // count the number of tasks by task status
    Map<TaskConstants.TaskStatus, Long> count =
        tasks.stream().collect(Collectors.groupingBy(TaskBean::getStatus, Collectors.counting()));
    if (count.size() != 0) {
      long completedTasks = count.getOrDefault(TaskConstants.TaskStatus.COMPLETED, 0L);
      long failedTasks = count.getOrDefault(
          TaskConstants.TaskStatus.FAILED, 0L);
      long timeoutTasks = count.getOrDefault(TaskConstants.TaskStatus.TIMEOUT, 0L);
      taskSuccessRate = (double) completedTasks / (failedTasks +  timeoutTasks + completedTasks);
    }
    return new DetectionTaskStatus(taskSuccessRate, classifyTaskStatus(taskSuccessRate), tasks);
  }

  private static HealthStatus classifyTaskStatus(double taskSuccessRate) {
    if (Double.isNaN(taskSuccessRate)) {
      return HealthStatus.UNKNOWN;
    }
    if (taskSuccessRate < TASK_SUCCESS_RATE_BAD_THRESHOLD) {
      return HealthStatus.BAD;
    }
    if (taskSuccessRate < TASK_SUCCESS_RATE_MODERATE_THRESHOLD) {
      return HealthStatus.MODERATE;
    }
    return HealthStatus.GOOD;
  }

}
