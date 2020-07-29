/*
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

package org.apache.pinot.thirdeye.api.detection;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.dropwizard.auth.Auth;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.api.Constants;
import org.apache.pinot.thirdeye.api.user.dashboard.UserDashboardResource;
import org.apache.pinot.thirdeye.auth.ThirdEyePrincipal;
import org.apache.pinot.thirdeye.common.metric.MetricType;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.AnomalySummary;
import org.apache.pinot.thirdeye.datalayer.bao.*;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultAggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.*;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.detection.validators.DatasetConfigValidator;
import org.apache.pinot.thirdeye.detection.validators.DetectionConfigValidator;
import org.apache.pinot.thirdeye.detection.validators.MetricConfigValidator;
import org.apache.pinot.thirdeye.detection.yaml.DetectionConfigTuner;
import org.apache.pinot.thirdeye.detection.yaml.translator.DetectionConfigTranslator;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.jfree.util.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Path("/anomaly-detection")
@Api(tags = { Constants.DETECTION_TAG })
public class AnomalyDetectionResource {
  protected static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionResource.class);

  private static final String TEMPLATE_DETECTION_PATH = "detection-config-template.yml";

  /* -------- Detection config fields -------- */
  private static final String DETECTION_YAML_FIELD = "detectionName";
  private static final String DEFAULT_DETECTION_NAME = "online_detection";

  /* -------- Metric config fields -------- */
  private static final String DATASET_YAML_FIELD = "dataset";
  private static final String DEFAULT_DATASET_NAME = "online_dataset";
  private static final String DATATYPE_YAML_FIELD = "datatype";
  private static final MetricType DEFAULT_DATA_TYPE = MetricType.DOUBLE;

  /* -------- Dataset config fields -------- */
  private static final String METRIC_YAML_FIELD = "metric";
  private static final String DEFAULT_METRIC_NAME = "online_metric";
  private static final String DEFAULT_METRIC_COLUMN = "metric";
  private static final String TIME_COLUMN_YAML_FIELD = "timeColumn";
  private static final String DEFAULT_TIME_COLUMN = "date";
  private static final String TIME_UNIT_YAML_FIELD = "timeUnit";
  private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.DAYS;
  private static final String TIME_DURATION_YAML_FIELD = "timeDuration";
  private static final String TIME_FORMAT_YAML_FIELD = "timeFormat";
  private static final String DEFAULT_TIME_FORMAT = "SIMPLE_DATE_FORMAT:yyyyMMdd";
  private static final String TIME_ZONE_YAML_FIELD = "timezone";
  private static final String DEFAULT_TIME_ZONE = "US/Pacific";
  private static final List<String> DEFAULT_DIMENSIONS =
      Collections.unmodifiableList(new ArrayList<>());

  /* -------- Request/Response field -------- */
  private static final String DATA_FIELD = "data";
  private static final String COLUMNS_FIELD = "columns";
  private static final String ROWS_FIELD = "rows";
  private static final String DATASET_FIELD = "datasetConfiguration";
  private static final String METRIC_FIELD = "metricConfiguration";
  private static final String DETECTION_FIELD = "detectionConfiguration";
  private static final String ANOMALIES_FIELD = "anomalies";

  /* -------- Others -------- */
  private static final String ONLINE_DATASOURCE = "OnlineThirdEyeDataSource";
  private static final String DETECTION_MYSQL_NAME_COLUMN = "name";
  private static final String TASK_MYSQL_NAME_COLUMN = "name";
  private static final String ANOMALY_ENDPOINT_URL = "/userdashboard/anomalies";
  private static final long POLLING_SLEEP_TIME = 5L;
  private static final int DEFAULT_TIME_DURATION = 1;
  private static final long MAX_ONLINE_PAYLOAD_SIZE = 10 * 1024 * 1024L;

  private final UserDashboardResource userDashboardResource;
  private final DetectionConfigManager detectionConfigDAO;
  private final DataProvider provider;
  private final MetricConfigManager metricConfigDAO;
  private final DatasetConfigManager datasetConfigDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final EvaluationManager evaluationDAO;
  private final TaskManager taskDAO;
  private final DetectionPipelineLoader loader;
  private final DetectionConfigValidator detectionValidator;
  private final DatasetConfigValidator datasetConfigValidator;
  private final MetricConfigValidator metricConfigValidator;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Yaml yaml;

  public AnomalyDetectionResource(UserDashboardResource userDashboardResource) {
    this.detectionConfigDAO = DAORegistry.getInstance().getDetectionConfigManager();
    this.metricConfigDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetConfigDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.taskDAO = DAORegistry.getInstance().getTaskDAO();
    this.evaluationDAO = DAORegistry.getInstance().getEvaluationManager();
    this.userDashboardResource = userDashboardResource;

    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricConfigDAO, datasetConfigDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getTimeSeriesCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricConfigDAO, datasetConfigDAO,
            ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.loader = new DetectionPipelineLoader();

    this.provider = new DefaultDataProvider(metricConfigDAO, datasetConfigDAO, eventDAO, anomalyDAO,
        evaluationDAO, timeseriesLoader, aggregationLoader, loader,
        TimeSeriesCacheBuilder.getInstance(), AnomaliesCacheBuilder.getInstance());
    this.detectionValidator = new DetectionConfigValidator(this.provider);
    this.metricConfigValidator = new MetricConfigValidator();
    this.datasetConfigValidator = new DatasetConfigValidator();

    // Read template from disk
    this.yaml = new Yaml();
  }

  /**
   * Run an online anomaly detection service synchronously. It will run anomaly detection using
   * default configs for detection, metric, dataset
   *
   * @param start     detection window start time
   * @param end       detection window end time
   * @param payload   payload in request including online data
   * @param principal user who sent this request. It's used to separate different config names
   * @return a message containing the detected anomalies and the detection config used
   */
  @POST
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation("Request an anomaly detection online task")
  public Response onlineApi(
          @QueryParam("start") long start,
          @QueryParam("end") long end,
          @ApiParam("jsonPayload") String payload,
          @Auth ThirdEyePrincipal principal) {
    DatasetConfigDTO datasetConfigDTO = null;
    MetricConfigDTO metricConfigDTO = null;
    DetectionConfigDTO detectionConfigDTO = null;
    TaskDTO taskDTO = null;
    List<AnomalySummary> anomalies = null;
    Response.Status responseStatus;
    Map<String, String> responseMessage = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();
    // Use username to separate different requests. One user can only send one request at a time
    String nameSuffix = "_" + principal.getName();

    try {
      if (payload.getBytes().length > MAX_ONLINE_PAYLOAD_SIZE) {
        responseStatus = Response.Status.BAD_REQUEST;
        responseMessage.put("message", "Payload too large");
        return Response.status(responseStatus).entity(responseMessage).build();
      }

      JsonNode payloadNode = objectMapper.readTree(payload);

      if (!validateOnlineRequestPayload(payloadNode)) {
        responseStatus = Response.Status.BAD_REQUEST;
        responseMessage.put("message", "Invalid request payload");
        return Response.status(responseStatus).entity(responseMessage).build();
      }

      // Preprocess: remove existing entities generated by the previous interrupted request
      cleanExistingOnlineTask(nameSuffix);

      // Create & save dataset
      datasetConfigDTO = generateDatasetConfig(payloadNode, nameSuffix);

      // Create & save metric along with online data
      metricConfigDTO = generateMetricConfig(payloadNode, nameSuffix);

      // Create & save detection
      detectionConfigDTO =
          generateDetectionConfig(payloadNode, nameSuffix, datasetConfigDTO, metricConfigDTO, start,
              end);

      // Create & save task
      taskDTO = generateTaskConfig(detectionConfigDTO.getId(), start, end);

      // Polling task status
      TaskDTO polledTaskDTO = pollingTask(taskDTO.getId());

      // Task failure
      if (polledTaskDTO.getStatus() != TaskConstants.TaskStatus.COMPLETED) {
        LOG.warn("Task is not completed after polling: " + polledTaskDTO);

        responseStatus = Response.Status.INTERNAL_SERVER_ERROR;

        switch (polledTaskDTO.getStatus()) {
        case FAILED:
          responseStatus = Response.Status.INTERNAL_SERVER_ERROR;
          responseMessage.put("message", "Failed to execute anomaly detection task.");
          break;
        case TIMEOUT:
          responseStatus = Response.Status.REQUEST_TIMEOUT;
          responseMessage.put("message", "Anomaly detection task timeout.");
        default:
          LOG.error("Error task status after polling: " + polledTaskDTO.getStatus());
          responseMessage.put("message", "unknown task status.");
          break;
        }

        responseMessage.put("more-info", "Error = " + polledTaskDTO.getMessage());

        // Send response
        return Response.status(responseStatus).entity(responseMessage).build();
      }

      // Task success
      // Retrieve task result
      anomalies = getAnomalies(start, end, metricConfigDTO.getName(), datasetConfigDTO.getName());

      // Build success response
      JsonNode anomalyNode = objectMapper.convertValue(anomalies, JsonNode.class);
      JsonNode detectionConfigNode =
          objectMapper.convertValue(detectionConfigDTO.getYaml(), JsonNode.class);
      ObjectNode responseNode = objectMapper.createObjectNode();
      responseNode.set(ANOMALIES_FIELD, anomalyNode);
      responseNode.set(DETECTION_FIELD, detectionConfigNode);

      responseStatus = Response.Status.OK;
      return Response.status(responseStatus).entity(objectMapper.writeValueAsString(responseNode))
          .build();
    } catch (JsonProcessingException e) {
      LOG.error("Error: {}", e.getMessage());
      responseStatus = Response.Status.BAD_REQUEST;
      responseMessage.put("message", "Invalid request payload");
      processException(e, responseMessage);
      return Response.status(responseStatus).entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error: {}", e.getMessage());
      responseStatus = Response.Status.INTERNAL_SERVER_ERROR;
      responseMessage.put("message", "Failed executing anomaly detection service.");
      processException(e, responseMessage);
      return Response.status(responseStatus).entity(responseMessage).build();
    } finally {
      // Online service is stateless
      cleanStates(anomalies, taskDTO, metricConfigDTO, datasetConfigDTO, detectionConfigDTO);
    }
  }

  void cleanExistingOnlineTask(String nameSuffix) {
    String metricName = DEFAULT_METRIC_NAME + nameSuffix;
    List<MetricConfigDTO> metricConfigDTOS = metricConfigDAO.findByMetricName(metricName);
    for (MetricConfigDTO metricConfigDTO : metricConfigDTOS) {
      metricConfigDAO.deleteById(metricConfigDTO.getId());
      LOG.info("Deleted existing metric: {}", metricConfigDTO);
    }

    String datasetName = DEFAULT_DATASET_NAME + nameSuffix;
    DatasetConfigDTO datasetConfigDTO = datasetConfigDAO.findByDataset(datasetName);
    if (datasetConfigDTO != null) {
      datasetConfigDAO.delete(datasetConfigDTO);
      LOG.info("Deleted existing dataset: {}", datasetConfigDTO);
    }

    String detectionName = DEFAULT_DETECTION_NAME + nameSuffix;
    List<DetectionConfigDTO> detectionConfigDTOS = detectionConfigDAO
        .findByPredicate(Predicate.EQ(DETECTION_MYSQL_NAME_COLUMN, detectionName));
    for (DetectionConfigDTO detectionConfigDTO : detectionConfigDTOS) {
      detectionConfigDAO.delete(detectionConfigDTO);
      taskDAO.deleteByPredicate(Predicate.EQ(TASK_MYSQL_NAME_COLUMN,
          TaskConstants.TaskType.DETECTION.name() + "_" + detectionConfigDTO.getId()));
      LOG.info("Deleted existing task with detection: {}", detectionConfigDTO);
    }
  }

  boolean validateOnlineRequestPayload(JsonNode payloadNode) {
    if (!payloadNode.has(DATA_FIELD))
      return false;

    JsonNode dataNode = payloadNode.get(DATA_FIELD);
    if (!dataNode.has(COLUMNS_FIELD) || !dataNode.has(ROWS_FIELD))
      return false;

    JsonNode columnsNode = dataNode.get(COLUMNS_FIELD);
    if (!columnsNode.isArray())
      return false;

    boolean hasTimeColumn = false, hasMetricColumn = false;
    for (JsonNode columnNode : columnsNode) {
      if (columnNode.textValue().equals(DEFAULT_TIME_COLUMN))
        hasTimeColumn = true;
      if (columnNode.textValue().equals(DEFAULT_METRIC_COLUMN))
        hasMetricColumn = true;
      if (hasTimeColumn && hasMetricColumn)
        break;
    }
    return hasTimeColumn && hasMetricColumn;
  }

  DatasetConfigDTO generateDatasetConfig(JsonNode payloadNode, String suffix) {
    DatasetConfigDTO datasetConfigDTO = new DatasetConfigDTO();

    // Default configuration
    datasetConfigDTO.setDataset(DEFAULT_DATASET_NAME + suffix);
    datasetConfigDTO.setDimensions(DEFAULT_DIMENSIONS);
    datasetConfigDTO.setTimeColumn(DEFAULT_TIME_COLUMN);
    datasetConfigDTO.setTimeDuration(DEFAULT_TIME_DURATION);
    datasetConfigDTO.setTimeUnit(DEFAULT_TIME_UNIT);
    datasetConfigDTO.setTimeFormat(DEFAULT_TIME_FORMAT);
    datasetConfigDTO.setTimezone(DEFAULT_TIME_ZONE);
    datasetConfigDTO.setDataSource(ONLINE_DATASOURCE);

    // Customized configuration
    if (payloadNode.has(DATASET_FIELD)) {

      Map<String, Object> datasetYaml =
          ConfigUtils.getMap(yaml.load(payloadNode.get(DATASET_FIELD).textValue()));

      if (datasetYaml.containsKey(TIME_COLUMN_YAML_FIELD)) {
        datasetConfigDTO.setTimeColumn((String) datasetYaml.get(TIME_COLUMN_YAML_FIELD));
      }
      if (datasetYaml.containsKey(TIME_UNIT_YAML_FIELD)) {
        datasetConfigDTO
            .setTimeUnit(TimeUnit.valueOf((String) datasetYaml.get(TIME_UNIT_YAML_FIELD)));
      }
      if (datasetYaml.containsKey(TIME_DURATION_YAML_FIELD)) {
        datasetConfigDTO.setTimeDuration((Integer) datasetYaml.get(TIME_DURATION_YAML_FIELD));
      }
      if (datasetYaml.containsKey(TIME_FORMAT_YAML_FIELD)) {
        datasetConfigDTO.setTimeFormat((String) datasetYaml.get(TIME_FORMAT_YAML_FIELD));
      }
      if (datasetYaml.containsKey(TIME_ZONE_YAML_FIELD)) {
        datasetConfigDTO.setTimezone((String) datasetYaml.get(TIME_ZONE_YAML_FIELD));
      }
    }

    this.datasetConfigValidator.validateConfig(datasetConfigDTO);

    datasetConfigDAO.save(datasetConfigDTO);
    LOG.info("Created dataset with config {}", datasetConfigDTO);

    return datasetConfigDTO;
  }

  MetricConfigDTO generateMetricConfig(JsonNode payloadNode, String suffix)
      throws JsonProcessingException {
    MetricConfigDTO metricConfigDTO = new MetricConfigDTO();
    JsonNode dataNode = payloadNode.get(DATA_FIELD);

    // Default configuration
    metricConfigDTO.setName(DEFAULT_METRIC_NAME + suffix);
    metricConfigDTO.setDataset(DEFAULT_DATASET_NAME + suffix);
    metricConfigDTO.setAlias(ThirdEyeUtils
        .constructMetricAlias(DEFAULT_DATASET_NAME + suffix,
            DEFAULT_METRIC_NAME + suffix));
    metricConfigDTO.setDatatype(DEFAULT_DATA_TYPE);
    metricConfigDTO.setDefaultAggFunction(MetricAggFunction.SUM);
    metricConfigDTO.setActive(true);

    // Customized configuration
    if (payloadNode.has(METRIC_FIELD)) {
      Map<String, Object> metricYaml =
          ConfigUtils.getMap(yaml.load(payloadNode.get(METRIC_FIELD).textValue()));

      if (metricYaml.containsKey(DATATYPE_YAML_FIELD)) {
        metricConfigDTO
            .setDatatype(MetricType.valueOf((String) metricYaml.get(DATATYPE_YAML_FIELD)));
      }
    }

    // Reformat Metric column name to keep consistency with metric config
    ArrayNode columnsNode = dataNode.withArray(COLUMNS_FIELD);
    if (columnsNode.isArray()) {
      int colIdx = 0;
      for (; colIdx < columnsNode.size(); colIdx++) {
        if (columnsNode.get(colIdx).textValue().equals(DEFAULT_METRIC_COLUMN)) {
          break;
        }
      }
      columnsNode.set(colIdx, new TextNode(DEFAULT_METRIC_NAME + suffix));
    }
    // TODO: should store online data into a new table
    metricConfigDTO.setOnlineData(this.objectMapper.writeValueAsString(dataNode));

    this.metricConfigValidator.validateConfig(metricConfigDTO);

    metricConfigDAO.save(metricConfigDTO);
    LOG.info("Created metric with config {}", metricConfigDTO);

    return metricConfigDTO;
  }

  DetectionConfigDTO generateDetectionConfig(JsonNode payloadNode, String suffix,
      DatasetConfigDTO datasetConfigDTO, MetricConfigDTO metricConfigDTO, long start, long end) {
    DetectionConfigDTO detectionConfigDTO;
    Map<String, Object> detectionYaml;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

    if (payloadNode.has(DETECTION_FIELD)) {
      // Customized configuration: retrieve config from user request
      detectionYaml = ConfigUtils.getMap(yaml.load(payloadNode.get(DETECTION_FIELD).textValue()));
    } else {
      // Default configuration: retrieve the template from disk
      detectionYaml =
          ConfigUtils.getMap(yaml.load(classLoader.getResourceAsStream(TEMPLATE_DETECTION_PATH)));
    }

    // Do not support customized detection name as it is not a common use case
    detectionYaml.put(DETECTION_YAML_FIELD, DEFAULT_DETECTION_NAME + suffix);
    detectionYaml.put(DATASET_YAML_FIELD, datasetConfigDTO.getName());
    detectionYaml.put(METRIC_YAML_FIELD, metricConfigDTO.getName());

    detectionConfigDTO =
        new DetectionConfigTranslator(this.yaml.dump(detectionYaml), this.provider).translate();
    detectionConfigDTO.setCron("0 0 0 1 1 ? 2200"); // Never scheduled

    // Tune the detection config - Passes the raw yaml params & injects tuned params
    DetectionConfigTuner detectionTuner = new DetectionConfigTuner(detectionConfigDTO, provider);
    detectionConfigDTO = detectionTuner.tune(start, end);

    // Validate the detection config
    detectionValidator.validateConfig(detectionConfigDTO);

    detectionConfigDAO.save(detectionConfigDTO);
    LOG.info("Created detection with config {}", detectionConfigDTO);

    return detectionConfigDTO;
  }

  TaskDTO generateTaskConfig(long detectionConfigId, long start, long end)
      throws JsonProcessingException {
    TaskDTO taskDTO = new TaskDTO();
    taskDTO.setJobName(TaskConstants.TaskType.DETECTION.toString() + "_" + detectionConfigId);
    taskDTO.setStatus(TaskConstants.TaskStatus.WAITING);
    taskDTO.setTaskType(TaskConstants.TaskType.DETECTION_ONLINE);
    DetectionPipelineTaskInfo taskInfo =
        new DetectionPipelineTaskInfo(detectionConfigId, start, end);
    String taskInfoJson = objectMapper.writeValueAsString(taskInfo);
    taskDTO.setTaskInfo(taskInfoJson);

    taskDAO.save(taskDTO);
    LOG.info("Created task: {}", taskDTO);

    return taskDTO;
  }

  private TaskDTO pollingTask(long taskId) {
    long startTime = System.currentTimeMillis();
    TaskDTO taskDTO;

    // Timeout mechanism will be handled by worker thread in the controller
    while (true) {
      taskDTO = taskDAO.findById(taskId);

      LOG.info("Polling task : " + taskDTO);

      TaskConstants.TaskStatus taskStatus = taskDTO.getStatus();
      if (!taskStatus.equals(TaskConstants.TaskStatus.WAITING) && !taskStatus
          .equals(TaskConstants.TaskStatus.RUNNING)) {
        LOG.info("Polling finished ({}ms). Task status: {}", System.currentTimeMillis() - startTime,
            taskStatus);
        break;
      }

      try {
        TimeUnit.SECONDS.sleep(POLLING_SLEEP_TIME);
      } catch (InterruptedException e) {
        Log.warn("Interrupted during polling sleep");
        break;
      }
    }

    return taskDTO;
  }

  private List<AnomalySummary> getAnomalies(long start, long end, String metric, String dataset) {
    List<AnomalySummary> anomalies =
        this.userDashboardResource.queryAnomalies(start, end, null, null, metric,
            dataset, null, false, null);

    LOG.info("Successfully returned " + anomalies.size() + " anomalies.");
    return anomalies;
  }

  private void cleanStates(List<AnomalySummary> anomalies, TaskDTO taskDTO,
      MetricConfigDTO metricConfigDTO, DatasetConfigDTO datasetConfigDTO,
      DetectionConfigDTO detectionConfigDTO) {
    if (anomalies != null) {
      for (AnomalySummary anomaly : anomalies) {
        anomalyDAO.deleteById(anomaly.getId());
        LOG.info("Deleted anomaly with id: {}", anomaly.getId());
      }
    }

    if (datasetConfigDTO != null) {
      datasetConfigDAO.delete(datasetConfigDTO);
      LOG.info("Deleted dataset: {}", datasetConfigDTO);
    }

    if (metricConfigDTO != null) {
      metricConfigDAO.delete(metricConfigDTO);
      LOG.info("Deleted metric: {}", metricConfigDTO);
    }

    if (detectionConfigDTO != null) {
      detectionConfigDAO.delete(detectionConfigDTO);
      LOG.info("Deleted detection: {}", detectionConfigDTO);
    }

    if (taskDTO != null) {
      taskDAO.delete(taskDTO);
      LOG.info("Deleted task: {}", taskDTO);
    }
  }

  /**
   * Given a detection config name, run a anomaly detection task using this detection config
   * asynchronously. It will return a task ID for query the task status later.
   *
   * @param start         detection window start time
   * @param end           detection window end time
   * @param detectionName the name of the detection config already existing in TE database
   * @return a message containing the ID of the anomaly detection task and HATEOAS links
   */
  @POST
  @Path("/tasks")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Submit an anomaly detection task")
  public Response taskSubmitApi(
      @QueryParam("start") long start,
      @QueryParam("end") long end,
      @QueryParam("detectionName") String detectionName) {
    long ts = System.currentTimeMillis();
    Map<String, String> responseMessage = new HashMap<>();
    try {
      // Find detection by name
      List<DetectionConfigDTO> detectionConfigDTOS =
          detectionConfigDAO.findByPredicate(Predicate.EQ("name", detectionName));

      // Precondition check
      if (detectionConfigDTOS.isEmpty()) {
        LOG.warn("Detection config not found: {}", detectionName);
        responseMessage.put("message", "Detection config not found: " + detectionName);
        return Response.status(Response.Status.NOT_FOUND).entity(responseMessage).build();
      } else if (detectionConfigDTOS.size() > 1) {
        LOG.error("Duplicate detection configs: {}", detectionConfigDTOS);
        responseMessage.put("message", "Duplicate detection configs");
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(responseMessage)
            .build();
      }

      DetectionConfigDTO detectionConfigDTO = detectionConfigDTOS.get(0);

      LOG.info("Find detection config: {}", detectionConfigDTO);

      // Create task
      DetectionPipelineTaskInfo taskInfo =
          new DetectionPipelineTaskInfo(detectionConfigDTO.getId(), start, end);
      String taskInfoJson;
      TaskDTO taskDTO;
      long taskId;
      try {
        taskInfoJson = objectMapper.writeValueAsString(taskInfo);
        taskDTO = TaskUtils
            .buildTask(taskInfo.getConfigId(), taskInfoJson, TaskConstants.TaskType.DETECTION);
        taskId = taskDAO.save(taskDTO);
        LOG.info("Saved task: " + taskDTO + " into DB");
        LOG.info("Create task successful, used {} milliseconds", System.currentTimeMillis() - ts);
      } catch (JsonProcessingException e) {
        LOG.error("Exception when converting DetectionPipelineTaskInfo {} to jsonString", taskInfo,
            e);
        responseMessage.put("message", "Error while creating detection task");
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(responseMessage)
            .build();
      }

      // Build HATEOAS response
      ObjectNode responseJson = buildResponseJson(
          UriBuilder.fromResource(AnomalyDetectionResource.class)
              .path(AnomalyDetectionResource.class, "taskSubmitApi")
              .build().toString(), "POST");

      Response.Status responseStatus = Response.Status.CREATED;

      if (taskId < 0) {
        responseStatus = Response.Status.BAD_REQUEST;
        return Response.status(responseStatus).entity(responseJson).build();
      }

      responseJson.put("taskId", taskId);
      addLink(responseJson, "taskStatus",
          UriBuilder.fromResource(AnomalyDetectionResource.class)
                .path(AnomalyDetectionResource.class, "taskStatusApi")
                .resolveTemplate("taskId", taskId)
                .build().toString(), "GET");

      return Response.status(responseStatus).entity(responseJson).build();
    } catch (IllegalArgumentException e) {
      LOG.error("Error: {}", e.getMessage());
      responseMessage.put("message", "Failed submitting anomaly detection task.");
      processException(e, responseMessage);
      return Response.status(Response.Status.BAD_REQUEST).entity(responseMessage).build();
    } catch (Exception e) {
      LOG.error("Error: {}", e.getMessage());
      responseMessage.put("message", "Failed submitting anomaly detection task.");
      processException(e, responseMessage);
      return Response.serverError().entity(responseMessage).build();
    }
  }

  private void processException(Throwable e, Map<String, String> responseMessage) {
    StringBuilder sb = new StringBuilder();
    // show more stack message to frontend for debugging
    getErrorMessage(0, 5, e, sb);
    responseMessage.put("more-info", "Error stack: " + sb.toString());
  }

  private void getErrorMessage(int curLevel, int totalLevel, Throwable e, StringBuilder sb) {
    if (curLevel <= totalLevel && e != null) {
      sb.append("==");
      if (e.getMessage() != null) {
        sb.append(e.getMessage());
      }
      getErrorMessage(curLevel + 1, totalLevel, e.getCause(), sb);
    }
  }

  /**
   * Given a anomaly detection task ID, return its current status.
   *
   * @param taskId the ID of the anomaly detection task to be queried
   * @return a message containing the status of the task and HATEOAS links
   */
  @GET @Path("/task/{taskId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation("Query a task status")
  public Response taskStatusApi(@PathParam("taskId") long taskId) {
    Map<String, String> responseMessage = new HashMap<>();

    try {
      // Find task
      TaskDTO taskDTO = taskDAO.findById(taskId);

      // Precondition check
      LOG.info("Try to find task by ID: " + taskId);
      if (taskDTO == null) {
        LOG.warn("Task not found {}", taskId);
        responseMessage.put("message", "Task not found: " + taskId);
        return Response.status(Response.Status.NOT_FOUND).entity(responseMessage).build();
      }

      LOG.info("Found task" + taskDTO);

      // Check task status
      TaskConstants.TaskStatus taskStatus = taskDTO.getStatus();
      Response.Status responseStatus;
      if (taskStatus.equals(TaskConstants.TaskStatus.COMPLETED)) {
        responseStatus = Response.Status.SEE_OTHER;
      } else {
        responseStatus = Response.Status.ACCEPTED;
      }

      // Build HATEOAS response
      ObjectNode responseJson = buildResponseJson(
          UriBuilder.fromResource(AnomalyDetectionResource.class)
              .path(AnomalyDetectionResource.class, "taskStatusApi")
              .resolveTemplate("taskId", taskId).build().toString(), "GET");

      responseJson.put("status", taskStatus.name());

      if (responseStatus.equals(Response.Status.SEE_OTHER)) {
        addLink(responseJson, "anomalies", ANOMALY_ENDPOINT_URL, "GET");
      }

      return Response.status(responseStatus).entity(responseJson).build();
    } catch (Exception e) {
      LOG.error("Error: {}", e.getMessage());
      responseMessage.put("message", "Failed querying anomaly detection task.");
      processException(e, responseMessage);
      return Response.serverError().entity(responseMessage).build();
    }
  }

  /* ----------- HATEOAS Utilities --------------- */
  private ObjectNode buildResponseJson(String selfUri, String selfMethod) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode rootNode = mapper.createObjectNode();

    ObjectNode linksNode = mapper.createObjectNode();
    ObjectNode selfNode = mapper.createObjectNode();

    rootNode.set("_links", linksNode);
    linksNode.set("self", selfNode);

    selfNode.put("href", selfUri);
    selfNode.put("method", selfMethod);

    return rootNode;
  }

  private void addLink(ObjectNode rootNode, String rel, String href, String method) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode linkNode = mapper.createObjectNode();
    linkNode.put("href", href);
    linkNode.put("method", method);
    ((ObjectNode) rootNode.get("_links")).set(rel, linkNode);
  }
}
