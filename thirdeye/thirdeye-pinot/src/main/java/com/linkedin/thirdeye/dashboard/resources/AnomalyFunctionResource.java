package com.linkedin.thirdeye.dashboard.resources;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.detection.TimeSeriesUtil;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutoTune;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterUtil;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.email.filter.AlphaBetaAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;

@Path("thirdeye/function")
@Produces(MediaType.APPLICATION_JSON)
public class AnomalyFunctionResource {

  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionResource.class);
  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final String ALPHABETA_ALERTFILTER_TYPE = "alpha_beta";
  private static final String ALPHABETALOGISTIC_ALERTFILTER_TYPE = "alpha_beta_logisitc";


  private final Map<String, Object> anomalyFunctionMetadata = new HashMap<>();
  private final AnomalyFunctionFactory anomalyFunctionFactory;
  private final AlertFilterAutotuneFactory alertFilterAutotuneFactory;
  private final AlertFilterFactory alertFilterFactory;


  public AnomalyFunctionResource(String functionConfigPath, String alertFilterAutotuneConfigPath, String alertFilterConfigPath) {
    buildFunctionMetadata(functionConfigPath);
    this.anomalyFunctionFactory = new AnomalyFunctionFactory(functionConfigPath);
    this.alertFilterAutotuneFactory = new AlertFilterAutotuneFactory(alertFilterAutotuneConfigPath);
    this.alertFilterFactory = new AlertFilterFactory(alertFilterConfigPath);
  }

  private void buildFunctionMetadata(String functionConfigPath) {
    Properties props = new Properties();
    InputStream input = null;
    try {
      input = new FileInputStream(functionConfigPath);
      props.load(input);
    } catch (IOException e) {
      LOG.error("Function config not found at {}", functionConfigPath);
    } finally {
      IOUtils.closeQuietly(input);
    }
    LOG.info("Loaded functions : " + props.keySet() + " from path : " + functionConfigPath);
    for (Object key : props.keySet()) {
      String functionName = key.toString();
      try {
        Class<AnomalyFunction> clz = (Class<AnomalyFunction>) Class.forName(props.get(functionName).toString());
        Method getFunctionProps = clz.getMethod("getPropertyKeys");
        anomalyFunctionMetadata.put(functionName, getFunctionProps.invoke(null));
      } catch (ClassNotFoundException e) {
        LOG.warn("Unknown class for function : " + functionName);
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        LOG.error("Unknown method", e);
      }
    }
  }

  /**
   * @return map of function name vs function property keys
   * <p/>
   * eg. { "WEEK_OVER_WEEK_RULE":["baseline","changeThreshold","averageVolumeThreshold"],
   * "MIN_MAX_THRESHOLD":["min","max"] }
   */
  @GET
  @Path("metadata")
  public Map<String, Object> getAnomalyFunctionMetadata() {
    return anomalyFunctionMetadata;
  }

  /**
   * @return List of metric functions
   * <p/>
   * eg. ["SUM","AVG","COUNT"]
   */
  @GET
  @Path("metric-function")
  public MetricAggFunction[] getMetricFunctions() {
    return MetricAggFunction.values();
  }

  @POST
  @Path("/analyze")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response analyze(AnomalyFunctionDTO anomalyFunctionSpec,
      @QueryParam("startTime") Long startTime, @QueryParam("endTime") Long endTime)
      throws Exception {
    // TODO: replace this with Job/Task framework and job tracker page
    BaseAnomalyFunction anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);

    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(startTime, endTime);

    Map<DimensionKey, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap =
        TimeSeriesUtil.getTimeSeriesForAnomalyDetection(anomalyFunctionSpec, startEndTimeRanges);

    List<RawAnomalyResultDTO> anomalyResults = new ArrayList<>();
    List<RawAnomalyResultDTO> results = new ArrayList<>();
    List<String> collectionDimensions = DAO_REGISTRY.getDatasetConfigDAO()
        .findByDataset(anomalyFunctionSpec.getCollection()).getDimensions();

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : dimensionKeyMetricTimeSeriesMap.entrySet()) {
      DimensionKey dimensionKey = entry.getKey();
      DimensionMap dimensionMap = DimensionMap.fromDimensionKey(dimensionKey, collectionDimensions);
      if (entry.getValue().getTimeWindowSet().size() < 2) {
        LOG.warn("Insufficient data for {} to run anomaly detection function", dimensionMap);
        continue;
      }
      try {
        // Run algorithm
        MetricTimeSeries metricTimeSeries = entry.getValue();
        LOG.info("Analyzing anomaly function with dimensionKey: {}, windowStart: {}, windowEnd: {}",
            dimensionMap, startTime, endTime);

        List<RawAnomalyResultDTO> resultsOfAnEntry = anomalyFunction
            .analyze(dimensionMap, metricTimeSeries, new DateTime(startTime), new DateTime(endTime),
                new ArrayList<>());
        if (resultsOfAnEntry.size() != 0) {
          results.addAll(resultsOfAnEntry);
        }
        LOG.info("{} has {} anomalies in window {} to {}", dimensionMap, resultsOfAnEntry.size(),
            new DateTime(startTime), new DateTime(endTime));
      } catch (Exception e) {
        LOG.error("Could not compute for {}", dimensionMap, e);
      }
    }
    if (results.size() > 0) {
      List<RawAnomalyResultDTO> validResults = new ArrayList<>();
      for (RawAnomalyResultDTO anomaly : results) {
        if (!anomaly.isDataMissing()) {
          LOG.info("Found anomaly, sev [{}] start [{}] end [{}]", anomaly.getWeight(),
              new DateTime(anomaly.getStartTime()), new DateTime(anomaly.getEndTime()));
          validResults.add(anomaly);
        }
      }
      anomalyResults.addAll(validResults);
    }
    return Response.ok(anomalyResults).build();
  }

  @POST
  @Path("/autotune/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response tuneAlertFilter(@PathParam("id") String id,
      @QueryParam("startTime") Long startTime, @QueryParam("endTime") Long endTime)
      throws Exception {
    AnomalyFunctionDTO anomalyFunctionSpec = DAO_REGISTRY.getAnomalyFunctionDAO().findById(Long.valueOf(id));
    AnomalyFunctionManager anomalyFunctionDAO = DAO_REGISTRY.getAnomalyFunctionDAO();
    AlertFilterAutoTune alertFilterAutotune = alertFilterAutotuneFactory.fromSpec(anomalyFunctionSpec);
    AlertFilter alertFilter;
    Map<String, String> alertFilterParams = anomalyFunctionSpec.getAlertFilter();
    if (alertFilterParams == null) {
      alertFilter = new DummyAlertFilter();
    } else if (alertFilterParams.get("type") == ALPHABETA_ALERTFILTER_TYPE) {
      alertFilter = new AlphaBetaAlertFilter();
      alertFilter.setParameters(alertFilterParams);
    } else {
      alertFilter = alertFilterFactory.fromSpec(anomalyFunctionSpec);
    }
    String collectionName = anomalyFunctionSpec.getCollection();
    String metricName = anomalyFunctionSpec.getMetric();
    MergedAnomalyResultManager anomalyMergedResultDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();
    List<MergedAnomalyResultDTO> anomalyResultDTOS = anomalyMergedResultDAO.findByCollectionMetricTime(collectionName, metricName, startTime, endTime, false);

    //evaluate current alert filter (calcualte current precision and recall)
    AlertFilterUtil evaluator = new AlertFilterUtil(alertFilter);
    double[] evals = evaluator.getEvalResults(anomalyResultDTOS);
    try{
      Map<String,String> tunedAlertFilter = alertFilterAutotune.tuneAlertFilter(anomalyResultDTOS, evals[AlertFilterUtil.PRECISION_INDEX], evals[AlertFilterUtil.RECALL_INDEX]);
      if (alertFilterAutotune.isUpdated()){
        anomalyFunctionSpec.setAlertFilter(tunedAlertFilter);
        anomalyFunctionDAO.update(anomalyFunctionSpec);
        alertFilter.setParameters(tunedAlertFilter);
      } else {
        LOG.info("Model hasn't been updated");
      }
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return Response.ok(alertFilter).build();
  }
}
