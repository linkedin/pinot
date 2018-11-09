package com.linkedin.thirdeye.detection.yaml;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.detection.ConfigUtils;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.algorithm.DimensionWrapper;
import com.linkedin.thirdeye.detection.annotation.DetectionRegistry;
import com.linkedin.thirdeye.detection.wrapper.AnomalyDetectorWrapper;
import com.linkedin.thirdeye.detection.wrapper.AnomalyFilterWrapper;
import com.linkedin.thirdeye.detection.wrapper.BaselineFillingMergeWrapper;
import com.linkedin.thirdeye.detection.wrapper.ChildKeepingMergeWrapper;
import com.linkedin.thirdeye.detection.wrapper.DetectionUtils;
import com.linkedin.thirdeye.rootcause.impl.MetricEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.MapUtils;

import static com.linkedin.thirdeye.detection.ConfigUtils.*;


/**
 * The YAML translator of composite pipeline. Convert YAML file into detection pipeline properties.
 * This pipeline supports multiple detection algorithm running in OR relationship.
 * Supports running multiple filter rule running in AND relationship.
 * Conceptually, the pipeline has the following structure. (Grouper not implemented yet).
 *
 *         +-----------+
 *         | Dimension |
 *         |Exploration|
 *         +-----+-----+
 *               |
 *         +-----v-----+
 *         |Dimension  |
 *         |Filter     |
 *       +-------------+--+
 *       |                |
 *       |                |
 * +-----v----+     +-----v----+
 * | Rule     |     | Algorithm|
 * | detection|     | Detection|
 * +-------------+  +-----+----+
 *       |                |
 * +-----v----+     +-----v----+
 * |Rule      |     |Algorithm |
 * |  Merger  |     |  Merger  |
 * +-----+----+     +-----+----+
 *       |                |
 *       v                v
 * +-----+----+     +-----+----+
 * |Rule      |     |Algorithm |
 * |Filters   |     |Filters   |
 * +-----+----+     +-----+----+
 *       +-------+--------+
 *               |
 *         +-----v-----+
 *         |   Merger  |
 *         |           |
 *         +-----+-----+
 *               |
 *               |
 *         +-----v-----+
 *         |  Grouper* |
 *         |           |
 *         +-----------+
 *
 *
 * This translator translates a yaml that describes the above detection flow
 * to the flowing wrapper structure to be execute in the detection pipeline.
 * +-----------------------------------------+
 * |  Merger                                 |
 * |  +--------------------------------------+
 * |  |   Dimension Exploration & Filter    ||
 * |  | +---------------------------------+ ||
 * |  | |        Filters                  | ||
 * |  | |  +--------Merger--------------+ | ||
 * |  | |  |  Rule detections           | | ||
 * |  | |  +----------------------------+ | ||
 * |  | +---------------------------------+ ||
 * |  | +---------------------------------+ ||
 * |  | |              Filters            | ||
 * |  | | +-----------Merger------------+ | ||
 * |  | | | +---------------------------| | ||
 * |  | | | | Algorithm detection       | | ||
 * |  | | | +---------------------------| | ||
 * |  | | +-----------------------------| | ||
 * |  | +---------------------------------+ ||
 * |  +--------------------------------------|
 * |  |--------------------------------------|
 * +-----------------------------------------+
 *
 */
public class CompositePipelineConfigTranslator extends YamlDetectionConfigTranslator {
  private static final String PROP_DIMENSION_EXPLORATION = "dimensionExploration";

  private static final String PROP_DETECTION = "detection";
  private static final String PROP_FILTER = "filter";
  private static final String PROP_FILTERS = "filters";
  private static final String PROP_METRIC = "metric";
  private static final String PROP_DATASET = "dataset";
  private static final String PROP_TYPE = "type";
  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_PARAMS = "params";
  private static final String PROP_METRIC_URN = "metricUrn";
  private static final String PROP_RULES = "rules";
  private static final String PROP_NESTED = "nested";
  private static final String PROP_BASELINE_PROVIDER = "baselineValueProvider";
  private static final String PROP_NAME = "name";
  private static final String PROP_DETECTOR = "detector";

  private static final DetectionRegistry DETECTION_REGISTRY = DetectionRegistry.getInstance();
  private static final Map<String, String> DETECTOR_TO_BASELINE = ImmutableMap.of();

  private final Map<String, Object> components = new HashMap<>();

  public CompositePipelineConfigTranslator(Map<String, Object> yamlConfig, DataProvider provider) {
    super(yamlConfig, provider);
  }

  @Override
  YamlTranslationResult translateYaml() {
    String metricUrn = buildMetricUrn(yamlConfig);

    List<Map<String, Object>> ruleYamls = getList(yamlConfig.get(PROP_RULES));
    List<Map<String, Object>> nestedPipelines = new ArrayList<>();
    for (Map<String, Object> ruleYaml : ruleYamls) {
      String ruleName = MapUtils.getString(ruleYaml, PROP_NAME);
      List<Map<String, Object>> filterYamls = ConfigUtils.getList(ruleYaml.get(PROP_FILTER));
      List<Map<String, Object>> detectionYamls = ConfigUtils.getList(ruleYaml.get(PROP_DETECTION));
      List<Map<String, Object>> detectionProperties = buildListOfMergeWrapperProperties(ruleName, detectionYamls);
      if (filterYamls == null || filterYamls.isEmpty()) {
        nestedPipelines.addAll(detectionProperties);
      } else {
        List<Map<String, Object>> filterNestedProperties = detectionProperties;
        for (Map<String, Object> filterProperties : filterYamls) {
          filterNestedProperties = buildFilterWrapperProperties(AnomalyFilterWrapper.class.getName(),
              filterProperties, filterNestedProperties, ruleName);
        }
        nestedPipelines.addAll(filterNestedProperties);
      }
    }
    Map<String, Object> dimensionWrapperProperties = new HashMap<>();
    dimensionWrapperProperties.putAll(MapUtils.getMap(yamlConfig, PROP_DIMENSION_EXPLORATION));
    dimensionWrapperProperties.put(PROP_METRIC_URN, metricUrn);
    Map<String, Object> properties = buildWrapperProperties(ChildKeepingMergeWrapper.class.getName(), Collections.singletonList(
        buildWrapperProperties(DimensionWrapper.class.getName(), nestedPipelines, dimensionWrapperProperties)));
    return new YamlTranslationResult().withProperties(properties).withComponents(this.components);
  }

  private List<Map<String, Object>>  buildListOfMergeWrapperProperties(String ruleName, List<Map<String, Object>> yamlConfigs) {
    List<Map<String, Object>> properties = new ArrayList<>();
    for (Map<String, Object> yamlConfig : yamlConfigs) {
      properties.add(buildMergeWrapperProperties(ruleName, yamlConfig));
    }
    return properties;
  }

  private Map<String, Object> buildMergeWrapperProperties(String ruleName, Map<String, Object> yamlConfig) {
    String detectorType = MapUtils.getString(yamlConfig, PROP_TYPE);
    Map<String, Object> nestedProperties = new HashMap<>();
    nestedProperties.put(PROP_CLASS_NAME, AnomalyDetectorWrapper.class.getName());
    String detectorKey = makeComponentKey(ruleName, detectorType);
    nestedProperties.put(PROP_DETECTOR, detectorKey);
    // TODO insert window size & unit

    buildComponentSpec(yamlConfig, detectorType, detectorKey);

    Map<String, Object> properties = new HashMap<>();
    properties.put(PROP_CLASS_NAME, BaselineFillingMergeWrapper.class.getName());
    properties.put(PROP_NESTED, Collections.singletonList(nestedProperties));
    String baselineProviderType = "RULE_BASELINE";
    if (DETECTOR_TO_BASELINE.containsKey(detectorType)){
      baselineProviderType = DETECTOR_TO_BASELINE.get(detectorType);
    }
    String baselineProviderKey = makeComponentKey(ruleName, baselineProviderType);
    properties.put(PROP_BASELINE_PROVIDER, baselineProviderKey);
    buildComponentSpec(yamlConfig, baselineProviderType, baselineProviderKey);

    return properties;
  }

  private void buildComponentSpec(Map<String, Object> yamlConfig, String type, String componentKey) {
    String componentClassName = DETECTION_REGISTRY.lookup(type);
    Map<String, Object> detectorSpecs = new HashMap<>();
    detectorSpecs.put(PROP_CLASS_NAME, componentClassName);
    Map<String, Object> params = MapUtils.getMap(yamlConfig, PROP_PARAMS);
    if (DETECTION_REGISTRY.isTrainable(componentClassName)){
      // TODO get trainer and train
    } else {
      detectorSpecs.putAll(params);
    }
    this.components.put(DetectionUtils.getComponentName(componentKey), detectorSpecs);
  }

  private String makeComponentKey(String name, String type) {
    return "$" + name + "_" + type;
  }

  private List<Map<String, Object>> buildFilterWrapperProperties(String wrapperClassName,
      Map<String, Object> yamlConfig, List<Map<String, Object>> nestedProperties, String ruleName) {
    if (yamlConfig == null || yamlConfig.isEmpty()) {
      return nestedProperties;
    }
    Map<String, Object> wrapperProperties = buildWrapperProperties(wrapperClassName, nestedProperties);
    if (wrapperProperties.isEmpty()) {
      return Collections.emptyList();
    }
    String filterType = MapUtils.getString(yamlConfig, PROP_TYPE);
    String filterKey = makeComponentKey(ruleName, filterType);
    wrapperProperties.put(PROP_FILTER, filterKey);
    buildComponentSpec(yamlConfig, filterType, filterKey);

    return Collections.singletonList(wrapperProperties);
  }


  private Map<String, Object> buildWrapperProperties(String wrapperClassName,
      List<Map<String, Object>> nestedProperties) {
    return buildWrapperProperties(wrapperClassName, nestedProperties, Collections.emptyMap());
  }

  private Map<String, Object> buildWrapperProperties(String wrapperClassName,
      List<Map<String, Object>> nestedProperties, Map<String, Object> defaultProperties) {
    Map<String, Object> properties = new HashMap<>();
    List<Map<String, Object>> wrapperNestedProperties = new ArrayList<>();
    for (Map<String, Object> nested : nestedProperties) {
      if (nested != null && !nested.isEmpty()) {
        wrapperNestedProperties.add(nested);
      }
    }
    if (wrapperNestedProperties.isEmpty()) {
      return properties;
    }
    properties.put(PROP_CLASS_NAME, wrapperClassName);
    properties.put(PROP_NESTED, wrapperNestedProperties);
    properties.putAll(defaultProperties);
    return properties;
  }


  private String buildMetricUrn(Map<String, Object> yamlConfig) {
    Map<String, Collection<String>> filterMaps = MapUtils.getMap(yamlConfig, PROP_FILTERS);

    Multimap<String, String> filters = ArrayListMultimap.create();
    if (filterMaps != null) {
      for (Map.Entry<String, Collection<String>> entry : filterMaps.entrySet()) {
        filters.putAll(entry.getKey(), entry.getValue());
      }
    }
    MetricConfigDTO metricConfig = this.dataProvider.fetchMetric(MapUtils.getString(yamlConfig, PROP_METRIC),
        MapUtils.getString(yamlConfig, PROP_DATASET));
    Preconditions.checkNotNull(metricConfig, "Metric not found");

    MetricEntity me = MetricEntity.fromMetric(1.0, metricConfig.getId(), filters);
    return me.getUrn();
  }

  @Override
  protected void validateYAML(Map<String, Object> yamlConfig) {
    super.validateYAML(yamlConfig);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_METRIC), "Property missing " + PROP_METRIC);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_DATASET), "Property missing " + PROP_DATASET);
    Preconditions.checkArgument(yamlConfig.containsKey(PROP_RULES), "Property missing " + PROP_RULES);

    List<Map<String, Object>> detectionYamls = getList(yamlConfig.get(PROP_RULES));
    for (int i = 0; i < detectionYamls.size(); i++) {
      Map<String, Object> detectionYaml = detectionYamls.get(i);
      Preconditions.checkArgument(detectionYaml.containsKey(PROP_DETECTION), "In rule No." + (i+1) + ", detection stage property missing. ");
      List<Map<String, Object>> detectionStageYamls = ConfigUtils.getList(detectionYaml.get(PROP_DETECTION));
      for (Map<String, Object> detectionStageYaml : detectionStageYamls) {
        Preconditions.checkArgument(detectionStageYaml.containsKey(PROP_TYPE), "In rule No." + (i+1) + ", detection stage type missing. ");
      }
      if (detectionYaml.containsKey(PROP_FILTER)) {
        List<Map<String, Object>> filterStageYamls = ConfigUtils.getList(MapUtils.getMap(detectionYaml, PROP_FILTER));
        for (Map<String, Object> filterStageYaml : filterStageYamls) {
          Preconditions.checkArgument(filterStageYaml.containsKey(PROP_TYPE), "In rule No." + (i+1) + ", filter stage type missing. ");
        }
      }
    }
  }
}
