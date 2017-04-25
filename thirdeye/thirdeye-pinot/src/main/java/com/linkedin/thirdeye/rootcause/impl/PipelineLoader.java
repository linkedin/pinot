package com.linkedin.thirdeye.rootcause.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.rootcause.Pipeline;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineLoader {
  public static final String PROP_PATH = "path";

  private static final Logger LOG = LoggerFactory.getLogger(PipelineLoader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  public static List<Pipeline> getPipelinesFromConfig(File rcaConfig) throws Exception {
    List<Pipeline> pipelines = new ArrayList<>();

    RCAConfiguration rcaConfiguration = OBJECT_MAPPER.readValue(rcaConfig, RCAConfiguration.class);
    List<PipelineConfiguration> rcaPipelinesConfiguration = rcaConfiguration.getRcaPipelinesConfiguration();
    if (CollectionUtils.isNotEmpty(rcaPipelinesConfiguration)) {
      for (PipelineConfiguration pipelineConfig : rcaPipelinesConfiguration) {
        String name = pipelineConfig.getName();
        Set<String> inputs = new HashSet<>(pipelineConfig.getInputs());
        String className = pipelineConfig.getClassName();
        Map<String, String> properties = pipelineConfig.getProperties();

        if(properties != null)
          properties = augmentPathProperty(properties, rcaConfig);

        LOG.info("Creating pipeline '{}' [{}] with inputs '{}'", name, className, inputs);
        Constructor<?> constructor = Class.forName(className).getConstructor(String.class, Set.class, Map.class);
        Pipeline pipeline = (Pipeline) constructor.newInstance(name, inputs, properties);

        pipelines.add(pipeline);
      }
    }

    return pipelines;
  }

  static Map<String, String> augmentPathProperty(Map<String, String> properties, File rcaConfig) {
    if(properties.containsKey(PROP_PATH)) {
      File path = new File(properties.get(PROP_PATH));
      if(!path.isAbsolute())
        properties.put(PROP_PATH, rcaConfig.getParent() + File.separator + path);
    }
    return properties;
  }

}
