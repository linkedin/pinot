package com.linkedin.thirdeye.autoload.pinot.metrics;

import com.google.common.collect.Sets;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AbstractManagerTestBase;
import com.linkedin.thirdeye.datalayer.dto.DashboardConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.DashboardConfigBean;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AutoloadPinotMetricsServiceTest  extends AbstractManagerTestBase {

  private AutoLoadPinotMetricsService testAutoLoadPinotMetricsService;
  private String dataset = "test-collection";
  private Schema schema;
  DatasetConfigDTO datasetConfig = null;

  @BeforeClass
  void beforeClass() {
    super.init();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    super.cleanup();
  }

  /**
   * This test class needs to use a class that accesses the global singleton DAO registry. Therefore,
   * we need to copy the local DAO registry, which is created for this test, to the global one.
   * Otherwise, the global singleton one may reference to an arbitrary DB that is created for this
   * test.
   */
  @BeforeMethod
  void beforeMethod(){
     // DAORegistry.overrideSingletonDAORegistryForTesting(testDBResources.getTestDaoRegistry());
  }

  @Test
  public void testAddNewDataset() throws Exception {
    testAutoLoadPinotMetricsService = new AutoLoadPinotMetricsService();
    schema = Schema.fromInputSteam(ClassLoader.getSystemResourceAsStream("sample-pinot-schema.json"));

    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, datasetConfig);

    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    datasetConfig = datasetConfigDAO.findByDataset(dataset);
    Assert.assertEquals(datasetConfig.getDataset(), dataset);
    Assert.assertEquals(datasetConfig.getDimensions(), schema.getDimensionNames());
    Assert.assertEquals(datasetConfig.getTimeColumn(), schema.getTimeColumnName());
    TimeGranularitySpec timeGranularitySpec = schema.getTimeFieldSpec().getOutgoingGranularitySpec();
    Assert.assertEquals(datasetConfig.getTimeUnit(), timeGranularitySpec.getTimeType());
    Assert.assertEquals(datasetConfig.getTimeDuration(), new Integer(timeGranularitySpec.getTimeUnitSize()));
    Assert.assertEquals(datasetConfig.getTimeFormat(), timeGranularitySpec.getTimeFormat());
    Assert.assertEquals(datasetConfig.getTimezone(), "UTC");
    Assert.assertEquals(datasetConfig.getExpectedDelay().getUnit(), TimeUnit.HOURS);

    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    List<String> schemaMetricNames = schema.getMetricNames();
    List<Long> metricIds = new ArrayList<>();
    Assert.assertEquals(metricConfigs.size(), schemaMetricNames.size());
    for (MetricConfigDTO metricConfig : metricConfigs) {
      Assert.assertTrue(schemaMetricNames.contains(metricConfig.getName()));
      metricIds.add(metricConfig.getId());
    }

    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.
        findByName(DashboardConfigBean.DEFAULT_DASHBOARD_PREFIX + dataset);
    Assert.assertEquals(dashboardConfig.getMetricIds(), metricIds);
  }

  @Test(dependsOnMethods = {"testAddNewDataset"})
  public void testRefreshDataset() throws Exception {
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec("newDimension", DataType.STRING, true);
    schema.addField(dimensionFieldSpec);
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, datasetConfig);
    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    DatasetConfigDTO newDatasetConfig1 = datasetConfigDAO.findByDataset(dataset);
    Assert.assertEquals(newDatasetConfig1.getDataset(), dataset);
    Assert.assertEquals(Sets.newHashSet(newDatasetConfig1.getDimensions()), Sets.newHashSet(schema.getDimensionNames()));

    MetricFieldSpec metricFieldSpec = new MetricFieldSpec("newMetric", DataType.LONG);
    schema.addField(metricFieldSpec);
    testAutoLoadPinotMetricsService.addPinotDataset(dataset, schema, newDatasetConfig1);

    Assert.assertEquals(datasetConfigDAO.findAll().size(), 1);
    List<MetricConfigDTO> metricConfigs = metricConfigDAO.findByDataset(dataset);
    List<String> schemaMetricNames = schema.getMetricNames();
    List<Long> metricIds = new ArrayList<>();
    Assert.assertEquals(metricConfigs.size(), schemaMetricNames.size());
    for (MetricConfigDTO metricConfig : metricConfigs) {
      Assert.assertTrue(schemaMetricNames.contains(metricConfig.getName()));
      metricIds.add(metricConfig.getId());
    }

    DashboardConfigDTO dashboardConfig = dashboardConfigDAO.
        findByName(DashboardConfigBean.DEFAULT_DASHBOARD_PREFIX + dataset);
    Assert.assertEquals(dashboardConfig.getMetricIds(), metricIds);
  }



}
