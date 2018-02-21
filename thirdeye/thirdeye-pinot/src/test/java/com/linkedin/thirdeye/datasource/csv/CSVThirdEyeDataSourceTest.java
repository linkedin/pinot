package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CSVThirdEyeDataSourceTest {
  ThirdEyeDataSource dataSource;

  @BeforeMethod
  public void beforeMethod(){

    Map<String, DataFrame> sources = new HashMap<>();
    sources.put("source", new DataFrame()
        .addSeries("timestamp", 0, 3600000, 7200000, 10800000)
        .addSeries("views", 1000, 1001, 1002, 1003)
        .addSeries("country", "us", "us", "us", "us"));
    sources.put("other", new DataFrame());
    Map<Long, String> metricNameMap = new HashMap<>();
    metricNameMap.put(1L, "views");

    dataSource = CSVThirdEyeDataSource.fromDataFrame(sources, metricNameMap);
  }

  @Test
  public void testFromDataFrame() throws Exception {
    Map<String, DataFrame> sources = new HashMap<>();
    sources.put("a", new DataFrame());
    sources.put("b", new DataFrame());
    sources.put("c", new DataFrame());

    CSVThirdEyeDataSource dataSource = CSVThirdEyeDataSource.fromDataFrame(sources, Collections.<Long, String>emptyMap());
    Assert.assertEquals(new HashSet<>(dataSource.getDatasets()), sources.keySet());
    Assert.assertNotSame(new HashSet<>(dataSource.getDatasets()), sources.keySet());
  }

  @Test
  public void testFromUrl() throws Exception {
    URL business = this.getClass().getResource("business.csv");

    Map<String, URL> sources = new HashMap<>();
    sources.put("business", business);

    CSVThirdEyeDataSource dataSource = CSVThirdEyeDataSource.fromUrl(sources, Collections.<Long, String>emptyMap());

    Map<String, List<String>> dimensions = new HashMap<>();
    dimensions.put("country", Arrays.asList("us", "cn"));
    dimensions.put("browser", Arrays.asList("chrome", "firefox", "safari"));

    Assert.assertEquals(dataSource.getDatasets(), Collections.singletonList("business"));
    Assert.assertEquals(dataSource.getMaxDataTime("business"), 7200000);
    Assert.assertEquals(new HashSet<>(dataSource.getDimensionFilters("business").get("country")), new HashSet<>(dimensions.get("country")));
    Assert.assertEquals(new HashSet<>(dataSource.getDimensionFilters("business").get("browser")), new HashSet<>(dimensions.get("browser")));
  }

  @Test
  public void testGetName() {
    Assert.assertEquals(dataSource.getName(), CSVThirdEyeDataSource.class.getSimpleName());
  }

  @Test
  public void testGetDatasets() throws Exception{
    Assert.assertEquals(new HashSet<>(dataSource.getDatasets()), new HashSet<>(Arrays.asList("source", "other")));
  }

  @Test
  public void testGetMaxDataTime() throws Exception{
    Assert.assertEquals(dataSource.getMaxDataTime("source"), 10800000);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetMaxDataTimeFail() throws Exception {
    dataSource.getMaxDataTime("invalid");
  }

  @Test
  public void testGetDimensionFilters() throws Exception{
    Assert.assertEquals(dataSource.getDimensionFilters("source"),
        Collections.singletonMap("country", Collections.singletonList("us")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testGetDimensionFiltersFail() throws Exception {
    dataSource.getDimensionFilters("invalid");
  }

  @Test
  public void testExecuteAggregation() throws Exception {
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .build("ref");
    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("SUM_views", 4006d)
            .addSeries("timestamp", -1L)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);

  }

  @Test
  public void testExecuteTimeSeries() throws Exception {
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .setGroupByTimeGranularity(new TimeGranularity(1, TimeUnit.HOURS))
        .build("ref");
    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("SUM_views", 1000d, 1001d, 1002d, 1003d)
            .addSeries("timestamp", 0, 3600000, 7200000, 10800000)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);

  }

  @Test
  public void testExecuteSelectTimeSeries() throws Exception {
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .setGroupByTimeGranularity(new TimeGranularity(1, TimeUnit.HOURS))
        .setStartTimeInclusive(2000000)
        .setEndTimeExclusive(10800000)
        .build("ref");
    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("SUM_views", 1001d, 1002d)
            .addSeries("timestamp", 3600000, 7200000)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);

  }

  @Test
  public void testExecuteSelectAggregation() throws Exception {
    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .addMetricFunction(new MetricFunction(MetricAggFunction.SUM, "views", 1L, "source", null, null))
        .setDataSource("source")
        .setStartTimeInclusive(1000000)
        .setEndTimeExclusive(11000000)
        .build("ref");
    ThirdEyeResponse expectedResponse = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame()
            .addSeries("SUM_views", 3006d)
            .addSeries("timestamp", -1L)
    );

    ThirdEyeResponse response = dataSource.execute(request);
    Assert.assertEquals(response, expectedResponse);

  }
}
