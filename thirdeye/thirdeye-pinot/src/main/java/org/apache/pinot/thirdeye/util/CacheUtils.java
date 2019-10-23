package org.apache.pinot.thirdeye.util;

import com.couchbase.client.java.document.json.JsonObject;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.detection.cache.TimeSeriesDataPoint;


public class CacheUtils {

  public static Map<Long, MetricSlice> findMaxRangeInterval(Collection<MetricSlice> slices) {
    if (slices == null || slices.isEmpty()) {
      return null;
    }

    Map<Long, MetricSlice> result = new HashMap<>();

    for (MetricSlice slice : slices) {
      long metricId = slice.getMetricId();
      if (result.containsKey(metricId)) {
        MetricSlice val = result.get(metricId);
        long minStart = Math.min(val.getStart(), slice.getStart());
        long maxEnd = Math.max(val.getEnd(), slice.getEnd());

        result.put(metricId, MetricSlice.from(metricId, minStart, maxEnd, slice.getFilters(), slice.getGranularity()));
      } else {
        result.put(metricId, slice);
      }
    }

    return result;
  }

  public static String hashMetricUrn(String metricUrn) {
    CRC32 c = new CRC32();
    c.update(metricUrn.getBytes());
    return String.valueOf(c.getValue());
  }

  public static JsonObject buildDocumentStructure(TimeSeriesDataPoint point) {
    Map<String, String> dims = new HashMap<>();
    dims.put(point.getMetricUrnHash(), point.getDataValue());

    JsonObject body = JsonObject.create()
        .put("time", point.getTimestamp())
        .put("metricId", point.getMetricId())
        .put("dims", dims);

    return body;
  }
}
