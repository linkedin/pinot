package org.apache.pinot.core.query.aggregation.function;

import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.spi.utils.ByteArray;
import org.locationtech.jts.geom.Geometry;


public class StUnionAggregationFunction extends BaseSingleInputAggregationFunction<Geometry, ByteArray> {

  /**
   * Constructor for the class.
   *
   * @param expression Expression to aggregate on.
   */
  public StUnionAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.ST_UNION;
  }

  @Override
  public void accept(AggregationFunctionVisitorBase visitor) {
    visitor.visit(this);
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    byte[][] bytesArray = blockValSetMap.get(_expression).getBytesValuesSV();
    Geometry geometry = aggregationResultHolder.getResult();
    for (int i = 0; i < length; i++) {
      Geometry value = GeometrySerializer.deserialize(bytesArray[i]);
      geometry = geometry == null ? value : geometry.union(value);
    }
    aggregationResultHolder.setValue(geometry);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    byte[][] bytesArray = blockValSetMap.get(_expression).getBytesValuesSV();
    for (int i = 0; i < length; i++) {
      int groupKey = groupKeyArray[i];
      Geometry value = GeometrySerializer.deserialize(bytesArray[i]);
      groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getResult(groupKey) == null ? value
          : ((Geometry) groupByResultHolder.getResult(groupKey)).union(value));
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    byte[][] bytesArray = blockValSetMap.get(_expression).getBytesValuesSV();
    for (int i = 0; i < length; i++) {
      Geometry value = GeometrySerializer.deserialize(bytesArray[i]);
      for (int groupKey : groupKeysArray[i]) {
        groupByResultHolder.setValueForKey(groupKey, groupByResultHolder.getResult(groupKey) == null ? value
            : ((Geometry) groupByResultHolder.getResult(groupKey)).union(value));
      }
    }
  }

  @Override
  public Geometry extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return aggregationResultHolder.getResult();
  }

  @Override
  public Geometry extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return groupByResultHolder.getResult(groupKey);
  }

  @Override
  public Geometry merge(Geometry intermediateResult1, Geometry intermediateResult2) {
    return intermediateResult1.union(intermediateResult2);
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return true;
  }

  @Override
  public DataSchema.ColumnDataType getIntermediateResultColumnType() {
    return DataSchema.ColumnDataType.OBJECT;
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.BYTES;
  }

  @Override
  public ByteArray extractFinalResult(Geometry geometry) {
    return new ByteArray(GeometrySerializer.serialize(geometry));
  }
}
