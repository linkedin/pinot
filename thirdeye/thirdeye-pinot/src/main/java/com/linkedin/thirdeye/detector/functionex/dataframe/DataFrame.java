package com.linkedin.thirdeye.detector.functionex.dataframe;

import com.udojava.evalex.Expression;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.math.NumberUtils;


public class DataFrame {
  public static Pattern SERIES_NAME_PATTERN = Pattern.compile("([A-Za-z_]\\w*)");

  public static final String COLUMN_INDEX = "index";
  public static final String COLUMN_JOIN = "join";

  public interface ResamplingStrategy {
    Series apply(Series s, List<Series.Bucket> buckets);
  }

  public static class ResampleLast implements ResamplingStrategy {
    @Override
    public Series apply(Series s, List<Series.Bucket> buckets) {
      switch(s.type()) {
        case DOUBLE:
          return ((DoubleSeries)s).aggregate(buckets, new DoubleSeries.DoubleBatchLast());
        case LONG:
          return ((LongSeries)s).aggregate(buckets, new LongSeries.LongBatchLast());
        case STRING:
          return ((StringSeries)s).aggregate(buckets, new StringSeries.StringBatchLast());
        case BOOLEAN:
          return ((BooleanSeries)s).aggregate(buckets, new BooleanSeries.BooleanBatchLast());
        default:
          throw new IllegalArgumentException(String.format("Cannot resample series type '%s'", s.type()));
      }
    }
  }

  Map<String, Series> series = new HashMap<>();

  public static DoubleSeries toSeries(double... values) {
    return new DoubleSeries(values);
  }

  public static LongSeries toSeries(long... values) {
    return new LongSeries(values);
  }

  public static BooleanSeries toSeries(boolean... values) {
    return new BooleanSeries(values);
  }

  public static StringSeries toSeries(String... values) {
    return new StringSeries(values);
  }

  public DataFrame(int defaultIndexSize) {
    long[] indexValues = new long[defaultIndexSize];
    for(int i=0; i<defaultIndexSize; i++) {
      indexValues[i] = i;
    }
    this.addSeries(COLUMN_INDEX, new LongSeries(indexValues));
  }

  public DataFrame(long[] indexValues) {
    this.addSeries(COLUMN_INDEX, new LongSeries(indexValues));
  }

  public DataFrame(LongSeries index) {
    this.addSeries(COLUMN_INDEX, index);
  }

  public DataFrame() {
    // left blank
  }

  public int size() {
    if(this.series.isEmpty())
      return 0;
    return this.series.values().iterator().next().size();
  }

  public DataFrame sliceRows(int from, int to) {
    DataFrame newDataFrame = new DataFrame();
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().slice(from, to));
    }
    return newDataFrame;
  }

  public boolean isEmpty() {
    return this.size() <= 0;
  }

  public DataFrame copy() {
    DataFrame newDataFrame = new DataFrame();
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().copy());
    }
    return newDataFrame;
  }

  public void addSeries(String seriesName, Series s) {
    if(seriesName == null || !SERIES_NAME_PATTERN.matcher(seriesName).matches())
      throw new IllegalArgumentException(String.format("Series name must match pattern '%s'", SERIES_NAME_PATTERN));
    if(!this.series.isEmpty() && s.size() != this.size())
      throw new IllegalArgumentException("DataFrame index and series must be of same length");
    series.put(seriesName, s);
  }

  public void addSeries(String seriesName, double... values) {
    addSeries(seriesName, DataFrame.toSeries(values));
  }

  public void addSeries(String seriesName, long... values) {
    addSeries(seriesName, DataFrame.toSeries(values));
  }

  public void addSeries(String seriesName, String... values) {
    addSeries(seriesName, DataFrame.toSeries(values));
  }

  public void addSeries(String seriesName, boolean... values) {
    addSeries(seriesName, DataFrame.toSeries(values));
  }

  public void dropSeries(String seriesName) {
    this.series.remove(seriesName);
  }

  public void renameSeries(String oldName, String newName) {
    Series s = assertSeriesExists(oldName);
    this.dropSeries(oldName);
    this.addSeries(newName, s);
  }

  public Set<String> getSeriesNames() {
    return Collections.unmodifiableSet(this.series.keySet());
  }

  public Map<String, Series> getSeries() {
    return Collections.unmodifiableMap(this.series);
  }

  public Series get(String seriesName) {
    return assertSeriesExists(seriesName);
  }

  public boolean contains(String seriesName) {
    return this.series.containsKey(seriesName);
  }

  public DoubleSeries toDoubles(String seriesName) {
    return assertSeriesExists(seriesName).toDoubles();
  }

  public LongSeries toLongs(String seriesName) {
    return assertSeriesExists(seriesName).toLongs();
  }

  public StringSeries toStrings(String seriesName) {
    return assertSeriesExists(seriesName).toStrings();
  }

  public BooleanSeries toBooleans(String seriesName) {
   return assertSeriesExists(seriesName).toBooleans();
  }

  public DoubleSeries map(DoubleSeries.DoubleBatchFunction function, String... seriesNames) {
    assertNotNull(seriesNames);
    return this.mapWithNull(function, seriesNames);
  }

  public DoubleSeries mapWithNull(DoubleSeries.DoubleBatchFunction function, String... seriesNames) {
    DoubleSeries[] inputSeries = new DoubleSeries[seriesNames.length];
    for(int i=0; i<seriesNames.length; i++) {
      inputSeries[i] = assertSeriesExists(seriesNames[i]).toDoubles();
    }

    double[] output = new double[this.size()];
    for(int i=0; i<this.size(); i++) {
      double[] input = new double[seriesNames.length];
      for(int j=0; j<inputSeries.length; j++) {
        input[j] = inputSeries[j].values[i];
      }
      output[i] = function.apply(input);
    }

    return new DoubleSeries(output);
  }

  public LongSeries map(LongSeries.LongBatchFunction function, String... seriesNames) {
    assertNotNull(seriesNames);
    return this.mapWithNull(function, seriesNames);
  }

  public LongSeries mapWithNull(LongSeries.LongBatchFunction function, String... seriesNames) {
    LongSeries[] inputSeries = new LongSeries[seriesNames.length];
    for(int i=0; i<seriesNames.length; i++) {
      inputSeries[i] = assertSeriesExists(seriesNames[i]).toLongs();
    }

    long[] output = new long[this.size()];
    for(int i=0; i<this.size(); i++) {
      long[] input = new long[seriesNames.length];
      for(int j=0; j<inputSeries.length; j++) {
        input[j] = inputSeries[j].values[i];
      }
      output[i] = function.apply(input);
    }

    return new LongSeries(output);
  }

  public StringSeries map(StringSeries.StringBatchFunction function, String... seriesNames) {
    assertNotNull(seriesNames);
    return this.mapWithNull(function, seriesNames);
  }

  public StringSeries mapWithNull(StringSeries.StringBatchFunction function, String... seriesNames) {
    StringSeries[] inputSeries = new StringSeries[seriesNames.length];
    for(int i=0; i<seriesNames.length; i++) {
      inputSeries[i] = assertSeriesExists(seriesNames[i]).toStrings();
    }

    String[] output = new String[this.size()];
    for(int i=0; i<this.size(); i++) {
      String[] input = new String[seriesNames.length];
      for(int j=0; j<inputSeries.length; j++) {
        input[j] = inputSeries[j].values[i];
      }
      output[i] = function.apply(input);
    }

    return new StringSeries(output);
  }

  public BooleanSeries map(BooleanSeries.BooleanBatchFunction function, String... seriesNames) {
    assertNotNull(seriesNames);
    return this.mapWithNull(function, seriesNames);
  }

  public BooleanSeries mapWithNull(BooleanSeries.BooleanBatchFunction function, String... seriesNames) {
    BooleanSeries[] inputSeries = new BooleanSeries[seriesNames.length];
    for(int i=0; i<seriesNames.length; i++) {
      inputSeries[i] = assertSeriesExists(seriesNames[i]).toBooleans();
    }

    boolean[] output = new boolean[this.size()];
    for(int i=0; i<this.size(); i++) {
      boolean[] input = new boolean[seriesNames.length];
      for(int j=0; j<inputSeries.length; j++) {
        input[j] = inputSeries[j].values[i];
      }
      output[i] = function.apply(input);
    }

    return new BooleanSeries(output);
  }

  public DoubleSeries map(String doubleExpression, String... seriesNames) {
    assertNotNull(seriesNames);
    return this.mapWithNull(doubleExpression, seriesNames);
  }

  public DoubleSeries mapWithNull(String doubleExpression, String... seriesNames) {
    Expression e = new Expression(doubleExpression);

    return this.mapWithNull(new DoubleSeries.DoubleBatchFunction() {
      @Override
      public double apply(double[] values) {
        for(int i=0; i<values.length; i++) {
          if(DoubleSeries.isNull(values[i]))
            return DoubleSeries.NULL_VALUE;
          e.with(seriesNames[i], new BigDecimal(values[i]));
        }
        return e.eval().doubleValue();
      }
    }, seriesNames);
  }

  public DoubleSeries map(String doubleExpression) {
    Set<String> variables = extractSeriesNames(doubleExpression);
    return this.map(doubleExpression, variables.toArray(new String[variables.size()]));
  }

  public DoubleSeries mapWithNull(String doubleExpression) {
    Set<String> variables = extractSeriesNames(doubleExpression);
    return this.mapWithNull(doubleExpression, variables.toArray(new String[variables.size()]));
  }

  public DataFrame project(int[] fromIndex) {
    DataFrame newDataFrame = new DataFrame();
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().project(fromIndex));
    }
    return newDataFrame;
  }

  /**
   * Sort data frame by series values.  The resulting sorted order is the equivalent of applying
   * a stable sorted to nth series first, and then sorting iteratively until the 1st series.
   *
   * @param seriesNames 1st series, 2nd series, ..., nth series
   * @return sorted data frame
   */
  public DataFrame sortBy(String... seriesNames) {
    DataFrame df = this;
    for(int i=seriesNames.length-1; i>=0; i--) {
      df = df.project(assertSeriesExists(seriesNames[i]).sortedIndex());
    }
    return df;
  }

  public DataFrame reverse() {
    DataFrame newDataFrame = new DataFrame();
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      newDataFrame.addSeries(e.getKey(), e.getValue().reverse());
    }
    return newDataFrame;
  }

  public DataFrame resampleBy(String seriesName, long interval, ResamplingStrategy strategy) {
    DataFrame baseDataFrame = this.sortBy(seriesName);

    List<Series.Bucket> buckets = baseDataFrame.toLongs(seriesName).groupByInterval(interval);

    // new index from intervals
    int startIndex = (int)(baseDataFrame.toLongs(seriesName).min() / interval);

    long[] rvalues = new long[buckets.size()];
    for(int i=0; i<buckets.size(); i++) {
      rvalues[i] = (i + startIndex) * interval;
    }

    // resample series
    DataFrame newDataFrame = new DataFrame();

    for(Map.Entry<String, Series> e : baseDataFrame.getSeries().entrySet()) {
      if(e.getKey().equals(seriesName))
        continue;
      newDataFrame.addSeries(e.getKey(), strategy.apply(e.getValue(), buckets));
    }

    newDataFrame.addSeries(seriesName, new LongSeries(rvalues));
    return newDataFrame;
  }

  DataFrame filter(int[] fromIndex) {
    DataFrame df = new DataFrame();
    for(Map.Entry<String, Series> e : this.getSeries().entrySet()) {
      df.addSeries(e.getKey(), e.getValue().project(fromIndex));
    }
    return df;
  }

  public DataFrame filter(BooleanSeries series) {
    if(series.size() != this.size())
      throw new IllegalArgumentException("Series size must be equal to index size");

    int[] fromIndex = new int[series.size()];
    int fromIndexCount = 0;
    for(int i=0; i<series.size(); i++) {
      if(series.values[i]) {
        fromIndex[fromIndexCount] = i;
        fromIndexCount++;
      }
    }

    int[] fromIndexCompressed = Arrays.copyOf(fromIndex, fromIndexCount);

    return this.filter(fromIndexCompressed);
  }

  public DataFrame filter(String seriesName) {
    return this.filter(this.toBooleans(seriesName));
  }

  public DataFrame filter(String seriesName, DoubleSeries.DoubleConditional conditional) {
    return this.filter(assertSeriesExists(seriesName).toDoubles().map(conditional));
  }

  public DataFrame filter(String seriesName, LongSeries.LongConditional conditional) {
    return this.filter(assertSeriesExists(seriesName).toLongs().map(conditional));
  }

  public DataFrame filter(String seriesName, StringSeries.StringConditional conditional) {
    return this.filter(assertSeriesExists(seriesName).toStrings().map(conditional));
  }

  public DataFrame filterEquals(String seriesName, double value) {
    return this.filter(seriesName, new DoubleSeries.DoubleConditional() {
      @Override
      public boolean apply(double v) {
        return value == v;
      }
    });
  }

  public DataFrame filterEquals(String seriesName, long value) {
    return this.filter(seriesName, new LongSeries.LongConditional() {
      @Override
      public boolean apply(long v) {
        return value == v;
      }
    });
  }

  public DataFrame filterEquals(String seriesName, String value) {
    return this.filter(seriesName, new StringSeries.StringConditional() {
      @Override
      public boolean apply(String v) {
        return value.equals(v);
      }
    });
  }

  public double getDouble(String seriesName) {
    return assertSingleValue(seriesName).toDoubles().first();
  }

  public long getLong(String seriesName) {
    return assertSingleValue(seriesName).toLongs().first();
  }

  public String getString(String seriesName) {
    return assertSingleValue(seriesName).toStrings().first();
  }

  public boolean getBoolean(String seriesName) {
    return assertSingleValue(seriesName).toBooleans().first();
  }

  public static DoubleSeries toDoubles(DoubleSeries s) {
    return s;
  }

  public static DoubleSeries toDoubles(LongSeries s) {
    double[] values = new double[s.size()];
    for(int i=0; i<values.length; i++) {
      if(LongSeries.isNull(s.values[i])) {
        values[i] = DoubleSeries.NULL_VALUE;
      } else {
        values[i] = (double) s.values[i];
      }
    }
    return new DoubleSeries(values);
  }

  public static DoubleSeries toDoubles(StringSeries s) {
    double[] values = new double[s.size()];
    for(int i=0; i<values.length; i++) {
      if(StringSeries.isNull(s.values[i])) {
        values[i] = DoubleSeries.NULL_VALUE;
      } else {
        values[i] = Double.parseDouble(s.values[i]);
      }
    }
    return new DoubleSeries(values);
  }

  public static DoubleSeries toDoubles(BooleanSeries s) {
    double[] values = new double[s.size()];
    for(int i=0; i<values.length; i++) {
      values[i] = s.values[i] ? 1.0d : 0.0d;
    }
    return new DoubleSeries(values);
  }

  public static LongSeries toLongs(DoubleSeries s) {
    long[] values = new long[s.size()];
    for(int i=0; i<values.length; i++) {
      if(DoubleSeries.isNull(s.values[i])) {
        values[i] = LongSeries.NULL_VALUE;
      } else {
        values[i] = (long) s.values[i];
      }
    }
    return new LongSeries(values);
  }

  public static LongSeries toLongs(LongSeries s) {
    return s;
  }

  public static LongSeries toLongs(StringSeries s) {
    long[] values = new long[s.size()];
    for(int i=0; i<values.length; i++) {
      if(StringSeries.isNull(s.values[i])) {
        values[i] = LongSeries.NULL_VALUE;
      } else {
        try {
          values[i] = Long.parseLong(s.values[i]);
        } catch (NumberFormatException e) {
          values[i] = (long) Double.parseDouble(s.values[i]);
        }
      }
    }
    return new LongSeries(values);
  }

  public static LongSeries toLongs(BooleanSeries s) {
    long[] values = new long[s.size()];
    for(int i=0; i<values.length; i++) {
      values[i] = s.values[i] ? 1L : 0L;
    }
    return new LongSeries(values);
  }

  public static BooleanSeries toBooleans(DoubleSeries s) {
    boolean[] values = new boolean[s.size()];
    for(int i=0; i<values.length; i++) {
      if(DoubleSeries.isNull(s.values[i])) {
        values[i] = BooleanSeries.NULL_VALUE;
      } else {
        values[i] = s.values[i] != 0.0d;
      }
    }
    return new BooleanSeries(values);
  }

  public static BooleanSeries toBooleans(LongSeries s) {
    boolean[] values = new boolean[s.size()];
    for(int i=0; i<values.length; i++) {
      if(LongSeries.isNull(s.values[i])) {
        values[i] = BooleanSeries.NULL_VALUE;
      } else {
        values[i] = s.values[i] != 0L;
      }
    }
    return new BooleanSeries(values);
  }

  public static BooleanSeries toBooleans(BooleanSeries s) {
    return s;
  }

  public static BooleanSeries toBooleans(StringSeries s) {
    boolean[] values = new boolean[s.size()];
    for(int i=0; i<values.length; i++) {
      if(StringSeries.isNull(s.values[i])) {
        values[i] = BooleanSeries.NULL_VALUE;
      } else {
        if(NumberUtils.isNumber(s.values[i])) {
          values[i] = Double.parseDouble(s.values[i]) != 0.0d;
        } else {
          values[i] = Boolean.parseBoolean(s.values[i]);
        }
      }
    }
    return new BooleanSeries(values);
  }

  public static StringSeries toStrings(DoubleSeries s) {
    String[] values = new String[s.size()];
    for(int i=0; i<values.length; i++) {
      if(DoubleSeries.isNull(s.values[i])) {
        values[i] = StringSeries.NULL_VALUE;
      } else {
        values[i] = String.valueOf(s.values[i]);
      }
    }
    return new StringSeries(values);
  }

  public static StringSeries toStrings(LongSeries s) {
    String[] values = new String[s.size()];
    for(int i=0; i<values.length; i++) {
      if(LongSeries.isNull(s.values[i])) {
        values[i] = StringSeries.NULL_VALUE;
      } else {
        values[i] = String.valueOf(s.values[i]);
      }
    }
    return new StringSeries(values);
  }

  public static StringSeries toStrings(BooleanSeries s) {
    String[] values = new String[s.size()];
    for(int i=0; i<values.length; i++) {
      values[i] = String.valueOf(s.values[i]);
    }
    return new StringSeries(values);
  }

  public static StringSeries toStrings(StringSeries s) {
    return s;
  }

  public static Series toType(Series s, Series.SeriesType type) {
    switch(type) {
      case DOUBLE:
        return s.toDoubles();
      case LONG:
        return s.toLongs();
      case BOOLEAN:
        return s.toBooleans();
      case STRING:
        return s.toStrings();
      default:
        throw new IllegalArgumentException(String.format("Unknown series type '%s'", type));
    }
  }

  // TODO partition class with typing and key
  public List<DataFrame> groupBy(Series labels) {
    if(this.size() != labels.size())
      throw new IllegalArgumentException("Series size must be equals to DataFrame size");
    List<Series.Bucket> buckets = labels.groupByValue();
    List<DataFrame> slices = new ArrayList<>();
    for(Series.Bucket b : buckets) {
      slices.add(this.project(b.fromIndex));
    }
    return slices;
  }

  public List<DataFrame> groupBy(String seriesNameLabels) {
    return this.groupBy(assertSeriesExists(seriesNameLabels));
  }

  public static DoubleSeries aggregate(List<DataFrame> groups, String seriesName, DoubleSeries.DoubleBatchFunction function) {
    double[] values = new double[groups.size()];
    int i = 0;
    for(DataFrame df : groups) {
      values[i++] = function.apply(df.toDoubles(seriesName).values());
    }
    return new DoubleSeries(values);
  }

  public static LongSeries aggregate(List<DataFrame> groups, String seriesName, LongSeries.LongBatchFunction function) {
    long[] values = new long[groups.size()];
    int i = 0;
    for(DataFrame df : groups) {
      values[i++] = function.apply(df.toLongs(seriesName).values());
    }
    return new LongSeries(values);
  }

  public static StringSeries aggregate(List<DataFrame> groups, String seriesName, StringSeries.StringBatchFunction function) {
    String[] values = new String[groups.size()];
    int i = 0;
    for(DataFrame df : groups) {
      values[i++] = function.apply(df.toStrings(seriesName).values());
    }
    return new StringSeries(values);
  }

  public static BooleanSeries aggregate(List<DataFrame> groups, String seriesName, BooleanSeries.BooleanBatchFunction function) {
    boolean[] values = new boolean[groups.size()];
    int i = 0;
    for(DataFrame df : groups) {
      values[i++] = function.apply(df.toBooleans(seriesName).values());
    }
    return new BooleanSeries(values);
  }

  public DataFrame dropNullRows() {
    int[] fromIndex = new int[this.size()];
    for(int i=0; i<fromIndex.length; i++) {
      fromIndex[i] = i;
    }

    for(Series s : this.series.values()) {
      int[] nulls = s.nullIndex();
      for(int n : nulls) {
        fromIndex[n] = -1;
      }
    }

    int countNotNull = 0;
    for(int i=0; i<fromIndex.length; i++) {
      if(fromIndex[i] >= 0) {
        fromIndex[countNotNull] = fromIndex[i];
        countNotNull++;
      }
    }

    int[] fromIndexCompressed = Arrays.copyOf(fromIndex, countNotNull);

    return this.filter(fromIndexCompressed);
  }

  public DataFrame dropNullColumns() {
    DataFrame df = new DataFrame();
    for(Map.Entry<String, Series> e : this.getSeries().entrySet()) {
      if(!e.getValue().hasNull())
        df.addSeries(e.getKey(), e.getValue());
    }
    return df;
  }

  public DataFrame joinOuter(DataFrame other, String onLeftSeries, String onRightSeries) {
    DataFrame left = this.sortBy(onLeftSeries);
    DataFrame right = other.sortBy(onRightSeries);

    int[] sortedLeft = this.get(onLeftSeries).sortedIndex();
    int[] sortedRight = other.get(onRightSeries).sortedIndex();

    return null; // TODO
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("DataFrame{\n");
    for(Map.Entry<String, Series> e : this.series.entrySet()) {
      builder.append(e.getKey());
      builder.append(": ");
      builder.append(e.getValue());
      builder.append("\n");
    }
    builder.append("}");
    return builder.toString();
  }

  private Series assertSeriesExists(String name) {
    if(!series.containsKey(name))
      throw new IllegalArgumentException(String.format("Unknown series '%s'", name));
    return series.get(name);
  }

  private Series assertSingleValue(String name) {
    if(assertSeriesExists(name).size() != 1)
      throw new IllegalArgumentException(String.format("Series '%s' must have exactly one element", name));
    return series.get(name);
  }

  private Series assertNotNull(String name) {
    if(assertSeriesExists(name).hasNull())
      throw new IllegalStateException(String.format("Series '%s' Must not contain null values", name));
    return series.get(name);
  }

  private void assertNotNull(String... names) {
    for(String s : names)
      assertNotNull(s);
  }

  private Set<String> extractSeriesNames(String doubleExpression) {
    Matcher m = SERIES_NAME_PATTERN.matcher(doubleExpression);

    Set<String> variables = new HashSet<>();
    while(m.find()) {
      if(this.series.keySet().contains(m.group()))
        variables.add(m.group());
    }

    return variables;
  }

}
