package com.linkedin.thirdeye.rootcause.timeseries;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.Grouping;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.DateTimeZone;
import org.joda.time.Partial;
import org.joda.time.Period;
import org.joda.time.PeriodType;


/**
 * Synthetic baseline from a list of time offsets, aggregated with a user-specified function.
 *
 * @see BaselineAggregateType
 */
public class BaselineAggregate implements Baseline {
  private static final String COL_KEY = Grouping.GROUP_KEY;

  private final BaselineAggregateType type;
  private final List<Period> offsets;
  private final DateTimeZone timeZone;
  private final PeriodType periodType;

  private BaselineAggregate(BaselineAggregateType type, List<Period> offsets, DateTimeZone timezone, PeriodType periodType) {
    this.type = type;
    this.offsets = offsets;
    this.timeZone = timezone;
    this.periodType = periodType;
  }

  public BaselineAggregate withType(BaselineAggregateType type) {
    return new BaselineAggregate(type, this.offsets, this.timeZone, this.periodType);
  }

  public BaselineAggregate withOffsets(List<Period> offsets) {
    return new BaselineAggregate(this.type, offsets, this.timeZone, this.periodType);
  }

  public BaselineAggregate withTimeZone(DateTimeZone timeZone) {
    return new BaselineAggregate(this.type, this.offsets, timeZone, this.periodType);
  }

  public BaselineAggregate withPeriodType(PeriodType periodType) {
    return new BaselineAggregate(this.type, this.offsets, this.timeZone, periodType);
  }

  @Override
  public List<MetricSlice> scatter(MetricSlice slice) {
    List<MetricSlice> slices = new ArrayList<>();
    for (Period offset : this.offsets) {
      slices.add(slice
          .withStart(new DateTime(slice.getStart(), this.timeZone).plus(offset).getMillis())
          .withEnd(new DateTime(slice.getEnd(), this.timeZone).plus(offset).getMillis()));
    }
    return slices;
  }

  private Map<MetricSlice, DataFrame> filter(MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    Map<MetricSlice, DataFrame> output = new HashMap<>();
    Set<MetricSlice> patterns = new HashSet<>(scatter(slice));

    for (Map.Entry<MetricSlice, DataFrame> entry : data.entrySet()) {
      if (patterns.contains(entry.getKey())) {
        output.put(entry.getKey(), entry.getValue());
      }
    }

    return output;
  }

  @Override
  public DataFrame gather(final MetricSlice slice, Map<MetricSlice, DataFrame> data) {
    Map<MetricSlice, DataFrame> filtered = this.filter(slice, data);

    DataFrame output = new DataFrame(COL_TIME, LongSeries.empty());

    List<String> colNames = new ArrayList<>();
    for (Map.Entry<MetricSlice, DataFrame> entry : filtered.entrySet()) {
      MetricSlice s = entry.getKey();

      Period period = new Period(
          new DateTime(slice.getStart(), this.timeZone),
          new DateTime(s.getStart(), this.timeZone),
          this.periodType);

      if (!offsets.contains(period)) {
        // throw new IllegalArgumentException(String.format("Found slice with invalid offset '%s'", period));
        continue;
      }

      String colName = String.valueOf(s.getStart());
      DataFrame df = new DataFrame(entry.getValue());

      DataFrame dfTransform = new DataFrame(df);
      dfTransform.addSeries(COL_TIME, this.toVirtualSeries(s.getStart(), dfTransform.getLongs(COL_TIME)));
      dfTransform = eliminateDuplicates(dfTransform);

      dfTransform.renameSeries(COL_VALUE, colName);

      if (output.isEmpty()) {
        // handle multi-index via prototyping
        output = dfTransform;

      } else {
        output = output.joinOuter(dfTransform);
      }

      colNames.add(colName);
    }

    String[] arrNames = colNames.toArray(new String[colNames.size()]);

    // aggregation
    output.addSeries(COL_VALUE, output.map(this.type.function, arrNames));

    // alignment
    output.addSeries(COL_TIME, this.toTimestampSeries(slice.getStart(), output.getLongs(COL_TIME)));

    // filter by original time range
    List<String> dropNames = new ArrayList<>(output.getSeriesNames());
    dropNames.removeAll(output.getIndexNames());

    output = output.filter(new Series.LongConditional() {
      @Override
      public boolean apply(long... values) {
        return values[0] >= slice.getStart() && values[0] < slice.getEnd();
      }
    }, COL_TIME).dropNull(output.getIndexNames());

    return output;
  }

  private static DataFrame eliminateDuplicates(DataFrame df) {
    List<String> aggExpressions = new ArrayList<>();
    for (String seriesName : df.getIndexNames()) {
      aggExpressions.add(String.format("%s:FIRST", seriesName));
    }
    aggExpressions.add(COL_VALUE + ":MEAN");

    DataFrame res = df.groupByValue(df.getIndexNames()).aggregate(aggExpressions).dropSeries(COL_KEY);

    return res.setIndex(df.getIndexNames());
  }

  /**
   * Returns an instance of BaselineAggregate for the specified type and offsets
   *
   * @see BaselineAggregateType
   *
   * @param type aggregation type
   * @param offsets time offsets
   * @return BaselineAggregate with given type and offsets
   */
  public static BaselineAggregate fromOffsets(BaselineAggregateType type, List<Period> offsets, DateTimeZone timeZone) {
    if (offsets.isEmpty()) {
      throw new IllegalArgumentException("Must provide at least one offset");
    }

    PeriodType periodType = offsets.get(0).getPeriodType();
    for (Period p : offsets) {
      if (!periodType.equals(p.getPeriodType())) {
        throw new IllegalArgumentException(String.format("Expected uniform period type but found '%s' and '%s'", periodType, p.getPeriodType()));
      }
    }

    return new BaselineAggregate(type, offsets, timeZone, periodType);
  }

  /**
   * Returns an instance of BaselineAggregate for the specified type and {@code numWeeks} offsets
   * computed on a consecutive week-over-week basis starting with a lag of {@code offsetWeeks}.
   * Additionally corrects for DST changes assuming a start date of {@code timestamp} in {@code timeZone}.
   * <br/><b>NOTE:</b> As offsets are pre-computed, the DST correction will produce incorrect offsets
   * if used to scatter a slice that does not start at {@code timestamp}.
   *
   * @see BaselineAggregate#fromWeekOverWeek(BaselineAggregateType, int, int, DateTimeZone)
   * @see BaselineAggregateType
   *
   * @param type aggregation type
   * @param numWeeks number of consecutive weeks
   * @param offsetWeeks lag for starting consecutive weeks
   * @param timeZone time zone
   * @return BaselineAggregate with given type and weekly offsets corrected for DST
   */
  public static BaselineAggregate fromWeekOverWeek(BaselineAggregateType type, int numWeeks, int offsetWeeks, DateTimeZone timeZone) {
    List<Period> offsets = new ArrayList<>();
    for (int i = 0; i < numWeeks; i++) {
      offsets.add(new Period(-1 * (i + offsetWeeks), PeriodType.weeks()));
    }
    return new BaselineAggregate(type, offsets, timeZone, PeriodType.weeks());
  }

  /**
   * Transform UTC timestamps into relative day-time-of-day timestamps
   *
   * @param origin origin timestamp
   * @param timestampSeries timestamp series
   * @return day-time-of-day series
   */
  private LongSeries toVirtualSeries(long origin, LongSeries timestampSeries) {
    final DateTime dateOrigin = new DateTime(origin, this.timeZone).withFields(makeOriginPartial());
    return timestampSeries.map(this.makeTimestampToVirtualFunction(dateOrigin));
  }

  /**
   * Transform day-time-of-day timestamps into UTC timestamps
   *
   * @param origin origin timestamp
   * @param virtualSeries day-time-of-day series
   * @return utc timestamp series
   */
  private LongSeries toTimestampSeries(long origin, LongSeries virtualSeries) {
    final DateTime dateOrigin = new DateTime(origin, this.timeZone).withFields(makeOriginPartial());
    return virtualSeries.map(this.makeVirtualToTimestampFunction(dateOrigin));
  }

  /**
   * Returns partial to zero out date fields based on period type
   *
   * @return partial
   */
  private Partial makeOriginPartial() {
    List<DateTimeFieldType> fields = new ArrayList<>();

    if (PeriodType.millis().equals(this.periodType)) {
      // left blank

    } else if (PeriodType.seconds().equals(this.periodType)) {
      fields.add(DateTimeFieldType.millisOfSecond());

    } else if (PeriodType.minutes().equals(this.periodType)) {
      fields.add(DateTimeFieldType.millisOfSecond());
      fields.add(DateTimeFieldType.secondOfMinute());

    } else if (PeriodType.hours().equals(this.periodType)) {
      fields.add(DateTimeFieldType.millisOfSecond());
      fields.add(DateTimeFieldType.secondOfMinute());
      fields.add(DateTimeFieldType.minuteOfHour());

    } else if (PeriodType.days().equals(this.periodType)
        || PeriodType.weeks().equals(this.periodType)) {
      fields.add(DateTimeFieldType.millisOfSecond());
      fields.add(DateTimeFieldType.secondOfMinute());
      fields.add(DateTimeFieldType.minuteOfHour());
      fields.add(DateTimeFieldType.hourOfDay());

    } else {
      throw new IllegalArgumentException(String.format("Unsupported PeriodType '%s'", this.periodType));
    }

    int[] zeros = new int[fields.size()];
    Arrays.fill(zeros, 0);

    return new Partial(fields.toArray(new DateTimeFieldType[fields.size()]), zeros);
  }

  /**
   * Returns a conversion function from utc timestamps to virtual, relative timestamps based
   * on period type and an origin
   *
   * @param origin origin to base relative timestamp on
   * @return LongFunction for converting to relative timestamps
   */
  private Series.LongFunction makeTimestampToVirtualFunction(final DateTime origin) {
    if (PeriodType.millis().equals(this.periodType)) {
      return new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          return values[0] - origin.getMillis();
        }
      };

    } else if (PeriodType.seconds().equals(this.periodType)) {
      return new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          DateTime dateTime = new DateTime(values[0], BaselineAggregate.this.timeZone);
          int seconds = new Period(origin, dateTime).getSeconds();
          int millis = dateTime.getMillisOfSecond();
          return seconds * 1000 + millis;
        }
      };

    } else if (PeriodType.minutes().equals(this.periodType)) {
      return new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          DateTime dateTime = new DateTime(values[0], BaselineAggregate.this.timeZone);
          int minutes = new Period(origin, dateTime).getMinutes();
          int seconds = dateTime.getSecondOfMinute();
          int millis = dateTime.getMillisOfSecond();
          return minutes * 100000 + seconds * 1000 + millis;
        }
      };

    } else if (PeriodType.hours().equals(this.periodType)) {
      return new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          DateTime dateTime = new DateTime(values[0], BaselineAggregate.this.timeZone);
          int hours = new Period(origin, dateTime).getHours();
          int minutes = dateTime.getMinuteOfHour();
          int seconds = dateTime.getSecondOfMinute();
          int millis = dateTime.getMillisOfSecond();
          return hours * 10000000 + minutes * 100000 + seconds * 1000 + millis;
        }
      };

    } else if (PeriodType.days().equals(this.periodType)
        || PeriodType.weeks().equals(this.periodType)) {
      return new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          DateTime dateTime = new DateTime(values[0], BaselineAggregate.this.timeZone);
          int days = new Period(origin, dateTime).getDays();
          int hours = dateTime.getHourOfDay();
          int minutes = dateTime.getMinuteOfHour();
          int seconds = dateTime.getSecondOfMinute();
          int millis = dateTime.getMillisOfSecond();
          return days * 1000000000 + hours * 10000000 + minutes * 100000 + seconds * 1000 + millis;
        }
      };

    } else {
      throw new IllegalArgumentException(String.format("Unsupported PeriodType '%s'", this.periodType));
    }
  }

  /**
   * Returns a conversion function from virtual, relative timestamps to UTC timestamps given
   * a period type and an origin
   *
   * @param origin origin to base absolute timestamps on
   * @return LongFunction for converting to UTC timestamps
   */
  private Series.LongFunction makeVirtualToTimestampFunction(final DateTime origin) {
    if (PeriodType.millis().equals(this.periodType)) {
      return new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          return values[0] + origin.getMillis();
        }
      };

    } else if (PeriodType.seconds().equals(this.periodType)) {
      return new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          int seconds = (int) (values[0] / 1000);
          int millis = (int) (values[0] % 1000);
          return origin
              .plusSeconds(seconds)
              .plusMillis(millis)
              .getMillis();
        }
      };

    } else if (PeriodType.minutes().equals(this.periodType)) {
      return new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          int minutes = (int) (values[0] / 100000);
          int seconds = (int) (values[0] / 1000 % 100000);
          int millis = (int) (values[0] % 1000);
          return origin
              .plusMinutes(minutes)
              .plusSeconds(seconds)
              .plusMillis(millis)
              .getMillis();
        }
      };

    } else if (PeriodType.hours().equals(this.periodType)) {
      return new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          int hours = (int) (values[0] / 10000000);
          int minutes = (int) (values[0] / 100000 % 10000000);
          int seconds = (int) (values[0] / 1000 % 100000);
          int millis = (int) (values[0] % 1000);
          return origin
              .plusHours(hours)
              .plusMinutes(minutes)
              .plusSeconds(seconds)
              .plusMillis(millis)
              .getMillis();
        }
      };

    } else if (PeriodType.days().equals(this.periodType)
        || PeriodType.weeks().equals(this.periodType)) {
      return new Series.LongFunction() {
        @Override
        public long apply(long... values) {
          int days = (int) (values[0] / 1000000000);
          int hours = (int) (values[0] / 10000000 % 1000000000);
          int minutes = (int) (values[0] / 100000 % 10000000);
          int seconds = (int) (values[0] / 1000 % 100000);
          int millis = (int) (values[0] % 1000);
          return origin
              .plusDays(days)
              .plusHours(hours)
              .plusMinutes(minutes)
              .plusSeconds(seconds)
              .plusMillis(millis)
              .getMillis();
        }
      };

    } else {
      throw new IllegalArgumentException(String.format("Unsupported PeriodType '%s'", this.periodType));
    }
  }
}
