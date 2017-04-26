package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


/**
 * DimensionEntity represents a data dimension (a cut) across multiple metrics. It is identified
 * by a key-value pair. Note, that dimension names may require standardization across different
 * metrics.
 */
public class DimensionEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:dimension:");

  public static DimensionEntity fromDimension(double score, String name, String value) {
    return new DimensionEntity(TYPE.formatURN(name, value), score, name, value);
  }

  private final String name;
  private final String value;

  public static DimensionEntity fromURN(String urn, double score) {
    String[] parts = urn.split(":");
    if(parts.length != 4)
      throw new IllegalArgumentException(String.format("Dimension URN must have 4 parts but has '%s'", parts.length));
    return fromDimension(score, parts[2], parts[3]);
  }

  protected DimensionEntity(String urn, double score, String name, String value) {
    super(urn, score);
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }
}
