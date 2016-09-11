package com.linkedin.thirdeye.datalayer.entity;

public class IngraphMetricConfigIndex extends AbstractIndexEntity {
  String name;
  String alias;

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}
