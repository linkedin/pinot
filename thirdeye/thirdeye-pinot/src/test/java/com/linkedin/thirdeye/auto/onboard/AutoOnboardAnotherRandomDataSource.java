package com.linkedin.thirdeye.auto.onboard;

import com.linkedin.thirdeye.datasource.DataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AutoOnboardAnotherRandomDataSource extends AutoOnboard {

  private static final Logger LOG = LoggerFactory.getLogger(AutoOnboardAnotherRandomDataSource.class);

  public AutoOnboardAnotherRandomDataSource(DataSourceConfig dataSourceConfig) {
    super(dataSourceConfig);
  }

  @Override
  public void run() {
    throw new RuntimeException("There was an exception while executing this Source");
  }

  @Override
  public void runAdhoc() {

  }
}
