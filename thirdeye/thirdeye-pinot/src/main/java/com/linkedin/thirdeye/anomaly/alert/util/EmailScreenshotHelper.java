package com.linkedin.thirdeye.anomaly.alert.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import jersey.repackaged.com.google.common.base.Joiner;
import jersey.repackaged.com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;

public class EmailScreenshotHelper {

  private static final Logger LOG = LoggerFactory.getLogger(EmailScreenshotHelper.class);

  public static String takeGraphScreenShot(String anomalyId, ThirdEyeAnomalyConfiguration configuration) {
    String imgPath = null;
    try {

      String imgRoute = configuration.getDashboardHost() + "/thirdeye#anomalies?anomaliesSearchMode=id&pageNumber=1&anomalyIds=" + anomalyId;
      String phantomScript = configuration.getRootDir() + "/getGraphPnj.js";
      System.out.println("Phantom Script : " + phantomScript);

      imgPath = "/tmp/graph" + anomalyId +".png";
      String[] cmdarray = new String[]{configuration.getPhantomJsPath(), "phantomjs", "--ssl-protocol=any", "--ignore-ssl-errors=true",
              phantomScript, imgRoute, imgPath};
      System.out.println(Joiner.on(" ").join(Lists.newArrayList(cmdarray)));
      Process proc = Runtime.getRuntime().exec(
                    cmdarray);

      InputStream stderr = proc.getErrorStream();
      InputStreamReader stderrReader = new InputStreamReader(stderr);
      InputStreamReader isr = new InputStreamReader(stderr);
      BufferedReader br = new BufferedReader(isr);
      // exhaust the error stream before waiting for the process to exit
      String line = br.readLine();
      if (line != null) {
        do {
          line = br.readLine();
        } while (line != null);
      }
      int exitVal = proc.waitFor();
      if (exitVal != 0) {

        throw new Exception("PhantomJS process failed with error code: " + exitVal);
      }
    } catch (Exception e) {
      LOG.error("Exception with openPageInPhantom", e);
    }
    return imgPath;
  }

}
