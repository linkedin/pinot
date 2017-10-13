package com.linkedin.thirdeye.datalayer.bao;

import com.linkedin.thirdeye.datalayer.DaoProvider;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAlertConfigManager{

  Long alertConfigid;

  private DaoProvider testDAOProvider;
  private AlertConfigManager alertConfigDAO;
  @BeforeClass
  void beforeClass() {
    testDAOProvider = DAOTestBase.getInstance();
    alertConfigDAO = testDAOProvider.getAlertConfigDAO();
  }

  @AfterClass(alwaysRun = true)
  void afterClass() {
    testDAOProvider.restart();
  }

  @Test
  public void testCreateAlertConfig() {
    AlertConfigDTO request = new AlertConfigDTO();
    request.setActive(true);
    request.setName("my alert config");
    alertConfigid = alertConfigDAO.save(request);
    Assert.assertTrue(alertConfigid > 0);
  }

  @Test (dependsOnMethods = {"testCreateAlertConfig"})
  public void testFetchAlertConfig() {
    // find by id
    AlertConfigDTO response = alertConfigDAO.findById(alertConfigid);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getId(), alertConfigid);
    Assert.assertEquals(alertConfigDAO.findAll().size(), 1);
  }

  @Test (dependsOnMethods = {"testFetchAlertConfig"})
  public void testDeleteAlertConfig() {
    alertConfigDAO.deleteById(alertConfigid);
    Assert.assertEquals(alertConfigDAO.findAll().size(), 0);
  }
}
