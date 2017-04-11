package com.linkedin.pinot.controller.api.restlet.resources;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Parameter;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.restlet.swagger.Tags;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.controller.api.ControllerRestApplication;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.restlet.data.Status;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotTableIndexingConfigs extends BasePinotControllerRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableIndexingConfigs.class);
  private final File baseDataDir;
  private final File tempDir;

  public PinotTableIndexingConfigs() throws IOException {
    baseDataDir = new File(_controllerConf.getDataDir());
    if (!baseDataDir.exists()) {
      FileUtils.forceMkdir(baseDataDir);
    }
    tempDir = new File(baseDataDir, "schemasTemp");
    if (!tempDir.exists()) {
      FileUtils.forceMkdir(tempDir);
    }
  }

  @Deprecated
  @Override
  @Put("json")
  public Representation put(Representation entity) {
    final String tableName = (String) getRequest().getAttributes().get("tableName");
    if (tableName == null) {
      String error = new String("Error: Table " + tableName + " not found.");
      LOGGER.error(error);
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation(error);
    }
    if (entity == null) {
      setStatus(Status.CLIENT_ERROR_BAD_REQUEST);
      return new StringRepresentation("{\"error\" : \"Request body is required\"}");
    }
    try {
      return updateIndexingConfig(tableName, entity);
    } catch (PinotHelixResourceManager.InvalidTableConfigException e) {
      LOGGER.info("Failed to update metadata configuration for table {}, error: {}", tableName, e.getMessage());
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(
          ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      return errorResponseRepresentation(Status.CLIENT_ERROR_BAD_REQUEST, e.getMessage());
    } catch (final Exception e) {
      LOGGER.error("Caught exception while updating indexing configs for table {}", tableName, e);
      ControllerRestApplication.getControllerMetrics().addMeteredGlobalValue(ControllerMeter.CONTROLLER_TABLE_UPDATE_ERROR, 1L);
      setStatus(Status.SERVER_ERROR_INTERNAL);
      return PinotSegmentUploadRestletResource.exceptionToStringRepresentation(e);
    }
  }

  @Deprecated
  @HttpVerb("put")
  @Summary("DEPRECATED: Updates the indexing configuration for a table")
  @Tags({"table"})
  @Paths({
      "/tables/{tableName}/indexingConfigs"
  })
  private Representation updateIndexingConfig(
      @Parameter(name = "tableName", in = "path", description = "The name of the table for which to update the indexing configuration", required = true)
      String tableName,
      Representation entity)
      throws Exception {
    AbstractTableConfig config = AbstractTableConfig.init(entity.getText());
    _pinotHelixResourceManager.updateIndexingConfigFor(config.getTableName(), TableType.valueOf(config.getTableType().toUpperCase()),
        config.getIndexingConfig());
    return new StringRepresentation("done");
  }
}
