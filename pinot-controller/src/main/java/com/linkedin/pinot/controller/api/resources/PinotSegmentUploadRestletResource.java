/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.resources;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.exception.InvalidConfigException;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import com.linkedin.pinot.common.metrics.ControllerMeter;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.common.segment.crypt.DefaultPinotCrypter;
import com.linkedin.pinot.common.segment.crypt.MultipartPinotCrypter;
import com.linkedin.pinot.common.segment.crypt.PinotCrypter;
import com.linkedin.pinot.common.segment.crypt.PinotCrypterFactory;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcher;
import com.linkedin.pinot.common.segment.fetcher.SegmentFetcherFactory;
import com.linkedin.pinot.core.metadata.DefaultMetadataProvider;
import com.linkedin.pinot.core.metadata.MetadataProviderFactory;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.common.utils.helix.HelixHelper;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.api.access.AccessControl;
import com.linkedin.pinot.controller.api.access.AccessControlFactory;
import com.linkedin.pinot.controller.api.upload.SegmentValidator;
import com.linkedin.pinot.controller.api.upload.ZKOperator;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import com.linkedin.pinot.controller.util.TableSizeReader;
import com.linkedin.pinot.controller.validation.StorageQuotaChecker;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.filesystem.PinotFS;
import com.linkedin.pinot.filesystem.PinotFSFactory;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.server.ManagedAsync;
import org.joda.time.Interval;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = Constants.SEGMENT_TAG)
@Path("/")
public class PinotSegmentUploadRestletResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentUploadRestletResource.class);

  @Inject
  PinotHelixResourceManager _pinotHelixResourceManager;

  @Inject
  ControllerConf _controllerConf;

  @Inject
  ControllerMetrics _controllerMetrics;

  @Inject
  HttpConnectionManager _connectionManager;

  @Inject
  Executor _executor;

  @Inject
  AccessControlFactory _accessControlFactory;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments")
  @Deprecated
  public String listAllSegmentNames() throws Exception {
    FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
    final JSONArray ret = new JSONArray();
    for (final File file : provider.getBaseDataDir().listFiles()) {
      final String fileName = file.getName();
      if (fileName.equalsIgnoreCase("fileUploadTemp") || fileName.equalsIgnoreCase("schemasTemp")) {
        continue;
      }

      final String url = _controllerConf.generateVipUrl() + "/segments/" + fileName;
      ret.put(url);
    }
    return ret.toString();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "Lists names of all segments of a table", notes = "Lists names of all segment names of a table")
  public String listAllSegmentNames(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline") @QueryParam("type") String tableTypeStr) throws Exception {
    JSONArray ret = new JSONArray();

    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableTypeStr == null) {
      ret.put(formatSegments(tableName, CommonConstants.Helix.TableType.OFFLINE));
      ret.put(formatSegments(tableName, CommonConstants.Helix.TableType.REALTIME));
    } else {
      ret.put(formatSegments(tableName, tableType));
    }
    return ret.toString();
  }

  @GET
  @Produces(MediaType.APPLICATION_OCTET_STREAM)
  @Path("/segments/{tableName}/{segmentName}")
  @ApiOperation(value = "Download a segment", notes = "Download a segment")
  public Response downloadSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @Context HttpHeaders httpHeaders) {
    // Validate data access
    boolean hasDataAccess;
    try {
      AccessControl accessControl = _accessControlFactory.create();
      hasDataAccess = accessControl.hasDataAccess(httpHeaders, tableName);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER,
          "Caught exception while validating access to table: " + tableName, Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    if (!hasDataAccess) {
      throw new ControllerApplicationException(LOGGER, "No data access to table: " + tableName,
          Response.Status.FORBIDDEN);
    }

    FileUploadPathProvider provider;
    try {
      provider = new FileUploadPathProvider(_controllerConf);
    } catch (Exception e) {
      throw new ControllerApplicationException(LOGGER, e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR, e);
    }
    try {
      segmentName = URLDecoder.decode(segmentName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      String errStr = "Could not decode segment name '" + segmentName + "'";
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST);
    }
    final File dataFile = new File(provider.getBaseDataDir(), StringUtil.join("/", tableName, segmentName));
    if (!dataFile.exists()) {
      throw new ControllerApplicationException(LOGGER,
          "Segment " + segmentName + " or table " + tableName + " not found", Response.Status.NOT_FOUND);
    }
    Response.ResponseBuilder builder = Response.ok(dataFile);
    builder.header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + dataFile.getName());
    builder.header(HttpHeaders.CONTENT_LENGTH, dataFile.length());
    return builder.build();
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}/{segmentName}")
  @ApiOperation(value = "Deletes a segment", notes = "Deletes a segment")
  public SuccessResponse deleteOneSegment(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "Name of the segment", required = true) @PathParam("segmentName") @Encoded String segmentName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type must not be null", Response.Status.BAD_REQUEST);
    }
    try {
      segmentName = URLDecoder.decode(segmentName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      String errStr = "Could not decode segment name '" + segmentName + "'";
      throw new ControllerApplicationException(LOGGER, errStr, Response.Status.BAD_REQUEST);
    }
    PinotSegmentRestletResource.toggleStateInternal(tableName, StateType.DROP, tableType, segmentName,
        _pinotHelixResourceManager);

    return new SuccessResponse("Segment deleted");
  }

  @DELETE
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/segments/{tableName}")
  @ApiOperation(value = "Deletes all segments of a table", notes = "Deletes all segments of a table")
  public SuccessResponse deleteAllSegments(
      @ApiParam(value = "Name of the table", required = true) @PathParam("tableName") String tableName,
      @ApiParam(value = "realtime|offline", required = true) @QueryParam("type") String tableTypeStr) {
    CommonConstants.Helix.TableType tableType = Constants.validateTableType(tableTypeStr);
    if (tableType == null) {
      throw new ControllerApplicationException(LOGGER, "Table type must not be null", Response.Status.BAD_REQUEST);
    }
    PinotSegmentRestletResource.toggleStateInternal(tableName, StateType.DROP, tableType, null,
        _pinotHelixResourceManager);

    return new SuccessResponse(
        "All segments of table " + TableNameBuilder.forType(tableType).tableNameWithType(tableName) + " deleted");
  }

  private SuccessResponse uploadSegment(FormDataMultiPart multiPart, boolean enableParallelPushProtection, HttpHeaders headers, Request request) throws Exception {
    if (headers != null) {
      // TODO: Add these headers into open source hadoop jobs
      LOGGER.info("HTTP Header {} is {}", CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER,
          headers.getRequestHeader(CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER));
      LOGGER.info("HTTP Header {} is {}", CommonConstants.Controller.TABLE_NAME_HTTP_HEADER,
          headers.getRequestHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER));
    }

    // Get upload type
    String uploadTypeStr = headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE);
    FileUploadDownloadClient.FileUploadType uploadType = getUploadType(uploadTypeStr);

    // Get crypter class
    String crypterClassHeader = headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.CRYPTER);

    // Get Download URI
    String downloadURI = headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI);

    File tempEncryptedFile = null;
    File tempDecryptedFile = null;
    File tempSegmentDir = null;
    try {
      FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
      String tempFileName = "tmp-" + System.nanoTime();
      tempEncryptedFile = new File(provider.getFileUploadTmpDir(), tempFileName + "_encrypted");
      tempDecryptedFile = new File(provider.getFileUploadTmpDir(), tempFileName);
      tempSegmentDir = new File(provider.getTmpUntarredPath(), tempFileName);

      // TODO: Change when metadata upload added
      String metadataProviderClass = DefaultMetadataProvider.class.getName();

      Object encryptedObject;
      switch (uploadType) {
        case URI:
          if (downloadURI == null || downloadURI.isEmpty()) {
            throw new ControllerApplicationException(LOGGER, "Failed to get downloadURI, needed for URI upload",
                Response.Status.BAD_REQUEST);
          }
          if (crypterClassHeader == null) {
            crypterClassHeader = DefaultPinotCrypter.class.getName();
          }
          encryptedObject = tempEncryptedFile;
          break;
        case TAR:
          // In the case of TAR, we will use local file
          if (crypterClassHeader == null) {
            crypterClassHeader = MultipartPinotCrypter.class.getName();
          }
          downloadURI = "multipart://dummy";
          encryptedObject = multiPart;
          break;
        default:
          throw new UnsupportedOperationException("Unsupported upload type: " + uploadType);

      }

      SegmentFetcherFactory.getInstance().getSegmentFetcherBasedOnURI(downloadURI).fetchSegmentToLocal(downloadURI, tempEncryptedFile);
      PinotCrypter pinotCrypter = PinotCrypterFactory.create(crypterClassHeader);
      pinotCrypter.init(_controllerConf.subset(CommonConstants.Segment.PREFIX_OF_CONFIG_OF_PINOT_CRYPTER));
      pinotCrypter.decrypt(encryptedObject, tempDecryptedFile);

      // Call metadata provider to extract metadata with file object uri
      SegmentMetadata segmentMetadata = MetadataProviderFactory.create(metadataProviderClass).extractMetadata(tempDecryptedFile, tempSegmentDir);

      String clientAddress = InetAddress.getByName(request.getRemoteAddr()).getHostName();
      String segmentName = segmentMetadata.getName();
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(segmentMetadata.getTableName());
      LOGGER.info("Processing upload request for segment: {} of table: {} from client: {}", segmentName,
          offlineTableName, clientAddress);

      // Validate segment
      new SegmentValidator(_pinotHelixResourceManager, _controllerConf, _executor, _connectionManager, _controllerMetrics)
          .validateSegment(segmentMetadata, tempSegmentDir);

      // Zk operations
      String finalSegmentPath = StringUtil.join("/", provider.getBaseDataDirURI().toString(), segmentMetadata.getTableName(),
          URLEncoder.encode(segmentName, "UTF-8"));
      URI finalSegmentLocationURI = new URI(finalSegmentPath);
      ZKOperator zkOperator = new ZKOperator(_pinotHelixResourceManager, _controllerConf, _controllerMetrics);
      zkOperator.completeSegmentOperations(segmentMetadata, finalSegmentLocationURI, tempDecryptedFile, enableParallelPushProtection, headers,
          provider);

      return new SuccessResponse("Successfully uploaded segment: " + segmentMetadata.getName() + " of table: " + segmentMetadata.getTableName());
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, "Caught internal server exception while uploading segment",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    } finally {
      FileUtils.deleteQuietly(tempEncryptedFile);
      FileUtils.deleteQuietly(tempDecryptedFile);
      FileUtils.deleteQuietly(tempSegmentDir);
    }
  }

@POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Path("/segments")
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as json")
  // We use this endpoint with URI upload because a request sent with the multipart content type will reject the POST
  // request if a multipart object is not sent
  public void uploadSegmentAsJson(String segmentJsonStr,
    @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false") @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION) boolean enableParallelPushProtection,
    @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
      asyncResponse.resume(uploadSegment(null, enableParallelPushProtection, headers, request));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

@POST
  @ManagedAsync
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("/segments")
  @ApiOperation(value = "Upload a segment", notes = "Upload a segment as binary")
  public void uploadSegmentAsMultiPart(FormDataMultiPart multiPart,
      @ApiParam(value = "Whether to enable parallel push protection") @DefaultValue("false") @QueryParam(FileUploadDownloadClient.QueryParameters.ENABLE_PARALLEL_PUSH_PROTECTION) boolean enableParallelPushProtection,
      @Context HttpHeaders headers, @Context Request request, @Suspended final AsyncResponse asyncResponse) {
    try {
       asyncResponse.resume(uploadSegment(multiPart, enableParallelPushProtection, headers, request));
    } catch (Throwable t) {
      asyncResponse.resume(t);
    }
  }

  private SuccessResponse uploadSegmentInternal(FormDataMultiPart multiPart, String segmentJsonStr,
      boolean enableParallelPushProtection, HttpHeaders headers, Request request, boolean isSegmentMetadataUpload) {
    File tempTarredFile = null;
    File tempSegmentDir = null;

    if (headers != null) {
      // TODO: Add these headers into open source hadoop jobs
      LOGGER.info("HTTP Header {} is {}", CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER,
          headers.getRequestHeader(CommonConstants.Controller.SEGMENT_NAME_HTTP_HEADER));
      LOGGER.info("HTTP Header {} is {}", CommonConstants.Controller.TABLE_NAME_HTTP_HEADER,
          headers.getRequestHeader(CommonConstants.Controller.TABLE_NAME_HTTP_HEADER));
    }

    try {
      FileUploadPathProvider provider = new FileUploadPathProvider(_controllerConf);
      // Note that these files could be either the segment file or the segment metadata zip containing only
      // creation.meta and metadata.properties
      String tempFileName = "tmp-" + System.nanoTime();
      tempTarredFile = new File(provider.getFileUploadTmpDir(), tempFileName);
      tempSegmentDir = new File(provider.getTmpUntarredPath(), tempFileName);

      // Get upload type
      String uploadTypeStr = headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE);
      FileUploadDownloadClient.FileUploadType uploadType = getUploadType(uploadTypeStr);

      SegmentMetadata segmentMetadata = null;
      String downloadURI = null;
      switch (uploadType) {
        case URI:
          // Get download URI
          try {
            downloadURI = getDownloadUri(uploadType, headers, segmentJsonStr);
          } catch (Exception e) {
            throw new ControllerApplicationException(LOGGER, "Failed to get download URI", Response.Status.BAD_REQUEST,
                e);
          }
          if (!isSegmentMetadataUpload) {
            // Get segment fetcher based on the download URI
            downloadSegment(tempTarredFile, downloadURI);
          } else {
            getFileFromMultipart(multiPart, tempTarredFile);
          }
          break;

        case TAR:
          getFileFromMultipart(multiPart, tempTarredFile);
          break;

        default:
          throw new UnsupportedOperationException("Unsupported upload type: " + uploadType);
      }

      // While there is TarGzCompressionUtils.unTarOneFile, we use unTar here to unpack all files
      // in the segment in order to ensure the segment is not corrupted
      TarGzCompressionUtils.unTar(tempTarredFile, tempSegmentDir);
      File[] files = tempSegmentDir.listFiles();
      Preconditions.checkState(files != null && files.length == 1);
      File indexDir = files[0];
      segmentMetadata = new SegmentMetadataImpl(indexDir);

      // For URI upload, user has the option to send us only metadata
      String segmentName = segmentMetadata.getName();
      String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(segmentMetadata.getTableName());
      String clientAddress = InetAddress.getByName(request.getRemoteAddr()).getHostName();
      LOGGER.info("Processing upload request for segment: {} of table: {} from client: {}", segmentName,
          offlineTableName, clientAddress);
      uploadSegment(indexDir, segmentMetadata, tempTarredFile, downloadURI, provider,
          enableParallelPushProtection, headers, isSegmentMetadataUpload);

      return new SuccessResponse("Successfully uploaded segment: " + segmentName + " of table: " + offlineTableName);
    } catch (WebApplicationException e) {
      throw e;
    } catch (Exception e) {
      _controllerMetrics.addMeteredGlobalValue(ControllerMeter.CONTROLLER_SEGMENT_UPLOAD_ERROR, 1L);
      throw new ControllerApplicationException(LOGGER, "Caught internal server exception while uploading segment",
          Response.Status.INTERNAL_SERVER_ERROR, e);
    } finally {
      FileUtils.deleteQuietly(tempTarredFile);
      FileUtils.deleteQuietly(tempSegmentDir);
    }
  }

  private File getFileFromMultipart(FormDataMultiPart multiPart, File dstFile) throws IOException {
    // Read segment file or segment metadata file and directly use that information to update zk
    Map<String, List<FormDataBodyPart>> segmentMetadataMap = multiPart.getFields();
    if (!validateMultiPart(segmentMetadataMap, null)) {
      throw new ControllerApplicationException(LOGGER, "Invalid multi-part form for segment metadata", Response.Status.BAD_REQUEST);
    }
    FormDataBodyPart segmentMetadataBodyPart = segmentMetadataMap.values().iterator().next().get(0);
    try (InputStream inputStream = segmentMetadataBodyPart.getValueAs(InputStream.class);
        OutputStream outputStream = new FileOutputStream(dstFile)) {
        IOUtils.copyLarge(inputStream, outputStream);
    }
    finally {
      multiPart.cleanup();
    }
    return dstFile;
  }

  private void downloadSegment(File tempTarredSegmentFile, String downloadURI) throws Exception {
    SegmentFetcher segmentFetcher;
    try {
      segmentFetcher = SegmentFetcherFactory.getInstance().getSegmentFetcherBasedOnURI(downloadURI);
    } catch (URISyntaxException e) {
      throw new ControllerApplicationException(LOGGER,
          "Caught exception while parsing download URI: " + downloadURI, Response.Status.BAD_REQUEST, e);
    }
    if (segmentFetcher == null) {
      throw new ControllerApplicationException(LOGGER,
          "Failed to get segment fetcher for download URI: " + downloadURI, Response.Status.BAD_REQUEST);
    }
    // Download segment tar file to local
    segmentFetcher.fetchSegmentToLocal(downloadURI, tempTarredSegmentFile);
  }

  private FileUploadDownloadClient.FileUploadType getUploadType(String uploadTypeStr) {
    if (uploadTypeStr != null) {
      return FileUploadDownloadClient.FileUploadType.valueOf(uploadTypeStr);
    } else {
      return FileUploadDownloadClient.FileUploadType.getDefaultUploadType();
    }
  }

  /**
   * Helper method to upload segment with the following steps:
   * <ul>
   *   <li>Check storage quota</li>
   *   <li>Check segment start/end time</li>
   *   <li>For new segment (non-refresh), directly add the segment</li>
   *   <li>
   *     For REFRESH case
   *     <ul>
   *       <li>Check IF-MATCH CRC if existing</li>
   *       <li>Lock the segment if parallel push protection enabled</li>
   *       <li>Update the custom map in segment ZK metadata</li>
   *       <li>Update the segment ZK metadata (this will also unlock the segment)</li>
   *     </ul>
   *   </li>
   * </ul>
   */
  private void uploadSegment(File indexDir, SegmentMetadata segmentMetadata, File tempTarredSegmentFile,
      String downloadUrl, FileUploadPathProvider provider, boolean enableParallelPushProtection, HttpHeaders headers, boolean isSegmentMetadataUpload)
      throws Exception {
    String rawTableName = segmentMetadata.getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(rawTableName);
    String segmentName = segmentMetadata.getName();
    TableConfig offlineTableConfig =
        ZKMetadataProvider.getOfflineTableConfig(_pinotHelixResourceManager.getPropertyStore(), offlineTableName);

    LOGGER.info("Starting upload for segment {}, tempTarredSegmentFile {}", segmentName,
        tempTarredSegmentFile.getAbsolutePath());

    if (offlineTableConfig == null) {
      throw new ControllerApplicationException(LOGGER, "Failed to find table config for table: " + offlineTableName,
          Response.Status.NOT_FOUND);
    }

    // Check quota when segment file is locally downloaded
    // For metadata-only upload, we have no way to check quota because we don't send the file itself and
    // we can't access length of zipped files in a remote filesystem
    if (isSegmentMetadataUpload) {
      StorageQuotaChecker.QuotaCheckerResponse quotaResponse;
      try {
        quotaResponse = checkStorageQuota(indexDir, segmentMetadata, offlineTableConfig);
      } catch (InvalidConfigException e) {
        // Admin port is missing, return response with 500 status code.
        throw new ControllerApplicationException(LOGGER,
            "Quota check failed for segment: " + segmentName + " of table: " + offlineTableName + ", reason: " + e.getMessage(), Response.Status.INTERNAL_SERVER_ERROR);
      }
      if (!quotaResponse.isSegmentWithinQuota) {
        throw new ControllerApplicationException(LOGGER,
            "Quota check failed for segment: " + segmentName + " of table: " + offlineTableName + ", reason: " + quotaResponse.reason, Response.Status.FORBIDDEN);
      }
    }

    // Check time range
    if (!isSegmentTimeValid(segmentMetadata)) {
      throw new ControllerApplicationException(LOGGER,
          "Invalid segment start/end time for segment: " + segmentName + " of table: " + offlineTableName,
          Response.Status.NOT_ACCEPTABLE);
    }

    ZNRecord znRecord = _pinotHelixResourceManager.getSegmentMetadataZnRecord(offlineTableName, segmentName);

    // Brand new segment, not refresh, directly add the segment
    if (znRecord == null) {
      LOGGER.info("Adding new segment: {}", segmentName);
      if (downloadUrl == null) {
        try {
          // Assumes local implementation because downloadUrl should be set in a header for non-local implementations
          downloadUrl = moveSegmentToPermanentDirectory(provider, rawTableName, segmentName, tempTarredSegmentFile.toURI());
        } catch (Exception e) {
          LOGGER.error("Could not move segment {} from table {} to permanent directory", segmentName, rawTableName);
          throw new RuntimeException(e);
        }
      }
      _pinotHelixResourceManager.addNewSegment(segmentMetadata, downloadUrl);
      return;
    }

    LOGGER.info("Segment {} already exists, refreshing if necessary", segmentName);

    OfflineSegmentZKMetadata existingSegmentZKMetadata = new OfflineSegmentZKMetadata(znRecord);
    long existingCrc = existingSegmentZKMetadata.getCrc();

    // Check if CRC match when IF-MATCH header is set
    String expectedCrcStr = headers.getHeaderString(HttpHeaders.IF_MATCH);
    if (expectedCrcStr != null) {
      long expectedCrc;
      try {
        expectedCrc = Long.parseLong(expectedCrcStr);
      } catch (NumberFormatException e) {
        throw new ControllerApplicationException(LOGGER,
            "Caught exception for segment: " + segmentName + " of table: " + offlineTableName
                + " while parsing IF-MATCH CRC: \"" + expectedCrcStr + "\"", Response.Status.PRECONDITION_FAILED);
      }
      if (expectedCrc != existingCrc) {
        throw new ControllerApplicationException(LOGGER,
            "For segment: " + segmentName + " of table: " + offlineTableName + ", expected CRC: " + expectedCrc
                + " does not match existing CRC: " + existingCrc, Response.Status.PRECONDITION_FAILED);
      }
    }

    // Check segment upload start time when parallel push protection enabled
    if (enableParallelPushProtection) {
      // When segment upload start time is larger than 0, that means another upload is in progress
      long segmentUploadStartTime = existingSegmentZKMetadata.getSegmentUploadStartTime();
      if (segmentUploadStartTime > 0) {
        if (System.currentTimeMillis() - segmentUploadStartTime > _controllerConf.getSegmentUploadTimeoutInMillis()) {
          // Last segment upload does not finish properly, replace the segment
          LOGGER.error("Segment: {} of table: {} was not properly uploaded, replacing it", segmentName,
              offlineTableName);
          _controllerMetrics.addMeteredGlobalValue(ControllerMeter.NUMBER_SEGMENT_UPLOAD_TIMEOUT_EXCEEDED, 1L);
        } else {
          // Another segment upload is in progress
          throw new ControllerApplicationException(LOGGER,
              "Another segment upload is in progress for segment: " + segmentName + " of table: " + offlineTableName
                  + ", retry later", Response.Status.CONFLICT);
        }
      }

      // Lock the segment by setting the upload start time in ZK
      existingSegmentZKMetadata.setSegmentUploadStartTime(System.currentTimeMillis());
      if (!_pinotHelixResourceManager.updateZkMetadata(existingSegmentZKMetadata, znRecord.getVersion())) {
        throw new ControllerApplicationException(LOGGER,
            "Failed to lock the segment: " + segmentName + " of table: " + offlineTableName + ", retry later",
            Response.Status.CONFLICT);
      }
    }

    // Reset segment upload start time to unlock the segment later
    // NOTE: reset this value even if parallel push protection is not enabled so that segment can recover in case
    // previous segment upload did not finish properly and the parallel push protection is turned off
    existingSegmentZKMetadata.setSegmentUploadStartTime(-1);

    try {
      // Modify the custom map in segment ZK metadata
      String segmentZKMetadataCustomMapModifierStr =
          headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.SEGMENT_ZK_METADATA_CUSTOM_MAP_MODIFIER);
      SegmentZKMetadataCustomMapModifier segmentZKMetadataCustomMapModifier;
      if (segmentZKMetadataCustomMapModifierStr != null) {
        segmentZKMetadataCustomMapModifier =
            new SegmentZKMetadataCustomMapModifier(segmentZKMetadataCustomMapModifierStr);
      } else {
        // By default, use REPLACE modify mode
        segmentZKMetadataCustomMapModifier =
            new SegmentZKMetadataCustomMapModifier(SegmentZKMetadataCustomMapModifier.ModifyMode.REPLACE, null);
      }
      existingSegmentZKMetadata.setCustomMap(
          segmentZKMetadataCustomMapModifier.modifyMap(existingSegmentZKMetadata.getCustomMap()));

      // Update ZK metadata and refresh the segment if necessary
      long newCrc = Long.valueOf(segmentMetadata.getCrc());
      if (newCrc == existingCrc) {
        LOGGER.info("New segment crc {} is same as existing segment crc {} for segment {}. Updating ZK metadata without refreshing the segment {}",
            newCrc, existingCrc, segmentName);
        if (!_pinotHelixResourceManager.updateZkMetadata(existingSegmentZKMetadata)) {
          throw new RuntimeException(
              "Failed to update ZK metadata for segment: " + segmentName + " of table: " + offlineTableName);
        }
      } else {
        // New segment is different with the existing one, update ZK metadata and refresh the segment
        LOGGER.info("New segment crc {} is different than the existing segment crc {}. Updating ZK metadata and refreshing segment {}",
            newCrc, existingCrc, segmentName);
        if (downloadUrl == null) {
          URI currentLocation = tempTarredSegmentFile.toURI();
          downloadUrl = moveSegmentToPermanentDirectory(provider, rawTableName, segmentName, currentLocation);
        }
        _pinotHelixResourceManager.refreshSegment(segmentMetadata, existingSegmentZKMetadata, downloadUrl);
      }
    } catch (Exception e) {
      if (!_pinotHelixResourceManager.updateZkMetadata(existingSegmentZKMetadata)) {
        LOGGER.error("Failed to update ZK metadata for segment: {} of table: {}", segmentName, offlineTableName);
      }
      throw e;
    }
  }

  private String moveSegmentToPermanentDirectory(FileUploadPathProvider provider, String tableName, String segmentName,
      URI srcUri) throws Exception {
    // Move tarred segment file to data directory when there is no external download URL
    File tarredSegmentFile = new File(new File(provider.getBaseDataDir(), tableName), segmentName);
    PinotFS pinotFS = PinotFSFactory.create(srcUri.getScheme());
    // The move will overwrite current segment file
    pinotFS.move(srcUri, tarredSegmentFile.toURI());
    LOGGER.info("Moved segment {} from temp location {} to {}", segmentName, srcUri.getPath(), tarredSegmentFile.getAbsolutePath());
    return ControllerConf.constructDownloadUrl(tableName, segmentName, provider.getVip());
  }

  private String getDownloadUri(FileUploadDownloadClient.FileUploadType uploadType, HttpHeaders headers,
      String segmentJsonStr) throws Exception {
    switch (uploadType) {
      case URI:
        return headers.getHeaderString(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI);
      default:
        break;
    }
    throw new UnsupportedOperationException("Not support getDownloadUri method for upload type - " + uploadType);
  }

  private org.json.JSONObject formatSegments(String tableName, CommonConstants.Helix.TableType tableType)
      throws Exception {
    org.json.JSONObject obj = new org.json.JSONObject();
    obj.put(tableType.toString(), getSegments(tableName, tableType.toString()));
    return obj;
  }

  private JSONArray getSegments(String tableName, String tableType) {

    final JSONArray ret = new JSONArray();

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);

    String tableNameWithType;
    if (CommonConstants.Helix.TableType.valueOf(tableType).toString().equals("REALTIME")) {
      tableNameWithType = realtimeTableName;
    } else {
      tableNameWithType = offlineTableName;
    }

    List<String> segmentList = _pinotHelixResourceManager.getSegmentsFor(tableNameWithType);
    IdealState idealState =
        HelixHelper.getTableIdealState(_pinotHelixResourceManager.getHelixZkManager(), tableNameWithType);

    for (String segmentName : segmentList) {
      Map<String, String> map = idealState.getInstanceStateMap(segmentName);
      if (map == null) {
        continue;
      }
      if (!map.containsValue(PinotHelixSegmentOnlineOfflineStateModelGenerator.OFFLINE_STATE)) {
        ret.put(segmentName);
      }
    }

    return ret;
  }

  // Validate that there is one file that is in the input.
  public static boolean validateMultiPart(Map<String, List<FormDataBodyPart>> map, String segmentName) {
    boolean isGood = true;
    if (map.size() != 1) {
      LOGGER.warn("Incorrect number of multi-part elements: {} (segmentName {}). Picking one", map.size(), segmentName);
      isGood = false;
    }
    List<FormDataBodyPart> bodyParts = map.get(map.keySet().iterator().next());
    if (bodyParts.size() != 1) {
      LOGGER.warn("Incorrect number of elements in list in first part: {} (segmentName {}). Picking first one",
          bodyParts.size(), segmentName);
      isGood = false;
    }
    return isGood;
  }

  /**
   * check if the segment represented by segmentFile is within the storage quota
   * @param segmentFile untarred segment. This should not be null.
   *                    segmentFile must exist on disk and must be a directory
   * @param metadata segment metadata. This should not be null.
   * @param offlineTableConfig offline table configuration. This should not be null.
   */
  private StorageQuotaChecker.QuotaCheckerResponse checkStorageQuota(@Nonnull File segmentFile,
      @Nonnull SegmentMetadata metadata, @Nonnull TableConfig offlineTableConfig) throws InvalidConfigException {
    if (!_controllerConf.getEnableStorageQuotaCheck()) {
      return StorageQuotaChecker.success("Quota check is disabled");
    }
    TableSizeReader tableSizeReader = new TableSizeReader(_executor, _connectionManager,
        _controllerMetrics, _pinotHelixResourceManager);
    StorageQuotaChecker quotaChecker = new StorageQuotaChecker(offlineTableConfig, tableSizeReader, _controllerMetrics, _pinotHelixResourceManager);
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(metadata.getTableName());
    return quotaChecker.isSegmentStorageWithinQuota(segmentFile, offlineTableName, metadata.getName(),
        _controllerConf.getServerAdminRequestTimeoutSeconds() * 1000);
  }

  /**
   * Returns true if:
   * - Segment does not have a start/end time, OR
   * - The start/end time are in a valid range (Jan 01 1971 - Jan 01, 2071)
   * @param metadata Segment metadata
   * @return
   */
  private boolean isSegmentTimeValid(SegmentMetadata metadata) {
    Interval interval = metadata.getTimeInterval();
    if (interval == null) {
      return true;
    }

    long startMillis = interval.getStartMillis();
    long endMillis = interval.getEndMillis();

    if (!TimeUtils.timeValueInValidRange(startMillis) || !TimeUtils.timeValueInValidRange(endMillis)) {
      Date minDate = new Date(TimeUtils.getValidMinTimeMillis());
      Date maxDate = new Date(TimeUtils.getValidMaxTimeMillis());

      LOGGER.error(
          "Invalid start time '{}ms' or end time '{}ms' for segment {}, must be between '{}' and '{}' (timecolumn {}, timeunit {})",
          interval.getStartMillis(), interval.getEndMillis(), metadata.getName(), minDate, maxDate,
          metadata.getTimeColumn(), metadata.getTimeUnit().toString());
      return false;
    }

    return true;
  }
}
