/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.view;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

/**
 * Stores mv schema in disk as json format
 */
@InterfaceAudience.Internal
public class MaterializedViewProvider {

  private static final Logger LOG = LogServiceFactory.getLogService(
      MaterializedViewProvider.class.getCanonicalName());

  private static final String STATUS_FILE_NAME = "mv_status";

  private final String storeLocation;

  private final Map<String, SchemaProvider> schemaProviders = new ConcurrentHashMap<>();

  private MaterializedViewProvider(String storeLocation) {
    this.storeLocation = storeLocation;
  }

  public static MaterializedViewProvider get() {
    String storeLocation =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION);
    if (storeLocation == null) {
      throw new RuntimeException(
          "Property [" + CarbonCommonConstants.STORE_LOCATION + "] is not set.");
    }
    return new MaterializedViewProvider(storeLocation);
  }

  private static String getSchemaPath(String schemaRoot, String viewName) {
    return schemaRoot + CarbonCommonConstants.FILE_SEPARATOR + "mv_schema." + viewName;
  }

  private SchemaProvider getSchemaProvider(String databaseName) {
    String databaseNameUpper = databaseName.toUpperCase();
    SchemaProvider schemaProvider = this.schemaProviders.get(databaseNameUpper);
    if (schemaProvider == null) {
      synchronized (this.schemaProviders) {
        schemaProvider = this.schemaProviders.get(databaseNameUpper);
        if (schemaProvider == null) {
          String databaseLocation;
          if (databaseNameUpper.equalsIgnoreCase("default")) {
            databaseLocation = CarbonUtil.checkAndAppendHDFSUrl(this.storeLocation);
          } else {
            databaseLocation = CarbonUtil.checkAndAppendHDFSUrl(this.storeLocation +
                CarbonCommonConstants.FILE_SEPARATOR + databaseName + ".db");
          }
          if (!FileFactory.getCarbonFile(databaseLocation).exists()) {
            return null;
          }
          schemaProvider = new SchemaProvider(databaseLocation);
          this.schemaProviders.put(databaseNameUpper, schemaProvider);
        }
      }
    }
    return schemaProvider;
  }

  public MaterializedViewSchema getSchema(MaterializedViewManager viewManager,
      String databaseName, String viewName) throws IOException {
    SchemaProvider schemaProvider = this.getSchemaProvider(databaseName);
    if (schemaProvider == null) {
      return null;
    }
    return schemaProvider.retrieveSchema(viewManager, viewName);
  }

  List<MaterializedViewSchema> getSchemas(MaterializedViewManager viewManager,
      String databaseName, CarbonTable carbonTable) throws IOException {
    SchemaProvider schemaProvider = this.getSchemaProvider(databaseName);
    if (schemaProvider == null) {
      return Collections.emptyList();
    } else {
      return schemaProvider.retrieveSchemas(viewManager, carbonTable);
    }
  }

  List<MaterializedViewSchema> getSchemas(MaterializedViewManager viewManager,
      String databaseName) throws IOException {
    SchemaProvider schemaProvider = this.getSchemaProvider(databaseName);
    if (schemaProvider == null) {
      return Collections.emptyList();
    } else {
      return schemaProvider.retrieveAllSchemas(viewManager);
    }
  }

  void saveSchema(MaterializedViewManager viewManager, String databaseName,
      MaterializedViewSchema viewSchema) throws IOException {
    SchemaProvider schemaProvider = this.getSchemaProvider(databaseName);
    if (schemaProvider == null) {
      throw new IOException("Database [" + databaseName + "] is not found.");
    }
    schemaProvider.saveSchema(viewManager, viewSchema);
  }

  public void dropSchema(String databaseName, String viewName)
      throws IOException {
    SchemaProvider schemaProvider = this.getSchemaProvider(databaseName);
    if (schemaProvider == null) {
      throw new IOException("Materialized view with name " + databaseName + "." + viewName +
          " does not exists in storage");
    }
    schemaProvider.dropSchema(viewName);
  }

  private String getStatusFileName(String databaseName) {
    if (databaseName.equalsIgnoreCase("default")) {
      return this.storeLocation +
          CarbonCommonConstants.FILE_SEPARATOR + "_system" +
          CarbonCommonConstants.FILE_SEPARATOR + STATUS_FILE_NAME;
    } else {
      return this.storeLocation +
          CarbonCommonConstants.FILE_SEPARATOR + databaseName + ".db" +
          CarbonCommonConstants.FILE_SEPARATOR + "_system" +
          CarbonCommonConstants.FILE_SEPARATOR + STATUS_FILE_NAME;
    }
  }

  public List<MaterializedViewStatusDetail> getStatusDetails(String databaseName)
      throws IOException {
    String statusPath = this.getStatusFileName(databaseName);
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    MaterializedViewStatusDetail[] statusDetails;
    try {
      if (!FileFactory.isFileExist(statusPath)) {
        return Collections.emptyList();
      }
      dataInputStream = FileFactory.getDataInputStream(statusPath);
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      statusDetails = gsonObjectToRead.fromJson(buffReader,
          MaterializedViewStatusDetail[].class);
    } catch (IOException e) {
      LOG.error("Failed to read datamap status", e);
      throw e;
    } finally {
      CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
    }
    // if status details is null, return empty array
    if (null == statusDetails) {
      return Collections.emptyList();
    }
    return Arrays.asList(statusDetails);
  }

  private static ICarbonLock getStatusLock(String databaseName) {
    return CarbonLockFactory
        .getSystemLevelCarbonLockObj(
            CarbonProperties.getInstance().getSystemFolderLocation(databaseName),
            LockUsage.MATERIALIZED_VIEW_STATUS_LOCK);
  }

  /**
   * Update or add the status of passed mvs with the given mv status. If the mv status
   * given is enabled/disabled then updates/adds the mv, in case of drop it just removes it
   * from the file.
   * This method always overwrites the old file.
   * @param schemaList schemas of which are need to be updated in mv status
   * @param status  status to be updated for the mv schemas
   */
  public void updateStatus(List<MaterializedViewSchema> schemaList,
      MaterializedViewStatus status) throws IOException {
    if (schemaList == null || schemaList.size() == 0) {
      // There is nothing to update
      return;
    }
    final Map<String, List<MaterializedViewSchema>> schemasMapByDatabase = new HashMap<>();
    for (MaterializedViewSchema schema : schemaList) {
      String databaseName = schema.getIdentifier().getDatabaseName().toLowerCase();
      List<MaterializedViewSchema> schemas = schemasMapByDatabase.get(databaseName);
      if (schemas == null) {
        schemas = new ArrayList<>();
        schemasMapByDatabase.put(databaseName, schemas);
      }
      schemas.add(schema);
    }
    for (Map.Entry<String, List<MaterializedViewSchema>> entry : schemasMapByDatabase.entrySet()) {
      this.updateStatus(entry.getKey(), entry.getValue(), status);
    }
  }

  private void updateStatus(String databaseName, List<MaterializedViewSchema> schemaList,
      MaterializedViewStatus status) throws IOException {
    ICarbonLock carbonTableStatusLock = getStatusLock(databaseName);
    boolean locked = false;
    try {
      locked = carbonTableStatusLock.lockWithRetries();
      if (locked) {
        LOG.info("Materialized view status lock has been successfully acquired.");
        if (status == MaterializedViewStatus.ENABLED) {
          // Enable mv only if mv tables and main table are in sync
          if (!isViewCanBeEnabled(schemaList.get(0))) {
            return;
          }
        }
        List<MaterializedViewStatusDetail> statusDetailList =
            new ArrayList<>(getStatusDetails(databaseName));
        List<MaterializedViewStatusDetail> changedStatusDetails = new ArrayList<>();
        List<MaterializedViewStatusDetail> newStatusDetails = new ArrayList<>();
        for (MaterializedViewSchema schema : schemaList) {
          boolean exists = false;
          for (MaterializedViewStatusDetail statusDetail : statusDetailList) {
            if (statusDetail.getIdentifier().equals(schema.getIdentifier())) {
              statusDetail.setStatus(status);
              changedStatusDetails.add(statusDetail);
              exists = true;
            }
          }
          if (!exists) {
            newStatusDetails
                .add(new MaterializedViewStatusDetail(schema.getIdentifier(),
                    status));
          }
        }
        // Add the newly added datamaps to the list.
        if (newStatusDetails.size() > 0 &&
            status != MaterializedViewStatus.DROPPED) {
          statusDetailList.addAll(newStatusDetails);
        }
        // In case of dropped datamap, just remove from the list.
        if (status == MaterializedViewStatus.DROPPED) {
          statusDetailList.removeAll(changedStatusDetails);
        }
        writeLoadDetailsIntoFile(
            this.getStatusFileName(databaseName),
            statusDetailList.toArray(
                new MaterializedViewStatusDetail[statusDetailList.size()]));
      } else {
        String errorMsg = "Upadating datamapstatus is failed due to another process taken the lock"
            + " for updating it";
        LOG.error(errorMsg);
        throw new IOException(errorMsg + " Please try after some time.");
      }
    } finally {
      if (locked) {
        CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.DATAMAP_STATUS_LOCK);
      }
    }
  }

  /**
   * writes mv status details
   */
  private static void writeLoadDetailsIntoFile(String location,
      MaterializedViewStatusDetail[] statusDetails) throws IOException {
    FileFactory.touchFile(FileFactory.getCarbonFile(location),
        new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    AtomicFileOperations fileWrite = AtomicFileOperationFactory.getAtomicFileOperations(location);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the mv status file.
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(statusDetails);
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      LOG.error("Error message: " + ioe.getLocalizedMessage());
      fileWrite.setFailed();
      throw ioe;
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }

  }

  /**
   * This method checks if main table and mv table are synchronised or not. If synchronised
   * return true to enable the mv
   *
   * @param schema of mv to be disabled or enabled
   * @return flag to enable or disable mv
   * @throws IOException
   */
  private static boolean isViewCanBeEnabled(MaterializedViewSchema schema)
      throws IOException {
    if (!schema.isRefreshIncremental()) {
      return true;
    }
    boolean isViewCanBeEnabled = true;
    String viewMetadataPath =
        CarbonTablePath.getMetadataPath(schema.getIdentifier().getTablePath());
    LoadMetadataDetails[] viewLoadMetadataDetails =
        SegmentStatusManager.readLoadMetadata(viewMetadataPath);
    Map<String, List<String>> viewSegmentMap = new HashMap<>();
    for (LoadMetadataDetails loadMetadataDetail : viewLoadMetadataDetails) {
      if (loadMetadataDetail.getSegmentStatus() == SegmentStatus.SUCCESS) {
        Map<String, List<String>> segmentMap =
            new Gson().fromJson(loadMetadataDetail.getExtraInfo(), Map.class);
        if (viewSegmentMap.isEmpty()) {
          viewSegmentMap.putAll(segmentMap);
        } else {
          for (Map.Entry<String, List<String>> entry : segmentMap.entrySet()) {
            if (null != viewSegmentMap.get(entry.getKey())) {
              viewSegmentMap.get(entry.getKey()).addAll(entry.getValue());
            }
          }
        }
      }
    }
    List<RelationIdentifier> associatedTables = schema.getAssociatedTables();
    for (RelationIdentifier associatedTable : associatedTables) {
      List<String> associatedTableSegmentList =
          SegmentStatusManager.getValidSegmentList(associatedTable);
      if (!associatedTableSegmentList.isEmpty()) {
        if (viewSegmentMap.isEmpty()) {
          isViewCanBeEnabled = false;
        } else {
          isViewCanBeEnabled = viewSegmentMap.get(
              associatedTable.getDatabaseName() + CarbonCommonConstants.POINT +
                  associatedTable.getTableName()).containsAll(associatedTableSegmentList);
        }
      }
    }
    return isViewCanBeEnabled;
  }

  /**
   * Data map schema provider of a database.
   */
  private static final class SchemaProvider {

    private String systemDirectory;

    private String schemaIndexFilePath;

    private long lastModifiedTime;

    private Set<MaterializedViewSchema> schemas = new HashSet<>();

    SchemaProvider(String databaseLocation) {
      final String systemDirectory =
          databaseLocation + CarbonCommonConstants.FILE_SEPARATOR + "_system";
      this.systemDirectory = systemDirectory;
      this.schemaIndexFilePath = systemDirectory + CarbonCommonConstants.FILE_SEPARATOR +
          "mv_schema_index";
    }

    void saveSchema(MaterializedViewManager viewManager, MaterializedViewSchema viewSchema)
        throws IOException {
      BufferedWriter brWriter = null;
      DataOutputStream dataOutputStream = null;
      Gson gsonObjectToWrite = new Gson();
      String schemaPath =
          getSchemaPath(this.systemDirectory, viewSchema.getIdentifier().getTableName());
      if (FileFactory.isFileExist(schemaPath)) {
        throw new IOException(
            "Materialized view with name " + viewSchema.getIdentifier().getTableName() +
                " already exists in storage");
      }
      // write the mv schema in json format.
      try {
        FileFactory.mkdirs(this.systemDirectory);
        FileFactory.createNewFile(schemaPath);
        dataOutputStream =
            FileFactory.getDataOutputStream(schemaPath);
        brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
            Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

        String metadataInstance = gsonObjectToWrite.toJson(viewSchema);
        brWriter.write(metadataInstance);
      } finally {
        if (null != brWriter) {
          brWriter.flush();
        }
        this.schemas.add(viewSchema);
        CarbonUtil.closeStreams(dataOutputStream, brWriter);
        checkAndReloadSchemas(viewManager, true);
        touchMDTFile();
      }
    }

    MaterializedViewSchema retrieveSchema(MaterializedViewManager viewManager, String viewName)
        throws IOException {
      checkAndReloadSchemas(viewManager, true);
      for (MaterializedViewSchema schema : this.schemas) {
        if (schema.getIdentifier().getTableName().equalsIgnoreCase(viewName)) {
          return schema;
        }
      }
      return null;
    }

    List<MaterializedViewSchema> retrieveSchemas(MaterializedViewManager viewManager,
        CarbonTable carbonTable) throws IOException {
      checkAndReloadSchemas(viewManager, false);
      List<MaterializedViewSchema> schemas = new ArrayList<>();
      for (MaterializedViewSchema schema : this.schemas) {
        List<RelationIdentifier> parentTables = schema.getAssociatedTables();
        for (RelationIdentifier identifier : parentTables) {
          if (StringUtils.isNotEmpty(identifier.getTableId())) {
            if (identifier.getTableId().equalsIgnoreCase(carbonTable.getTableId())) {
              schemas.add(schema);
              break;
            }
          } else if (identifier.getTableName().equalsIgnoreCase(carbonTable.getTableName()) &&
              identifier.getDatabaseName().equalsIgnoreCase(carbonTable.getDatabaseName())) {
            schemas.add(schema);
            break;
          }
        }
      }
      return schemas;
    }

    List<MaterializedViewSchema> retrieveAllSchemas(MaterializedViewManager viewManager)
        throws IOException {
      checkAndReloadSchemas(viewManager, true);
      return new ArrayList<>(this.schemas);
    }

    @SuppressWarnings("Convert2Lambda")
    private Set<MaterializedViewSchema> retrieveAllSchemasInternal(
        MaterializedViewManager viewManager) throws IOException {
      Set<MaterializedViewSchema> schemas = new HashSet<>();
      CarbonFile carbonFile = FileFactory.getCarbonFile(this.systemDirectory);
      CarbonFile[] carbonFiles = carbonFile.listFiles(new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile file) {
          return file.getName().startsWith("mv_schema.");
        }
      });
      Gson gson = new Gson();
      for (CarbonFile file :carbonFiles) {
        DataInputStream dataInputStream = null;
        BufferedReader buffReader = null;
        InputStreamReader inStream = null;
        try {
          String absolutePath = file.getAbsolutePath();
          dataInputStream =
              FileFactory.getDataInputStream(
                  absolutePath);
          inStream = new InputStreamReader(dataInputStream,
              Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
          buffReader = new BufferedReader(inStream);
          MaterializedViewSchema schema = gson.fromJson(buffReader, MaterializedViewSchema.class);
          schema.setManager(viewManager);
          schemas.add(schema);
        } finally {
          CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
        }
      }
      return schemas;
    }

    void dropSchema(String viewName) throws IOException {
      String schemaPath = getSchemaPath(this.systemDirectory, viewName);
      if (!FileFactory.isFileExist(schemaPath)) {
        throw new IOException("Materialized with name " + viewName + " does not exists in storage");
      }
      LOG.info(String.format("Trying to delete materialized view %s schema", viewName));
      this.schemas.removeIf(
          schema -> schema.getIdentifier().getTableName().equalsIgnoreCase(viewName)
      );
      touchMDTFile();
      if (!FileFactory.deleteFile(schemaPath)) {
        throw new IOException("Materialized view with name " + viewName + " cannot be deleted");
      }
      LOG.info(String.format("Materialized view %s schema is deleted", viewName));
    }

    private void checkAndReloadSchemas(MaterializedViewManager viewManager, boolean touchFile)
        throws IOException {
      if (FileFactory.isFileExist(this.schemaIndexFilePath)) {
        long lastModifiedTime =
            FileFactory.getCarbonFile(this.schemaIndexFilePath).getLastModifiedTime();
        if (this.lastModifiedTime != lastModifiedTime) {
          this.schemas = this.retrieveAllSchemasInternal(viewManager);
          touchMDTFile();
        }
      } else {
        this.schemas = this.retrieveAllSchemasInternal(viewManager);
        if (touchFile) {
          touchMDTFile();
        }
      }
    }

    private void touchMDTFile() throws IOException {
      if (!FileFactory.isFileExist(this.systemDirectory)) {
        FileFactory.createDirectoryAndSetPermission(this.systemDirectory,
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
      }
      if (!FileFactory.isFileExist(this.schemaIndexFilePath)) {
        FileFactory.createNewFile(
            this.schemaIndexFilePath,
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
      }
      long lastModifiedTime = System.currentTimeMillis();
      FileFactory.getCarbonFile(this.schemaIndexFilePath).setLastModifiedTime(lastModifiedTime);
      this.lastModifiedTime = lastModifiedTime;
    }

  }

}
