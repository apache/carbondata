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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
public class MVProvider {

  private static final Logger LOG = LogServiceFactory.getLogService(
      MVProvider.class.getCanonicalName());

  private static final String STATUS_FILE_NAME = "mv_status";

  private final Map<String, SchemaProvider> schemaProviders = new ConcurrentHashMap<>();

  private static String getSchemaPath(String schemaRoot, String viewName) {
    return schemaRoot + CarbonCommonConstants.FILE_SEPARATOR + "mv_schema." + viewName;
  }

  private SchemaProvider getSchemaProvider(MVManager viewManager, String databaseName) {
    String databaseNameUpper = databaseName.toUpperCase();
    SchemaProvider schemaProvider = this.schemaProviders.get(databaseNameUpper);
    if (schemaProvider == null) {
      synchronized (this.schemaProviders) {
        schemaProvider = this.schemaProviders.get(databaseNameUpper);
        if (schemaProvider == null) {
          String databaseLocation = viewManager.getDatabaseLocation(databaseName);
          CarbonFile databasePath = FileFactory.getCarbonFile(databaseLocation);
          if (!databasePath.exists()) {
            return null;
          }
          schemaProvider = new SchemaProvider(databasePath.getCanonicalPath());
          this.schemaProviders.put(databaseNameUpper, schemaProvider);
        }
      }
    }
    return schemaProvider;
  }

  public MVSchema getSchema(MVManager viewManager, String databaseName,
                            String viewName, boolean isRegisterMV) throws IOException {
    SchemaProvider schemaProvider = this.getSchemaProvider(viewManager, databaseName);
    if (schemaProvider == null) {
      return null;
    }
    if (!isRegisterMV) {
      return schemaProvider.retrieveSchema(viewManager, viewName);
    } else {
      // in case of old store, get schema by checking in system folder.
      return schemaProvider.retrieveSchemaFromFolder(viewManager, viewName);
    }
  }

  List<MVSchema> getSchemas(MVManager viewManager, String databaseName,
                            CarbonTable carbonTable) throws IOException {
    SchemaProvider schemaProvider = this.getSchemaProvider(viewManager, databaseName);
    if (schemaProvider == null) {
      return Collections.emptyList();
    } else {
      return schemaProvider.retrieveSchemas(viewManager, carbonTable);
    }
  }

  List<MVSchema> getSchemas(MVManager viewManager, String databaseName) throws IOException {
    SchemaProvider schemaProvider = this.getSchemaProvider(viewManager, databaseName);
    if (schemaProvider == null) {
      return Collections.emptyList();
    } else {
      return schemaProvider.retrieveAllSchemas(viewManager);
    }
  }

  void saveSchema(MVManager viewManager, String databaseName,
                  MVSchema viewSchema) throws IOException {
    SchemaProvider schemaProvider = this.getSchemaProvider(viewManager, databaseName);
    if (schemaProvider == null) {
      throw new IOException("Database [" + databaseName + "] is not found.");
    }
    schemaProvider.saveSchema(viewManager, viewSchema);
  }

  public void dropSchema(MVManager viewManager, String databaseName,
                         String viewName)throws IOException {
    SchemaProvider schemaProvider = this.getSchemaProvider(viewManager, databaseName);
    if (schemaProvider == null) {
      throw new IOException("Materialized view with name " + databaseName + "." + viewName +
          " does not exists in storage");
    }
    schemaProvider.dropSchema(viewName);
  }

  private String getStatusFileName(MVManager viewManager, String databaseName) {
    String databaseLocation = viewManager.getDatabaseLocation(databaseName);
    try {
      if (FileFactory.isFileExist(databaseLocation)) {
        return FileFactory.getCarbonFile(databaseLocation).getCanonicalPath()
            + CarbonCommonConstants.FILE_SEPARATOR + "_system"
            + CarbonCommonConstants.FILE_SEPARATOR + STATUS_FILE_NAME;
      } else {
        // this database folder is not exists
        return null;
      }
    } catch (IOException e) {
      // avoid to impact other query on all databases because of mv failure on this database
      LOG.warn("Failed to get mv status file for database " + databaseName, e);
      return null;
    }
  }

  public List<MVStatusDetail> getStatusDetails(MVManager viewManager, String databaseName)
      throws IOException {
    String statusPath = this.getStatusFileName(viewManager, databaseName);
    if (statusPath == null) {
      return Collections.emptyList();
    }
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    MVStatusDetail[] statusDetails;
    try {
      if (!FileFactory.isFileExist(statusPath)) {
        return Collections.emptyList();
      }
      dataInputStream = FileFactory.getDataInputStream(statusPath);
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      statusDetails = gsonObjectToRead.fromJson(buffReader,
          MVStatusDetail[].class);
    } catch (IOException e) {
      LOG.error("Failed to read MV status", e);
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

  private static ICarbonLock getStatusLock(String databaseLocation) {
    return CarbonLockFactory.getSystemLevelCarbonLockObj(
        CarbonProperties.getInstance().getSystemFolderLocationPerDatabase(databaseLocation),
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
  public void updateStatus(MVManager viewManager, List<MVSchema> schemaList,
      MVStatus status) throws IOException {
    if (schemaList == null || schemaList.size() == 0) {
      // There is nothing to update
      return;
    }
    final Map<String, List<MVSchema>> schemasMapByDatabase = new HashMap<>();
    for (MVSchema schema : schemaList) {
      String databaseName = schema.getIdentifier().getDatabaseName().toLowerCase();
      List<MVSchema> schemas = schemasMapByDatabase.get(databaseName);
      if (schemas == null) {
        schemas = new ArrayList<>();
        schemasMapByDatabase.put(databaseName, schemas);
      }
      schemas.add(schema);
    }
    for (Map.Entry<String, List<MVSchema>> entry : schemasMapByDatabase.entrySet()) {
      this.updateStatus(viewManager, entry.getKey(), entry.getValue(), status);
    }
  }

  private void updateStatus(MVManager viewManager, String databaseName, List<MVSchema> schemaList,
      MVStatus status) throws IOException {
    String databaseLocation =
        FileFactory.getCarbonFile(viewManager.getDatabaseLocation(databaseName)).getCanonicalPath();
    ICarbonLock carbonTableStatusLock = getStatusLock(databaseLocation);
    boolean locked = false;
    try {
      locked = carbonTableStatusLock.lockWithRetries();
      if (locked) {
        LOG.info("Materialized view status lock has been successfully acquired.");
        if (status == MVStatus.ENABLED) {
          // Enable mv only if mv tables and main table are in sync
          if (!isViewCanBeEnabled(schemaList.get(0), false)) {
            return;
          }
        }
        List<MVStatusDetail> statusDetailList =
            new ArrayList<>(getStatusDetails(viewManager, databaseName));
        List<MVStatusDetail> changedStatusDetails = new ArrayList<>();
        List<MVStatusDetail> newStatusDetails = new ArrayList<>();
        for (MVSchema schema : schemaList) {
          boolean exists = false;
          for (MVStatusDetail statusDetail : statusDetailList) {
            if (statusDetail.getIdentifier().equals(schema.getIdentifier())) {
              statusDetail.setStatus(status);
              changedStatusDetails.add(statusDetail);
              exists = true;
            }
          }
          if (!exists) {
            newStatusDetails
                .add(new MVStatusDetail(schema.getIdentifier(),
                    status));
          }
        }
        // Add the newly added MV to the list.
        if (newStatusDetails.size() > 0 &&
            status != MVStatus.DROPPED) {
          statusDetailList.addAll(newStatusDetails);
        }
        // In case of dropped MV, just remove from the list.
        if (status == MVStatus.DROPPED) {
          statusDetailList.removeAll(changedStatusDetails);
        }
        writeLoadDetailsIntoFile(
            this.getStatusFileName(viewManager, databaseName),
            statusDetailList.toArray(
                    new MVStatusDetail[statusDetailList.size()]));
      } else {
        String errorMsg = "Updating MV status is failed due to another process taken the lock"
            + " for updating it";
        LOG.error(errorMsg);
        throw new IOException(errorMsg + " Please try after some time.");
      }
    } finally {
      if (locked) {
        carbonTableStatusLock.unlock();
      }
    }
  }

  /**
   * writes mv status details
   */
  private static void writeLoadDetailsIntoFile(String location,
      MVStatusDetail[] statusDetails) throws IOException {
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
  public boolean isViewCanBeEnabled(MVSchema schema, boolean ignoreDeferredCheck)
      throws IOException {
    if (!ignoreDeferredCheck) {
      if (!schema.isRefreshIncremental()) {
        return true;
      }
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
    List<RelationIdentifier> relatedTables = schema.getRelatedTables();
    for (RelationIdentifier relatedTable : relatedTables) {
      SegmentStatusManager.ValidAndInvalidSegmentsInfo validAndInvalidSegmentsInfo =
          SegmentStatusManager.getValidAndInvalidSegmentsInfo(relatedTable);
      List<String> relatedTableSegmentList =
          SegmentStatusManager.getValidSegmentList(validAndInvalidSegmentsInfo);
      if (!relatedTableSegmentList.isEmpty()) {
        if (viewSegmentMap.isEmpty()) {
          isViewCanBeEnabled = false;
        } else {
          String tableUniqueName =
              relatedTable.getDatabaseName() + CarbonCommonConstants.POINT + relatedTable
                  .getTableName();
          isViewCanBeEnabled =
              viewSegmentMap.get(tableUniqueName).containsAll(relatedTableSegmentList);
          if (!isViewCanBeEnabled) {
            // in case if main table is compacted and mv table mapping is not updated,
            // check from merged Load Mapping
            isViewCanBeEnabled = viewSegmentMap.get(tableUniqueName).containsAll(
                relatedTableSegmentList.stream()
                    .map(validAndInvalidSegmentsInfo.getMergedLoadMapping()::get)
                    .flatMap(Collection::stream).collect(Collectors.toList()));
          }
        }
      }
    }
    if (!isViewCanBeEnabled) {
      LOG.error("MV `" + schema.getIdentifier().getTableName()
          + "` is not in Sync with its related tables. Refresh MV to sync it.");
    }
    return isViewCanBeEnabled;
  }

  /**
   * Index schema provider of a database.
   */
  private static final class SchemaProvider {

    private String systemDirectory;

    private String schemaIndexFilePath;

    private long lastModifiedTime;

    private Set<MVSchema> schemas = new HashSet<>();

    SchemaProvider(String databaseLocation) {
      final String systemDirectory =
          CarbonProperties.getInstance().getSystemFolderLocationPerDatabase(databaseLocation);
      this.systemDirectory = systemDirectory;
      this.schemaIndexFilePath =
          systemDirectory + CarbonCommonConstants.FILE_SEPARATOR + "mv_schema_index";
    }

    void saveSchema(MVManager viewManager, MVSchema viewSchema)
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
        if (!checkAndReloadSchemas(viewManager, true)) {
          touchMDTFile();
        }
      }
    }

    MVSchema retrieveSchema(MVManager viewManager, String viewName)
        throws IOException {
      checkAndReloadSchemas(viewManager, true);
      for (MVSchema schema : this.schemas) {
        if (schema.getIdentifier().getTableName().equalsIgnoreCase(viewName)) {
          return schema;
        }
      }
      return null;
    }

    List<MVSchema> retrieveSchemas(MVManager viewManager,
                                   CarbonTable carbonTable) throws IOException {
      checkAndReloadSchemas(viewManager, false);
      List<MVSchema> schemas = new ArrayList<>();
      for (MVSchema schema : this.schemas) {
        List<RelationIdentifier> parentTables = schema.getRelatedTables();
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

    List<MVSchema> retrieveAllSchemas(MVManager viewManager)
        throws IOException {
      checkAndReloadSchemas(viewManager, true);
      return new ArrayList<>(this.schemas);
    }

    @SuppressWarnings("Convert2Lambda")
    private Set<MVSchema> retrieveAllSchemasInternal(
        MVManager viewManager) throws IOException {
      Set<MVSchema> schemas = new HashSet<>();
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
          MVSchema schema = gson.fromJson(buffReader, MVSchema.class);
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

    private synchronized boolean checkAndReloadSchemas(MVManager viewManager, boolean touchFile)
        throws IOException {
      if (FileFactory.isFileExist(this.schemaIndexFilePath)) {
        long lastModifiedTime =
            FileFactory.getCarbonFile(this.schemaIndexFilePath).getLastModifiedTime();
        if (this.lastModifiedTime != lastModifiedTime) {
          this.schemas = this.retrieveAllSchemasInternal(viewManager);
          touchMDTFile();
          return true;
        }
      } else {
        this.schemas = this.retrieveAllSchemasInternal(viewManager);
        if (touchFile) {
          touchMDTFile();
          return true;
        }
      }
      return false;
    }

    public MVSchema retrieveSchemaFromFolder(MVManager viewManager, String mvName)
        throws IOException {
      this.schemas = this.retrieveAllSchemasInternal(viewManager);
      for (MVSchema schema : this.schemas) {
        if (schema.getIdentifier().getTableName().equalsIgnoreCase(mvName)) {
          touchMDTFile();
          return schema;
        }
      }
      return null;
    }

    private synchronized void touchMDTFile() throws IOException {
      if (!FileFactory.isFileExist(this.systemDirectory)) {
        FileFactory.createDirectoryAndSetPermission(this.systemDirectory,
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
      }
      // two or more JVM process can access this method to update last modified time at same
      // time causing exception. So take a system level lock on system folder and update
      // last modified time of schema index file
      ICarbonLock systemDirLock = CarbonLockFactory
          .getSystemLevelCarbonLockObj(this.systemDirectory,
              LockUsage.MATERIALIZED_VIEW_STATUS_LOCK);
      boolean locked = false;
      try {
        locked = systemDirLock.lockWithRetries();
        if (locked) {
          CarbonFile schemaIndexFile = FileFactory.getCarbonFile(this.schemaIndexFilePath);
          if (schemaIndexFile.exists()) {
            schemaIndexFile.delete();
          }
          schemaIndexFile.createNewFile(new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
          this.lastModifiedTime = schemaIndexFile.getLastModifiedTime();
        } else {
          LOG.warn("Unable to get Lock to refresh schema index last modified time");
        }
      } finally {
        if (locked) {
          systemDirLock.unlock();
        }
      }
    }
  }

}
