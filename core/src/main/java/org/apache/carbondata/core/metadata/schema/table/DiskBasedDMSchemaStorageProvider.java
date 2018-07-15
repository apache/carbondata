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

package org.apache.carbondata.core.metadata.schema.table;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Stores datamap schema in disk as json format
 */
public class DiskBasedDMSchemaStorageProvider implements DataMapSchemaStorageProvider {

  private String storePath;

  private String mdtFilePath;

  private long lastModifiedTime;

  private Set<DataMapSchema> dataMapSchemas = new HashSet<>();

  public DiskBasedDMSchemaStorageProvider(String storePath) {
    this.storePath = CarbonUtil.checkAndAppendHDFSUrl(storePath);
    this.mdtFilePath = storePath + CarbonCommonConstants.FILE_SEPARATOR + "datamap.mdtfile";
  }

  @Override public void saveSchema(DataMapSchema dataMapSchema) throws IOException {
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    String schemaPath = getSchemaPath(storePath, dataMapSchema.getDataMapName());
    FileFactory.FileType fileType = FileFactory.getFileType(schemaPath);
    if (FileFactory.isFileExist(schemaPath, fileType)) {
      throw new IOException(
          "DataMap with name " + dataMapSchema.getDataMapName() + " already exists in storage");
    }
    // write the datamap shema in json format.
    try {
      FileFactory.mkdirs(storePath, fileType);
      FileFactory.createNewFile(schemaPath, fileType);
      dataOutputStream =
          FileFactory.getDataOutputStream(schemaPath, fileType);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(dataMapSchema);
      brWriter.write(metadataInstance);
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      dataMapSchemas.add(dataMapSchema);
      CarbonUtil.closeStreams(dataOutputStream, brWriter);
      checkAndReloadDataMapSchemas(true);
      touchMDTFile();
    }
  }

  @Override public DataMapSchema retrieveSchema(String dataMapName)
      throws IOException, NoSuchDataMapException {
    checkAndReloadDataMapSchemas(true);
    for (DataMapSchema dataMapSchema : dataMapSchemas) {
      if (dataMapSchema.getDataMapName().equalsIgnoreCase(dataMapName)) {
        return dataMapSchema;
      }
    }
    throw new NoSuchDataMapException(dataMapName);
  }

  @Override public List<DataMapSchema> retrieveSchemas(CarbonTable carbonTable) throws IOException {
    checkAndReloadDataMapSchemas(false);
    List<DataMapSchema> dataMapSchemas = new ArrayList<>();
    for (DataMapSchema dataMapSchema : this.dataMapSchemas) {
      List<RelationIdentifier> parentTables = dataMapSchema.getParentTables();
      for (RelationIdentifier identifier : parentTables) {
        if (StringUtils.isNotEmpty(identifier.getTableId())) {
          if (identifier.getTableId().equalsIgnoreCase(carbonTable.getTableId())) {
            dataMapSchemas.add(dataMapSchema);
            break;
          }
        } else if (identifier.getTableName().equalsIgnoreCase(carbonTable.getTableName()) &&
            identifier.getDatabaseName().equalsIgnoreCase(carbonTable.getDatabaseName())) {
          dataMapSchemas.add(dataMapSchema);
          break;
        }
      }
    }
    return dataMapSchemas;
  }

  @Override public List<DataMapSchema> retrieveAllSchemas() throws IOException {
    checkAndReloadDataMapSchemas(true);
    return new ArrayList<>(dataMapSchemas);
  }

  private Set<DataMapSchema> retrieveAllSchemasInternal() throws IOException {
    Set<DataMapSchema> dataMapSchemas = new HashSet<>();
    CarbonFile carbonFile = FileFactory.getCarbonFile(storePath);
    CarbonFile[] carbonFiles = carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return file.getName().endsWith(".dmschema");
      }
    });

    for (CarbonFile file :carbonFiles) {
      Gson gsonObjectToRead = new Gson();
      DataInputStream dataInputStream = null;
      BufferedReader buffReader = null;
      InputStreamReader inStream = null;
      try {
        String absolutePath = file.getAbsolutePath();
        dataInputStream =
            FileFactory.getDataInputStream(
                absolutePath, FileFactory.getFileType(absolutePath));
        inStream = new InputStreamReader(dataInputStream,
            Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        buffReader = new BufferedReader(inStream);
        dataMapSchemas.add(gsonObjectToRead.fromJson(buffReader, DataMapSchema.class));
      } finally {
        CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
      }
    }
    return dataMapSchemas;
  }

  @Override public void dropSchema(String dataMapName)
      throws IOException {
    String schemaPath = getSchemaPath(storePath, dataMapName);
    if (!FileFactory.isFileExist(schemaPath, FileFactory.getFileType(schemaPath))) {
      throw new IOException("DataMap with name " + dataMapName + " does not exists in storage");
    }
    Iterator<DataMapSchema> iterator = dataMapSchemas.iterator();
    while (iterator.hasNext()) {
      DataMapSchema schema = iterator.next();
      if (schema.getDataMapName().equalsIgnoreCase(dataMapName)) {
        iterator.remove();
      }
    }
    touchMDTFile();
    if (!FileFactory.deleteFile(schemaPath, FileFactory.getFileType(schemaPath))) {
      throw new IOException("DataMap with name " + dataMapName + " cannot be deleted");
    }
  }

  private void checkAndReloadDataMapSchemas(boolean touchFile) throws IOException {
    if (FileFactory.isFileExist(mdtFilePath)) {
      long lastModifiedTime = FileFactory.getCarbonFile(mdtFilePath).getLastModifiedTime();
      if (this.lastModifiedTime != lastModifiedTime) {
        dataMapSchemas = retrieveAllSchemasInternal();
        touchMDTFile();
      }
    } else {
      dataMapSchemas = retrieveAllSchemasInternal();
      if (touchFile) {
        touchMDTFile();
      }
    }
  }

  private void touchMDTFile() throws IOException {
    if (!FileFactory.isFileExist(storePath)) {
      FileFactory.createDirectoryAndSetPermission(
          storePath,
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
    if (!FileFactory.isFileExist(mdtFilePath)) {
      FileFactory.createNewFile(
          mdtFilePath,
          FileFactory.getFileType(mdtFilePath),
          true,
          new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
    }
    long lastModifiedTime = System.currentTimeMillis();
    FileFactory.getCarbonFile(mdtFilePath).setLastModifiedTime(lastModifiedTime);
    this.lastModifiedTime = lastModifiedTime;
  }

  /**
   * it returns the schema path for the datamap
   * @param storePath
   * @param dataMapName
   * @return
   */
  public static String getSchemaPath(String storePath, String dataMapName) {
    String schemaPath =  storePath + CarbonCommonConstants.FILE_SEPARATOR + dataMapName
        + ".dmschema";;
    return schemaPath;
  }
}
