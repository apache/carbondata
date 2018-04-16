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
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;

/**
 * Stores datamap schema in disk as json format
 */
public class DiskBasedDMSchemaStorageProvider implements DataMapSchemaStorageProvider {

  private String storePath;

  public DiskBasedDMSchemaStorageProvider(String storePath) {
    this.storePath = storePath;
  }

  @Override public void saveSchema(DataMapSchema dataMapSchema) throws IOException {
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    String schemaPath =
        storePath + CarbonCommonConstants.FILE_SEPARATOR + dataMapSchema.relationIdentifier
            .getTableName() + CarbonCommonConstants.UNDERSCORE + dataMapSchema.getDataMapName()
            + ".dmschema";
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
      CarbonUtil.closeStreams(dataOutputStream, brWriter);
    }
  }

  @Override public DataMapSchema retrieveSchema(String dataMapName) throws IOException {
    if (!dataMapName.endsWith(".dmschema")) {
      dataMapName = dataMapName + ".dmschema";
    }
    String schemaPath =
        storePath + CarbonCommonConstants.FILE_SEPARATOR + dataMapName;
    if (!FileFactory.isFileExist(schemaPath, FileFactory.getFileType(schemaPath))) {
      throw new IOException("DataMap with name " + dataMapName + " does not exists in storage");
    }

    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    try {
      dataInputStream =
          FileFactory.getDataInputStream(schemaPath, FileFactory.getFileType(schemaPath));
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      return gsonObjectToRead.fromJson(buffReader, DataMapSchema.class);
    } finally {
      CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
    }

  }

  @Override public List<DataMapSchema> retrieveSchemas(List<String> dataMapNames)
      throws IOException {
    List<DataMapSchema> dataMapSchemas = new ArrayList<>(dataMapNames.size());
    for (String dataMapName : dataMapNames) {
      dataMapSchemas.add(retrieveSchema(dataMapName));
    }
    return dataMapSchemas;
  }

  @Override public List<DataMapSchema> retrieveAllSchemas() throws IOException {
    List<DataMapSchema> dataMapSchemas = new ArrayList<>();
    CarbonFile carbonFile = FileFactory.getCarbonFile(storePath);
    CarbonFile[] carbonFiles = carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return file.getName().endsWith(".dmschema");
      }
    });

    for (CarbonFile file :carbonFiles) {
      dataMapSchemas.add(retrieveSchema(file.getName()));
    }
    return dataMapSchemas;
  }

  @Override public void dropSchema(String dataMapName,String tableName) throws IOException {
    String schemaPath = storePath + CarbonCommonConstants.FILE_SEPARATOR + tableName
        + CarbonCommonConstants.UNDERSCORE + dataMapName + ".dmschema";
    if (!FileFactory.isFileExist(schemaPath, FileFactory.getFileType(schemaPath))) {
      throw new IOException("DataMap with name " + dataMapName + " does not exists in storage");
    }

    if (!FileFactory.deleteFile(schemaPath, FileFactory.getFileType(schemaPath))) {
      throw new IOException("DataMap with name " + dataMapName + " cannot be deleted");
    }
  }
}
