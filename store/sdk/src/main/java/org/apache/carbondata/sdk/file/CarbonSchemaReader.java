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

package org.apache.carbondata.sdk.file;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonFooterReaderV3;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.FileFooter3;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;

import static org.apache.carbondata.core.util.CarbonUtil.thriftColumnSchemaToWrapperColumnSchema;
import static org.apache.carbondata.core.util.path.CarbonTablePath.CARBON_DATA_EXT;
import static org.apache.carbondata.core.util.path.CarbonTablePath.INDEX_FILE_EXT;

/**
 * Schema reader for carbon files, including carbondata file, carbonindex file, and schema file
 */
public class CarbonSchemaReader {

  /**
   * Read schema file and return the schema
   *
   * @param schemaFilePath complete path including schema file name
   * @return schema object
   * @throws IOException
   */
  @Deprecated
  public static Schema readSchemaInSchemaFile(String schemaFilePath) throws IOException {
    org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil.readSchemaFile(schemaFilePath);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    List<ColumnSchema> schemaList = schemaConverter
        .fromExternalToWrapperTableInfo(tableInfo, "", "", "")
        .getFactTable()
        .getListOfColumns();
    return new Schema(schemaList);
  }

  /**
   * get carbondata/carbonindex file in path
   *
   * @param path carbon file path
   * @return CarbonFile array
   */
  private static CarbonFile[] getCarbonFile(String path, final String extension)
      throws IOException {
    String dataFilePath = path;
    if (!(dataFilePath.endsWith(extension))) {
      CarbonFile[] carbonFiles = FileFactory
          .getCarbonFile(path)
          .listFiles(new CarbonFileFilter() {
            @Override
            public boolean accept(CarbonFile file) {
              if (file == null) {
                return false;
              }
              return file.getName().endsWith(extension);
            }
          });
      if (carbonFiles == null || carbonFiles.length < 1) {
        throw new IOException("Carbon file not exists.");
      }
      return carbonFiles;
    } else {
      throw new CarbonDataLoadingException("Please ensure path "
          + path + " end with " + extension);
    }
  }

  /**
   * read schema from path,
   * path can be folder path, carbonindex file path, and carbondata file path
   * and will not check all files schema
   *
   * @param path file/folder path
   * @return schema
   * @throws IOException
   */
  public static Schema readSchema(String path) throws IOException {
    return readSchema(path, false);
  }

  /**
   * read schema from path,
   * path can be folder path, carbonindex file path, and carbondata file path
   * and user can decide whether check all files schema
   *
   * @param path             file/folder path
   * @param validateSchema whether check all files schema
   * @return schema
   * @throws IOException
   */
  public static Schema readSchema(String path, boolean validateSchema) throws IOException {
    if (path.endsWith(INDEX_FILE_EXT)) {
      return readSchemaFromIndexFile(path);
    } else if (path.endsWith(CARBON_DATA_EXT)) {
      return readSchemaFromDataFile(path);
    } else if (validateSchema) {
      CarbonFile[] carbonIndexFiles = getCarbonFile(path, INDEX_FILE_EXT);
      Schema schema;
      if (carbonIndexFiles != null && carbonIndexFiles.length != 0) {
        schema = readSchemaFromIndexFile(carbonIndexFiles[0].getAbsolutePath());
        for (int i = 1; i < carbonIndexFiles.length; i++) {
          Schema schema2 = readSchemaFromIndexFile(carbonIndexFiles[i].getAbsolutePath());
          if (!schema.equals(schema2)) {
            throw new CarbonDataLoadingException("Schema is different between different files.");
          }
        }
        CarbonFile[] carbonDataFiles = getCarbonFile(path, CARBON_DATA_EXT);
        for (int i = 0; i < carbonDataFiles.length; i++) {
          Schema schema2 = readSchemaFromDataFile(carbonDataFiles[i].getAbsolutePath());
          if (!schema.equals(schema2)) {
            throw new CarbonDataLoadingException("Schema is different between different files.");
          }
        }
        return schema;
      } else {
        throw new CarbonDataLoadingException("No carbonindex file in this path.");
      }
    } else {
      String indexFilePath = getCarbonFile(path, INDEX_FILE_EXT)[0].getAbsolutePath();
      return readSchemaFromIndexFile(indexFilePath);
    }
  }

  /**
   * Read carbondata file and return the schema
   * This interface will be removed,
   * please use readSchema instead of this interface
   *
   * @param dataFilePath carbondata file store path
   * @return Schema object
   * @throws IOException
   */
  @Deprecated
  public static Schema readSchemaInDataFile(String dataFilePath) throws IOException {
    return readSchema(dataFilePath, false);
  }

  /**
   * Read schema from carbondata file
   *
   * @param dataFilePath carbondata file path
   * @return carbon data schema
   * @throws IOException
   */
  public static Schema readSchemaFromDataFile(String dataFilePath) throws IOException {
    CarbonHeaderReader reader = new CarbonHeaderReader(dataFilePath);
    List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
    List<ColumnSchema> schemaList = reader.readSchema();
    for (int i = 0; i < schemaList.size(); i++) {
      ColumnSchema columnSchema = schemaList.get(i);
      if (!(columnSchema.getColumnName().contains("."))) {
        columnSchemaList.add(columnSchema);
      }
    }
    return new Schema(columnSchemaList);
  }

  /**
   * Read carbonindex file and return the schema
   * This interface will be removed,
   * please use readSchema instead of this interface
   *
   * @param indexFilePath complete path including index file name
   * @return schema object
   * @throws IOException
   */
  @Deprecated
  public static Schema readSchemaInIndexFile(String indexFilePath) throws IOException {
    return readSchema(indexFilePath, false);
  }

  /**
   * Read schema from carbonindex file
   *
   * @param indexFilePath carbonindex file path
   * @return carbon data Schema
   * @throws IOException
   */
  private static Schema readSchemaFromIndexFile(String indexFilePath) throws IOException {
    CarbonFile indexFile =
        FileFactory.getCarbonFile(indexFilePath, FileFactory.getFileType(indexFilePath));
    if (!indexFile.getName().endsWith(INDEX_FILE_EXT)) {
      throw new IOException("Not an index file name");
    }
    // read schema from the first index file
    DataInputStream dataInputStream =
        FileFactory.getDataInputStream(indexFilePath, FileFactory.getFileType(indexFilePath));
    byte[] bytes = new byte[(int) indexFile.getSize()];
    try {
      //get the file in byte buffer
      dataInputStream.readFully(bytes);
      CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
      // read from byte buffer.
      indexReader.openThriftReader(bytes);
      // get the index header
      org.apache.carbondata.format.IndexHeader readIndexHeader = indexReader.readIndexHeader();
      List<ColumnSchema> columnSchemaList = new ArrayList<ColumnSchema>();
      List<org.apache.carbondata.format.ColumnSchema> table_columns =
          readIndexHeader.getTable_columns();
      for (org.apache.carbondata.format.ColumnSchema columnSchema : table_columns) {
        if (!(columnSchema.column_name.contains("."))) {
          columnSchemaList.add(thriftColumnSchemaToWrapperColumnSchema(columnSchema));
        }
      }
      return new Schema(columnSchemaList);
    } finally {
      dataInputStream.close();
    }
  }

  /**
   * This method return the version details in formatted string by reading from carbondata file
   *
   * @param dataFilePath
   * @return
   * @throws IOException
   */
  public static String getVersionDetails(String dataFilePath) throws IOException {
    long fileSize =
        FileFactory.getCarbonFile(dataFilePath, FileFactory.getFileType(dataFilePath)).getSize();
    FileReader fileReader = FileFactory.getFileHolder(FileFactory.getFileType(dataFilePath));
    ByteBuffer buffer =
        fileReader.readByteBuffer(FileFactory.getUpdatedFilePath(dataFilePath), fileSize - 8, 8);
    fileReader.finish();
    CarbonFooterReaderV3 footerReader = new CarbonFooterReaderV3(dataFilePath, buffer.getLong());
    FileFooter3 footer = footerReader.readFooterVersion3();
    if (null != footer.getExtra_info()) {
      return footer.getExtra_info().get(CarbonCommonConstants.CARBON_WRITTEN_BY_FOOTER_INFO)
          + " in version: " + footer.getExtra_info()
          .get(CarbonCommonConstants.CARBON_WRITTEN_VERSION);
    } else {
      return "Version Details are not found in carbondata file";
    }
  }
}
