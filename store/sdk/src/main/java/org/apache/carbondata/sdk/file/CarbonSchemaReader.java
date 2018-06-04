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
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import static org.apache.carbondata.core.util.CarbonUtil.thriftColumnSchemaToWrapperColumnSchema;

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
   * Read carbondata file and return the schema
   *
   * @param dataFilePath complete path including carbondata file name
   * @return Schema object
   * @throws IOException
   */
  public static Schema readSchemaInDataFile(String dataFilePath) throws IOException {
    CarbonHeaderReader reader = new CarbonHeaderReader(dataFilePath);
    return new Schema(reader.readSchema());
  }

  /**
   * Read carbonindex file and return the schema
   *
   * @param indexFilePath complete path including index file name
   * @return schema object
   * @throws IOException
   */
  public static Schema readSchemaInIndexFile(String indexFilePath) throws IOException {
    CarbonFile indexFile =
        FileFactory.getCarbonFile(indexFilePath, FileFactory.getFileType(indexFilePath));
    if (!indexFile.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
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
        columnSchemaList.add(thriftColumnSchemaToWrapperColumnSchema(columnSchema));
      }
      return new Schema(columnSchemaList);
    } finally {
      dataInputStream.close();
    }
  }

}
