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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import static org.apache.carbondata.core.util.CarbonUtil.thriftColumnSchemaToWrapperColumnSchema;

import org.apache.hadoop.mapreduce.RecordReader;


/**
 * Reader for carbondata file
 */
@InterfaceAudience.User
@InterfaceStability.Evolving
public class CarbonReader<T> {

  private List<RecordReader<Void, T>> readers;

  private RecordReader<Void, T> currentReader;

  private int index;

  private boolean initialise;

  /**
   * Call {@link #builder(String)} to construct an instance
   */
  CarbonReader(List<RecordReader<Void, T>> readers) {
    if (readers.size() == 0) {
      throw new IllegalArgumentException("no reader");
    }
    this.initialise = true;
    this.readers = readers;
    this.index = 0;
    this.currentReader = readers.get(0);
  }

  /**
   * Return true if has next row
   */
  public boolean hasNext() throws IOException, InterruptedException {
    validateReader();
    if (currentReader.nextKeyValue()) {
      return true;
    } else {
      if (index == readers.size() - 1) {
        // no more readers
        return false;
      } else {
        index++;
        currentReader = readers.get(index);
        return currentReader.nextKeyValue();
      }
    }
  }

  /**
   * Read and return next row object
   */
  public T readNextRow() throws IOException, InterruptedException {
    validateReader();
    return currentReader.getCurrentValue();
  }

  /**
   * Return a new {@link CarbonReaderBuilder} instance
   */
  public static CarbonReaderBuilder builder(String tablePath, String tableName) {
    return new CarbonReaderBuilder(tablePath, tableName);
  }

  /**
   * Read carbondata file and return the schema
   */
  public static List<ColumnSchema> readSchemaInDataFile(String dataFilePath) throws IOException {
    CarbonHeaderReader reader = new CarbonHeaderReader(dataFilePath);
    return reader.readSchema();
  }

  /**
   * Read carbonindex file and return the schema
   *
   * @param indexFilePath complete path including index file name
   * @return null, if the index file is not present in the path.
   * List<ColumnSchema> from the index file.
   * @throws IOException
   */
  public static List<ColumnSchema> readSchemaInIndexFile(String indexFilePath) throws IOException {
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
      return columnSchemaList;
    } finally {
      dataInputStream.close();
    }
  }

  /**
   * Read CarbonData file and return the user schema,
   * the schema order is the same as user save schema
   */
  public static List<ColumnSchema> readUserSchema(String indexFilePath) throws IOException {
    List<ColumnSchema> columnSchemas = readSchemaInIndexFile(indexFilePath);
    Collections.sort(columnSchemas, new Comparator<ColumnSchema>() {
      @Override
      public int compare(ColumnSchema o1, ColumnSchema o2) {
        return Integer.compare(o1.getSchemaOrdinal(), o2.getSchemaOrdinal());
      }
    });
    return columnSchemas;
  }

  /**
   * Read schema file and return table info object
   */
  public static TableInfo readSchemaFile(String schemaFilePath) throws IOException {
    org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil.readSchemaFile(schemaFilePath);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    return schemaConverter.fromExternalToWrapperTableInfo(tableInfo, "", "", "");
  }

  /**
   * Close reader
   *
   * @throws IOException
   */
  public void close() throws IOException {
    validateReader();
    this.currentReader.close();
    this.initialise = false;
  }

  /**
   * Validate the reader
   */
  private void validateReader() {
    if (!this.initialise) {
      throw new RuntimeException(this.getClass().getSimpleName() +
          " not initialise, please create it first.");
    }
  }
}
