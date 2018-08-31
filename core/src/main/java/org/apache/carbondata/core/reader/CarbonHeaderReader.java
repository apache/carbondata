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
package org.apache.carbondata.core.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.format.FileHeader;

import static org.apache.carbondata.core.util.CarbonUtil.thriftColumnSchemaToWrapperColumnSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TBase;

/**
 * Below class to read file header of version3
 * carbon data file
 */
public class CarbonHeaderReader {

  //Fact file path
  private String filePath;

  private Configuration configuration;

  public CarbonHeaderReader(String filePath) {
    this.filePath = filePath;
  }

  public CarbonHeaderReader(String filePath, Configuration configuration) {
    this.filePath = filePath;
    this.configuration = configuration;
  }

  /**
   * It reads the metadata in FileFooter thrift object format.
   *
   * @return
   * @throws IOException
   */
  public FileHeader readHeader() throws IOException {
    ThriftReader thriftReader = openThriftReader(filePath);
    thriftReader.open();
    FileHeader header = (FileHeader) thriftReader.read();
    thriftReader.close();
    return header;
  }

  /**
   * Open the thrift reader
   *
   * @param filePath
   * @return
   * @throws IOException
   */
  private ThriftReader openThriftReader(String filePath) {
    Configuration conf = configuration != null ? configuration : FileFactory.getConfiguration();
    return new ThriftReader(filePath, new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new FileHeader();
      }
    }, conf);
  }

  /**
   * Read and return the schema in the header
   */
  public List<ColumnSchema> readSchema() throws IOException {
    FileHeader fileHeader = readHeader();
    List<ColumnSchema> columnSchemaList = new ArrayList<>();
    List<org.apache.carbondata.format.ColumnSchema> table_columns = fileHeader.getColumn_schema();
    for (org.apache.carbondata.format.ColumnSchema table_column : table_columns) {
      ColumnSchema col = thriftColumnSchemaToWrapperColumnSchema(table_column);
      col.setColumnReferenceId(col.getColumnUniqueId());
      columnSchemaList.add(col);
    }
    return columnSchemaList;
  }
}
