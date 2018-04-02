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

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.reader.CarbonHeaderReader;
import org.apache.carbondata.core.util.CarbonUtil;

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

  /**
   * Call {@link #builder(String)} to construct an instance
   */
  CarbonReader(List<RecordReader<Void, T>> readers) {
    if (readers.size() == 0) {
      throw new IllegalArgumentException("no reader");
    }
    this.readers = readers;
    this.index = 0;
    this.currentReader = readers.get(0);
  }

  /**
   * Return true if has next row
   */
  public boolean hasNext() throws IOException, InterruptedException {
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
    return currentReader.getCurrentValue();
  }

  /**
   * Return a new {@link CarbonReaderBuilder} instance
   */
  public static CarbonReaderBuilder builder(String tablePath) {
    return new CarbonReaderBuilder(tablePath);
  }

  /**
   * Read carbondata file and return the schema
   */
  public static List<ColumnSchema> readSchemaInDataFile(String dataFilePath) throws IOException {
    CarbonHeaderReader reader = new CarbonHeaderReader(dataFilePath);
    return reader.readSchema();
  }

  /**
   * Read schmea file and return table info object
   */
  public static TableInfo readSchemaFile(String schemaFilePath) throws IOException {
    org.apache.carbondata.format.TableInfo tableInfo = CarbonUtil.readSchemaFile(schemaFilePath);
    SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
    return schemaConverter.fromExternalToWrapperTableInfo(tableInfo, "", "", "");
  }
}
