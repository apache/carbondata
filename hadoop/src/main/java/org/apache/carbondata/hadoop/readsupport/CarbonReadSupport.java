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
package org.apache.carbondata.hadoop.readsupport;

import java.io.IOException;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

/**
 * This is the interface to convert data reading from RecordReader to row representation.
 */
public interface CarbonReadSupport<T> {

  /**
   * Initialization if needed based on the projected column list
   *
   * @param carbonColumns column list
   * @param carbonTable table identifier
   */
  void initialize(CarbonColumn[] carbonColumns, CarbonTable carbonTable) throws IOException;

  /**
   * convert column data back to row representation
   * @param data column data
   */
  T readRow(Object[] data);

  /**
   * cleanup step if necessary
   */
  void close();

}
