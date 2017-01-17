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

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

/**
 * It converts to the desired class while reading the rows from RecordReader
 */
public interface CarbonReadSupport<T> {

  /**
   * It can use [{@link CarbonColumn}] array to create its own schema to create its row.
   *
   * @param carbonColumns
   */
  void initialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws IOException;

  T readRow(Object[] data);

  /**
   * This method will be used to clear the dictionary cache and update access count for each
   * column involved which will be used during eviction of columns from LRU cache if memory
   * reaches threshold
   */
  void close();

}
