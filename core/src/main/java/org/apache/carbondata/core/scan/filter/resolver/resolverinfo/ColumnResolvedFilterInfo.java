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

package org.apache.carbondata.core.scan.filter.resolver.resolverinfo;

import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;

public abstract class ColumnResolvedFilterInfo {

  /**
   * in case column min/max cache is configured then this index will map the filter column to
   * its index in min/max byte array. For example
   * Total columns = 10
   * Columns to Be cached = 3
   * Column ordinals to be cached = 1,5,7
   * then when checking for isScanRequired column if we consider the above ordinal 5 and 7 then
   * ArrayIndexOutOfBoundException will be thrown although the min/max value for the ordinal 5
   * is present at array index 1 and for ordinal 7 at array index 2. To avoid these scenario this
   * index is maintained
   */
  protected int columnIndexInMinMaxByteArray = -1;
  /**
   * column index in file
   */
  protected int columnIndex = -1;

  public void setColumnIndexInMinMaxByteArray(int columnIndexInMinMaxByteArray) {
    this.columnIndexInMinMaxByteArray = columnIndexInMinMaxByteArray;
  }

  public int getColumnIndexInMinMaxByteArray() {
    // -1 means
    // 1. On driver side either the filter dimension does not exist in the cached min/max columns
    // or columns min/max to be cached are not specified
    // 2. For RowFilterExecutorImpl and ExcludeFilterExecutorImpl this value will be -1
    if (columnIndexInMinMaxByteArray == -1) {
      return columnIndex;
    }
    return columnIndexInMinMaxByteArray;
  }

  public abstract CarbonMeasure getMeasure();

  public abstract CarbonDimension getDimension();
}
