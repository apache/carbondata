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
package org.apache.carbondata.presto;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;

public class CarbonVectorBatch {

  private static final int DEFAULT_BATCH_SIZE = 1024;

  private final StructField[] schema;
  private final int capacity;
  private int numRows;
  private final CarbonColumnVectorImpl[] columns;

  // True if the row is filtered.
  private final boolean[] filteredRows;

  // Column indices that cannot have null values.
  private final Set<Integer> nullFilteredColumns;

  // Total number of rows that have been filtered.
  private int numRowsFiltered = 0;


  private CarbonVectorBatch(StructField[] schema, int maxRows) {
    this.schema = schema;
    this.capacity = maxRows;
    this.columns = new CarbonColumnVectorImpl[schema.length];
    this.nullFilteredColumns = new HashSet<>();
    this.filteredRows = new boolean[maxRows];

    for (int i = 0; i < schema.length; ++i) {
      StructField field = schema[i];
      columns[i] = new CarbonColumnVectorImpl(maxRows, field.getDataType());
    }

  }


  public static CarbonVectorBatch allocate(StructField[] schema) {
    return new CarbonVectorBatch(schema, DEFAULT_BATCH_SIZE);
  }

  public static CarbonVectorBatch allocate(StructField[] schema,  int maxRows) {
    return new CarbonVectorBatch(schema, maxRows);
  }
  /**
   * Resets the batch for writing.
   */
  public void reset() {
    for (int i = 0; i < numCols(); ++i) {
      columns[i].reset();
    }
    if (this.numRowsFiltered > 0) {
      Arrays.fill(filteredRows, false);
    }
    this.numRows = 0;
    this.numRowsFiltered = 0;
  }


  /**
   * Returns the number of columns that make up this batch.
   */
  public int numCols() { return columns.length; }

  /**
   * Sets the number of rows that are valid. Additionally, marks all rows as "filtered" if one or
   * more of their attributes are part of a non-nullable column.
   */
  public void setNumRows(int numRows) {
    assert(numRows <= this.capacity);
    this.numRows = numRows;

    for (int ordinal : nullFilteredColumns) {
      for (int rowId = 0; rowId < numRows; rowId++) {
        if (!filteredRows[rowId] && columns[ordinal].isNull(rowId)) {
          filteredRows[rowId] = true;
          ++numRowsFiltered;
        }
      }
    }
  }


  /**
   * Returns the number of rows for read, including filtered rows.
   */
  public int numRows() { return numRows; }

  /**
   * Returns the number of valid rows.
   */
  public int numValidRows() {
    assert(numRowsFiltered <= numRows);
    return numRows - numRowsFiltered;
  }

  /**
   * Returns the column at `ordinal`.
   */
  public CarbonColumnVectorImpl column(int ordinal) { return columns[ordinal]; }

  /**
   * Returns the max capacity (in number of rows) for this batch.
   */
  public int capacity() { return capacity; }



}
