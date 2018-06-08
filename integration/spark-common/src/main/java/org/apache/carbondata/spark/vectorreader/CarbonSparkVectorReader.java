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
package org.apache.carbondata.spark.vectorreader;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;

/**
 * columnar vector reader interface based on spark columnar vector and
 * batch api's.
 */
public interface CarbonSparkVectorReader {


  /**
   * Returns the number of columns that make up this batch.
   */
  int numRows();


  /**
   * Resets this column for writing. The currently stored values are no longer accessible.
   */
  void reset();

  /**
   * Returns the row in this batch at `rowId`. Returned row is reused across calls.
   */
  InternalRow getRow(int rowId);


  /**
   * ColumnarBatch wraps multiple ColumnVectors as a row-wise table. It provides a row view of this
   * batch so that Spark can access the data row by row. Instance of it is meant to be reused
   * during the entire data loading process.
   */
  Object getColumnarBatch();

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after
   * calling this. This must be called at the end to clean up memory allocations.
   */
  void close();

  /**
   * Sets the number of rows in this batch.
   */
  void setNumRows(int numRows);


  /**
   * API will add a specific row to columnar batch using spark api
   *
   * @param rowId
   * @param value
   */
  void putRowToColumnBatch(int rowId, Object value, int offset);

  /**
   * API will add a specific row to columnar batch using spark api
   *
   * @param rowId
   * @param value
   */
  void putBoolean(int rowId, boolean value, int ordinal);

  void putByte(int rowId, byte value, int ordinal);

  void putShort(int rowId, short value, int ordinal);

  void putInt(int rowId, int value, int ordinal);

  void putFloat(int rowId, float value, int ordinal);

  void putLong(int rowId, long value, int ordinal);

  void putDouble(int rowId, double value, int ordinal);

  void putByteArray(int rowId, byte[] value, int ordinal);

  void putInts(int rowId, int count, int value, int ordinal);

  void putShorts(int rowId, int count, short value, int ordinal);

  void putLongs(int rowId, int count, long value, int ordinal);

  void putDecimal(int rowId, Decimal value, int precision, int ordinal);

  void putDoubles(int rowId, int count, double value, int ordinal);

  void putByteArray(int rowId, byte[] value, int offset, int length, int ordinal);

  void putNull(int rowId, int ordinal);

  void putNulls(int rowId, int count, int ordinal);

  boolean isNullAt(int rowId, int ordinal);

  DataType dataType(int ordinal);


}
