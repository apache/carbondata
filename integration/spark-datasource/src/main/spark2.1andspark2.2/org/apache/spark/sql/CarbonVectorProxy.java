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
package org.apache.spark.sql;

import java.lang.reflect.Field;
import java.math.BigInteger;

import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.scanner.LazyPageLoader;

import org.apache.parquet.column.Encoding;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Adapter class which handles the columnar vector reading of the carbondata
 * based on the spark ColumnVector and ColumnarBatch API. This proxy class
 * handles the complexity of spark 2.3 version related api changes since
 * spark ColumnVector and ColumnarBatch interfaces are still evolving.
 */
public class CarbonVectorProxy {

  private ColumnarBatch columnarBatch;
  private ColumnVectorProxy[] columnVectorProxies;


  private void updateColumnVectors() {
    try {
      Field field = columnarBatch.getClass().getDeclaredField("columns");
      field.setAccessible(true);
      field.set(columnarBatch, columnVectorProxies);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Adapter class which handles the columnar vector reading of the carbondata
   * based on the spark ColumnVector and ColumnarBatch API. This proxy class
   * handles the complexity of spark 2.3 version related api changes since
   * spark ColumnVector and ColumnarBatch interfaces are still evolving.
   *
   * @param memMode       which represent the type onheap or offheap vector.
   * @param outputSchema, metadata related to current schema of table.
   * @param rowNum        rows number for vector reading
   * @param useLazyLoad   Whether to use lazy load while getting the data.
   */
  public CarbonVectorProxy(MemoryMode memMode, StructType outputSchema, int rowNum,
      boolean useLazyLoad) {
    columnarBatch = ColumnarBatch.allocate(outputSchema, memMode, rowNum);
    columnVectorProxies = new ColumnVectorProxy[columnarBatch.numCols()];
    for (int i = 0; i < columnVectorProxies.length; i++) {
      if (useLazyLoad) {
        columnVectorProxies[i] =
            new ColumnVectorProxyWithLazyLoad(columnarBatch.column(i), rowNum, memMode);
      } else {
        columnVectorProxies[i] = new ColumnVectorProxy(columnarBatch.column(i), rowNum, memMode);
      }
    }
    updateColumnVectors();
  }

  public ColumnVectorProxy getColumnVector(int ordinal) {
    return columnVectorProxies[ordinal];
  }

  /**
   * Returns the number of rows for read, including filtered rows.
   */
  public int numRows() {
    return columnarBatch.capacity();
  }

  /**
   * This API will return a columnvector from a batch of column vector rows
   * based on the ordinal
   *
   * @param ordinal
   * @return
   */
  public ColumnVector column(int ordinal) {
    return columnarBatch.column(ordinal);
  }

  /**
   * Resets this column for writing. The currently stored values are no longer accessible.
   */
  public void reset() {
    for (int i = 0; i < columnarBatch.numCols(); i++) {
      ((ColumnVectorProxy) columnarBatch.column(i)).reset();
    }
  }

  public void resetDictionaryIds(int ordinal) {
    (((ColumnVectorProxy) columnarBatch.column(ordinal)).getVector()).getDictionaryIds().reset();
  }

  /**
   * Returns the row in this batch at `rowId`. Returned row is reused across calls.
   */
  public InternalRow getRow(int rowId) {
    return columnarBatch.getRow(rowId);
  }

  /**
   * Returns the row in this batch at `rowId`. Returned row is reused across calls.
   */
  public Object getColumnarBatch() {
    return columnarBatch;
  }

  /**
   * Called to close all the columns in this batch. It is not valid to access the data after
   * calling this. This must be called at the end to clean up memory allocations.
   */
  public void close() {
    columnarBatch.close();
  }

  /**
   * Sets the number of rows in this batch.
   */
  public void setNumRows(int numRows) {
    columnarBatch.setNumRows(numRows);
  }

  public DataType dataType(int ordinal) {
    return columnarBatch.column(ordinal).dataType();
  }

  public static class ColumnVectorProxy extends ColumnVector {

    private ColumnVector vector;

    public ColumnVectorProxy(ColumnVector columnVector, int capacity, MemoryMode mode) {
      super(capacity, columnVector.dataType(), mode);
      try {
        Field childColumns =
            columnVector.getClass().getSuperclass().getDeclaredField("childColumns");
        childColumns.setAccessible(true);
        Object o = childColumns.get(columnVector);
        childColumns.set(this, o);
        Field resultArray =
            columnVector.getClass().getSuperclass().getDeclaredField("resultArray");
        resultArray.setAccessible(true);
        Object o1 = resultArray.get(columnVector);
        resultArray.set(this, o1);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      vector = columnVector;
    }

    public void putRowToColumnBatch(int rowId, Object value) {
      org.apache.spark.sql.types.DataType t = dataType();
      if (null == value) {
        putNull(rowId);
      } else {
        if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
          putBoolean(rowId, (boolean) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
          putByte(rowId, (byte) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
          putShort(rowId, (short) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
          putInt(rowId, (int) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
          putLong(rowId, (long) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
          putFloat(rowId, (float) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
          putDouble(rowId, (double) value);
        } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
          UTF8String v = (UTF8String) value;
          putByteArray(rowId, v.getBytes());
        } else if (t instanceof org.apache.spark.sql.types.DecimalType) {
          DecimalType dt = (DecimalType) t;
          Decimal d = Decimal.fromDecimal(value);
          if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
            putInt(rowId, (int) d.toUnscaledLong());
          } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
            putLong(rowId, d.toUnscaledLong());
          } else {
            final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
            byte[] bytes = integer.toByteArray();
            putByteArray(rowId, bytes, 0, bytes.length);
          }
        } else if (t instanceof CalendarIntervalType) {
          CalendarInterval c = (CalendarInterval) value;
          vector.getChildColumn(0).putInt(rowId, c.months);
          vector.getChildColumn(1).putLong(rowId, c.microseconds);
        } else if (t instanceof org.apache.spark.sql.types.DateType) {
          putInt(rowId, (int) value);
        } else if (t instanceof org.apache.spark.sql.types.TimestampType) {
          putLong(rowId, (long) value);
        }
      }
    }

    public void putBoolean(int rowId, boolean value) {
      vector.putBoolean(rowId, value);
    }

    public void putByte(int rowId, byte value) {
      vector.putByte(rowId, value);
    }

    public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
      vector.putBytes(rowId, count, src, srcIndex);
    }

    public void putShort(int rowId, short value) {
      vector.putShort(rowId, value);
    }

    public void putInt(int rowId, int value) {
      vector.putInt(rowId, value);
    }

    public void putFloat(int rowId, float value) {
      vector.putFloat(rowId, value);
    }

    public void putFloats(int rowId, int count, float[] src, int srcIndex) {
      vector.putFloats(rowId, count, src, srcIndex);
    }

    public void putLong(int rowId, long value) {
      vector.putLong(rowId, value);
    }

    public void putDouble(int rowId, double value) {
      vector.putDouble(rowId, value);
    }

    public void putInts(int rowId, int count, int value) {
      vector.putInts(rowId, count, value);
    }

    public void putInts(int rowId, int count, int[] src, int srcIndex) {
      vector.putInts(rowId, count, src, srcIndex);
    }

    public void putShorts(int rowId, int count, short value) {
      vector.putShorts(rowId, count, value);
    }

    public void putShorts(int rowId, int count, short[] src, int srcIndex) {
      vector.putShorts(rowId, count, src, srcIndex);
    }

    public void putLongs(int rowId, int count, long value) {
      vector.putLongs(rowId, count, value);
    }

    public void putLongs(int rowId, int count, long[] src, int srcIndex) {
      vector.putLongs(rowId, count, src, srcIndex);
    }

    public void putDoubles(int rowId, int count, double value) {
      vector.putDoubles(rowId, count, value);
    }

    public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
      vector.putDoubles(rowId, count, src, srcIndex);
    }

    public DataType dataType(int ordinal) {
      return vector.dataType();
    }

    public void putNotNull(int rowId) {
      vector.putNotNull(rowId);
    }

    public void putNotNulls(int rowId, int count) {
      vector.putNotNulls(rowId, count);
    }

    public void putDictionaryInt(int rowId, int value) {
      vector.getDictionaryIds().putInt(rowId, value);
    }

    public void setDictionary(CarbonDictionary dictionary) {
      if (null != dictionary) {
        CarbonDictionaryWrapper dictionaryWrapper =
            new CarbonDictionaryWrapper(Encoding.PLAIN, dictionary);
        vector.setDictionary(dictionaryWrapper);
        this.dictionary = dictionaryWrapper;
      } else {
        this.dictionary = null;
        vector.setDictionary(null);
      }
    }

    public void putNull(int rowId) {
      vector.putNull(rowId);
    }

    public void putNulls(int rowId, int count) {
      vector.putNulls(rowId, count);
    }

    public boolean hasDictionary() {
      return vector.hasDictionary();
    }

    public ColumnVector reserveDictionaryIds(int capacity) {
      this.dictionaryIds = vector.reserveDictionaryIds(capacity);
      return dictionaryIds;
    }

    @Override public boolean isNullAt(int i) {
      return vector.isNullAt(i);
    }

    @Override public boolean getBoolean(int i) {
      return vector.getBoolean(i);
    }

    @Override public byte getByte(int i) {
      return vector.getByte(i);
    }

    @Override public short getShort(int i) {
      return vector.getShort(i);
    }

    @Override public int getInt(int i) {
      return vector.getInt(i);
    }

    @Override public long getLong(int i) {
      return vector.getLong(i);
    }

    @Override public float getFloat(int i) {
      return vector.getFloat(i);
    }

    @Override public double getDouble(int i) {
      return vector.getDouble(i);
    }

    @Override protected void reserveInternal(int capacity) {
    }

    @Override public void reserve(int requiredCapacity) {
      vector.reserve(requiredCapacity);
    }

    @Override public long nullsNativeAddress() {
      return vector.nullsNativeAddress();
    }

    @Override public long valuesNativeAddress() {
      return vector.valuesNativeAddress();
    }

    @Override public void putBooleans(int rowId, int count, boolean value) {
      vector.putBooleans(rowId, count, value);
    }

    @Override public void putBytes(int rowId, int count, byte value) {
      vector.putBytes(rowId, count, value);
    }

    @Override public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
      vector.putIntsLittleEndian(rowId, count, src, srcIndex);
    }

    @Override public int getDictId(int rowId) {
      return vector.getDictId(rowId);
    }

    @Override public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
      vector.putLongsLittleEndian(rowId, count, src, srcIndex);
    }

    @Override public void putFloats(int rowId, int count, float value) {
      vector.putFloats(rowId, count, value);
    }

    @Override public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
      vector.putFloats(rowId, count, src, srcIndex);
    }

    @Override public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
      vector.putDoubles(rowId, count, src, srcIndex);
    }

    @Override public void putArray(int rowId, int offset, int length) {
      vector.putArray(rowId, offset, length);
    }

    @Override public int getArrayLength(int rowId) {
      return vector.getArrayLength(rowId);
    }

    @Override public int getArrayOffset(int rowId) {
      return vector.getArrayOffset(rowId);
    }

    @Override public void loadBytes(Array array) {
      vector.loadBytes(array);
    }

    @Override public int putByteArray(int rowId, byte[] value, int offset, int count) {
      return vector.putByteArray(rowId, value, offset, count);
    }

    /**
     * It keeps all binary data of all rows to it.
     * Should use along with @{putArray(int rowId, int offset, int length)} to keep lengths
     * and offset.
     */
    public void putAllByteArray(byte[] data, int offset, int length) {
      vector.arrayData().appendBytes(length, data, offset);
    }

    @Override public void close() {
      vector.close();
    }

    public void reset() {
      if (isConstant) {
        return;
      }
      vector.reset();
    }

    public void setLazyPage(LazyPageLoader lazyPage) {
      lazyPage.loadPage();
    }

    public ColumnVector getVector() {
      return vector;
    }
  }

  public static class ColumnVectorProxyWithLazyLoad extends ColumnVectorProxy {

    private ColumnVector vector;

    private LazyPageLoader pageLoad;

    private boolean isLoaded;

    public ColumnVectorProxyWithLazyLoad(ColumnVector columnVector, int capacity, MemoryMode mode) {
      super(columnVector, capacity, mode);
      vector = columnVector;
    }

    @Override public boolean isNullAt(int i) {
      checkPageLoaded();
      return vector.isNullAt(i);
    }

    @Override public boolean getBoolean(int i) {
      checkPageLoaded();
      return vector.getBoolean(i);
    }

    @Override public byte getByte(int i) {
      checkPageLoaded();
      return vector.getByte(i);
    }

    @Override public short getShort(int i) {
      checkPageLoaded();
      return vector.getShort(i);
    }

    @Override public int getInt(int i) {
      checkPageLoaded();
      return vector.getInt(i);
    }

    @Override public long getLong(int i) {
      checkPageLoaded();
      return vector.getLong(i);
    }

    @Override public float getFloat(int i) {
      checkPageLoaded();
      return vector.getFloat(i);
    }

    @Override public double getDouble(int i) {
      checkPageLoaded();
      return vector.getDouble(i);
    }

    @Override public int getArrayLength(int rowId) {
      checkPageLoaded();
      return vector.getArrayLength(rowId);
    }

    @Override public int getArrayOffset(int rowId) {
      checkPageLoaded();
      return vector.getArrayOffset(rowId);
    }

    private void checkPageLoaded() {
      if (!isLoaded) {
        if (pageLoad != null) {
          pageLoad.loadPage();
        }
        isLoaded = true;
      }
    }

    public void reset() {
      if (isConstant) {
        return;
      }
      isLoaded = false;
      vector.reset();
    }

    public void setLazyPage(LazyPageLoader lazyPage) {
      this.pageLoad = lazyPage;
    }

  }
}
