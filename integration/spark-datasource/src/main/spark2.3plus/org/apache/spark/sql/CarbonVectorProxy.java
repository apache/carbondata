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

import java.math.BigInteger;

import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.scanner.LazyPageLoader;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnarMap;
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
        WritableColumnVector[] columnVectors = ColumnVectorFactory
                .getColumnVector(memMode, outputSchema, rowNum);
        columnVectorProxies = new ColumnVectorProxy[columnVectors.length];
        for (int i = 0; i < columnVectorProxies.length; i++) {
          if (useLazyLoad) {
            columnVectorProxies[i] = new ColumnVectorProxyWithLazyLoad(columnVectors[i]);
          } else {
            columnVectorProxies[i] = new ColumnVectorProxy(columnVectors[i]);
          }
        }
        columnarBatch = new ColumnarBatch(columnVectorProxies);
        columnarBatch.setNumRows(rowNum);
    }

    /**
     * Returns the number of rows for read, including filtered rows.
     */
    public int numRows() {
        return columnarBatch.numRows();
    }

    /**
     * This API will return a columnvector from a batch of column vector rows
     * based on the ordinal
     *
     * @param ordinal
     * @return
     */
    public WritableColumnVector column(int ordinal) {
        return ((ColumnVectorProxy) columnarBatch.column(ordinal)).getVector();
    }

    public ColumnVectorProxy getColumnVector(int ordinal) {
        return columnVectorProxies[ordinal];
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

        private WritableColumnVector vector;

        public ColumnVectorProxy(ColumnVector columnVector) {
            super(columnVector.dataType());
            vector = (WritableColumnVector) columnVector;
        }

        public void putRowToColumnBatch(int rowId, Object value) {
            org.apache.spark.sql.types.DataType t = vector.dataType();
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
                    vector.getChild(0).putInt(rowId, c.months);
                    vector.getChild(1).putLong(rowId, c.microseconds);
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

        public void putFloats(int rowId, int count, float[] src, int srcIndex)  {
            vector.putFloats(rowId, count, src, srcIndex);
        }

        public void putLong(int rowId, long value) {
            vector.putLong(rowId, value);
        }

        public void putDouble(int rowId, double value) {
            vector.putDouble(rowId, value);
        }

        public void putByteArray(int rowId, byte[] value) {
            vector.putByteArray(rowId, value);
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

        public void putDecimal(int rowId, Decimal value, int precision) {
            vector.putDecimal(rowId, value, precision);

        }

        public void putDoubles(int rowId, int count, double value) {
            vector.putDoubles(rowId, count, value);
        }

        public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
            vector.putDoubles(rowId, count, src, srcIndex);
        }

        public void putByteArray(int rowId, byte[] value, int offset, int length) {
            vector.putByteArray(rowId, value, offset, length);
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
          vector.setDictionary(new CarbonDictionaryWrapper(dictionary));
        } else {
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

        public Object reserveDictionaryIds(int capacity) {
            return vector.reserveDictionaryIds(capacity);
        }

        @Override
        public boolean isNullAt(int i) {
            return vector.isNullAt(i);
        }

        @Override
        public boolean getBoolean(int i) {
            return vector.getBoolean(i);
        }

        @Override
        public byte getByte(int i) {
            return vector.getByte(i);
        }

        @Override
        public short getShort(int i) {
            return vector.getShort(i);
        }

        @Override
        public int getInt(int i) {
            return vector.getInt(i);
        }

        @Override
        public long getLong(int i) {
            return vector.getLong(i);
        }

        @Override
        public float getFloat(int i) {
            return vector.getFloat(i);
        }

        @Override
        public double getDouble(int i) {
            return vector.getDouble(i);
        }

        @Override
        public void close() {
            vector.close();
        }

        @Override
        public boolean hasNull() {
            return vector.hasNull();
        }

        @Override
        public int numNulls() {
            return vector.numNulls();
        }

        @Override
        public ColumnarArray getArray(int i) {
            return vector.getArray(i);
        }

        @Override
        public ColumnarMap getMap(int i) {
            return vector.getMap(i);
        }

        @Override
        public Decimal getDecimal(int i, int i1, int i2) {
            return vector.getDecimal(i, i1, i2);
        }

        @Override
        public UTF8String getUTF8String(int i) {
            return vector.getUTF8String(i);
        }

        @Override
        public byte[] getBinary(int i) {
            return vector.getBinary(i);
        }

        @Override
        protected ColumnVector getChild(int i) {
            return vector.getChild(i);
        }

        public void reset() {
            vector.reset();
        }

        public void setLazyPage(LazyPageLoader lazyPage) {
            lazyPage.loadPage();
        }

      /**
       * It keeps all binary data of all rows to it.
       * Should use along with @{putArray(int rowId, int offset, int length)} to keep lengths
       * and offset.
       */
      public void putAllByteArray(byte[] data, int offset, int length) {
        vector.arrayData().appendBytes(length, data, offset);
      }

      public void putArray(int rowId, int offset, int length) {
        vector.putArray(rowId, offset, length);
      }

      public WritableColumnVector getVector() {
            return vector;
        }
    }

  public static class ColumnVectorProxyWithLazyLoad extends ColumnVectorProxy {

    private WritableColumnVector vector;

    private LazyPageLoader pageLoad;

    private boolean isLoaded;

    public ColumnVectorProxyWithLazyLoad(ColumnVector columnVector) {
      super(columnVector);
      vector = (WritableColumnVector) columnVector;
    }

    @Override
    public boolean isNullAt(int i) {
      checkPageLoaded();
      return vector.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int i) {
      checkPageLoaded();
      return vector.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {
      checkPageLoaded();
      return vector.getByte(i);
    }

    @Override
    public short getShort(int i) {
      checkPageLoaded();
      return vector.getShort(i);
    }

    @Override
    public int getInt(int i) {
      checkPageLoaded();
      return vector.getInt(i);
    }

    @Override
    public long getLong(int i) {
      checkPageLoaded();
      return vector.getLong(i);
    }

    @Override
    public float getFloat(int i) {
      checkPageLoaded();
      return vector.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
      checkPageLoaded();
      return vector.getDouble(i);
    }

    @Override
    public boolean hasNull() {
      checkPageLoaded();
      return vector.hasNull();
    }

    @Override
    public int numNulls() {
      checkPageLoaded();
      return vector.numNulls();
    }

    @Override
    public ColumnarArray getArray(int i) {
      checkPageLoaded();
      return vector.getArray(i);
    }

    @Override
    public ColumnarMap getMap(int i) {
      checkPageLoaded();
      return vector.getMap(i);
    }

    @Override
    public Decimal getDecimal(int i, int i1, int i2) {
      checkPageLoaded();
      return vector.getDecimal(i, i1, i2);
    }

    @Override
    public UTF8String getUTF8String(int i) {
      checkPageLoaded();
      return vector.getUTF8String(i);
    }

    @Override
    public byte[] getBinary(int i) {
      checkPageLoaded();
      return vector.getBinary(i);
    }

    @Override
    protected ColumnVector getChild(int i) {
      checkPageLoaded();
      return vector.getChild(i);
    }

    public void reset() {
      isLoaded = false;
      pageLoad = null;
      vector.reset();
    }

    private void checkPageLoaded() {
      if (!isLoaded) {
        if (pageLoad != null) {
          pageLoad.loadPage();
        }
        isLoaded = true;
      }
    }

    public void setLazyPage(LazyPageLoader lazyPage) {
      this.pageLoad = lazyPage;
    }

  }
}
