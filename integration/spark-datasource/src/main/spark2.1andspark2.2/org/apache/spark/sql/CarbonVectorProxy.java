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

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Adapter class which handles the columnar vector reading of the carbondata
 * based on the spark ColumnVector and ColumnarBatch API. This proxy class
 * handles the complexity of spark 2.1 version related api changes since
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
     * @param rowNum        rows number for vector reading
     * @param structFileds, metadata related to current schema of table.
     */
    public CarbonVectorProxy(MemoryMode memMode, int rowNum, StructField[] structFileds) {
        columnarBatch = ColumnarBatch.allocate(new StructType(structFileds), memMode, rowNum);
        columnVectorProxies = new ColumnVectorProxy[columnarBatch.numCols()];
        for (int i = 0; i < columnVectorProxies.length; i++) {
            columnVectorProxies[i] = new ColumnVectorProxy(columnarBatch, i);
        }
    }

    public CarbonVectorProxy(MemoryMode memMode, StructType outputSchema, int rowNum) {
        columnarBatch = ColumnarBatch.allocate(outputSchema, memMode, rowNum);
        columnVectorProxies = new ColumnVectorProxy[columnarBatch.numCols()];
        for (int i = 0; i < columnVectorProxies.length; i++) {
            columnVectorProxies[i] = new ColumnVectorProxy(columnarBatch, i);
        }
    }

    public ColumnVectorProxy getColumnVector(int ordinal) {
        return columnVectorProxies[ordinal];
    }

    /**
     * Sets the number of rows in this batch.
     */
    public void setNumRows(int numRows) {
        columnarBatch.setNumRows(numRows);
    }


    /**
     * Returns the number of rows for read, including filtered rows.
     */
    public int numRows() {
        return columnarBatch.capacity();
    }


    /**
     * Called to close all the columns in this batch. It is not valid to access the data after
     * calling this. This must be called at the end to clean up memory allocations.
     */
    public void close() {
        columnarBatch.close();
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

    public void resetDictionaryIds(int ordinal) {
        columnarBatch.column(ordinal).getDictionaryIds().reset();
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
        columnarBatch.reset();
    }


    public static class ColumnVectorProxy {

        private ColumnVector vector;

        public ColumnVectorProxy(ColumnarBatch columnarBatch, int ordinal) {
            this.vector = columnarBatch.column(ordinal);
        }

        public void putRowToColumnBatch(int rowId, Object value, int offset) {
            org.apache.spark.sql.types.DataType t = dataType(offset);
            if (null == value) {
                putNull(rowId, offset);
            } else {
                if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
                    putBoolean(rowId, (boolean) value, offset);
                } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
                    putByte(rowId, (byte) value, offset);
                } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
                    putShort(rowId, (short) value, offset);
                } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
                    putInt(rowId, (int) value, offset);
                } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
                    putLong(rowId, (long) value, offset);
                } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
                    putFloat(rowId, (float) value, offset);
                } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
                    putDouble(rowId, (double) value, offset);
                } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
                    UTF8String v = (UTF8String) value;
                    putByteArray(rowId, v.getBytes(), offset);
                } else if (t instanceof org.apache.spark.sql.types.DecimalType) {
                    DecimalType dt = (DecimalType) t;
                    Decimal d = Decimal.fromDecimal(value);
                    if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
                        putInt(rowId, (int) d.toUnscaledLong(), offset);
                    } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
                        putLong(rowId, d.toUnscaledLong(), offset);
                    } else {
                        final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
                        byte[] bytes = integer.toByteArray();
                        putByteArray(rowId, bytes, 0, bytes.length, offset);
                    }
                } else if (t instanceof CalendarIntervalType) {
                    CalendarInterval c = (CalendarInterval) value;
                    vector.getChildColumn(0).putInt(rowId, c.months);
                    vector.getChildColumn(1).putLong(rowId, c.microseconds);
                } else if (t instanceof org.apache.spark.sql.types.DateType) {
                    putInt(rowId, (int) value, offset);
                } else if (t instanceof org.apache.spark.sql.types.TimestampType) {
                    putLong(rowId, (long) value, offset);
                }
            }
        }

        public void putBoolean(int rowId, boolean value, int ordinal) {
            vector.putBoolean(rowId, value);
        }

        public void putByte(int rowId, byte value, int ordinal) {
            vector.putByte(rowId, value);
        }

        public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
            vector.putBytes(rowId, count, src, srcIndex);
        }

        public void putShort(int rowId, short value, int ordinal) {
            vector.putShort(rowId, value);
        }

        public void putInt(int rowId, int value, int ordinal) {
            vector.putInt(rowId, value);
        }

        public void putFloat(int rowId, float value, int ordinal) {
            vector.putFloat(rowId, value);
        }

        public void putFloats(int rowId, int count, float[] src, int srcIndex)  {
            vector.putFloats(rowId, count, src, srcIndex);
        }

        public void putLong(int rowId, long value, int ordinal) {
            vector.putLong(rowId, value);
        }

        public void putDouble(int rowId, double value, int ordinal) {
            vector.putDouble(rowId, value);
        }

        public void putByteArray(int rowId, byte[] value, int ordinal) {
            vector.putByteArray(rowId, value);
        }

        public void putInts(int rowId, int count, int value, int ordinal) {
            vector.putInts(rowId, count, value);
        }

        public void putInts(int rowId, int count, int[] src, int srcIndex) {
            vector.putInts(rowId, count, src, srcIndex);
        }

        public void putShorts(int rowId, int count, short value, int ordinal) {
            vector.putShorts(rowId, count, value);
        }

        public void putShorts(int rowId, int count, short[] src, int srcIndex) {
            vector.putShorts(rowId, count, src, srcIndex);
        }

        public void putLongs(int rowId, int count, long value, int ordinal) {
            vector.putLongs(rowId, count, value);
        }

        public void putLongs(int rowId, int count, long[] src, int srcIndex) {
            vector.putLongs(rowId, count, src, srcIndex);
        }

        public void putDecimal(int rowId, Decimal value, int precision, int ordinal) {
            vector.putDecimal(rowId, value, precision);

        }

        public void putDoubles(int rowId, int count, double value, int ordinal) {
            vector.putDoubles(rowId, count, value);
        }

        public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
            vector.putDoubles(rowId, count, src, srcIndex);
        }

        public void putByteArray(int rowId, byte[] value, int offset, int length, int ordinal) {
            vector.putByteArray(rowId, value, offset, length);
        }

        public boolean isNullAt(int rowId, int ordinal) {
            return vector.isNullAt(rowId);
        }

        public DataType dataType(int ordinal) {
            return vector.dataType();
        }

        public void putNotNull(int rowId, int ordinal) {
            vector.putNotNull(rowId);
        }

        public void putNotNulls(int rowId, int count, int ordinal) {
            vector.putNotNulls(rowId, count);
        }

        public void putDictionaryInt(int rowId, int value, int ordinal) {
            vector.getDictionaryIds().putInt(rowId, value);
        }

      public void setDictionary(CarbonDictionary dictionary, int ordinal) {
        if (null != dictionary) {
          vector.setDictionary(new CarbonDictionaryWrapper(Encoding.PLAIN, dictionary));
        } else {
          vector.setDictionary(null);
        }
      }

        public void putNull(int rowId, int ordinal) {
            vector.putNull(rowId);
        }

        public void putNulls(int rowId, int count, int ordinal) {
            vector.putNulls(rowId, count);
        }

        public boolean hasDictionary(int ordinal) {
            return vector.hasDictionary();
        }

        public Object reserveDictionaryIds(int capacity , int ordinal) {
            return vector.reserveDictionaryIds(capacity);
        }

     

    }
}
