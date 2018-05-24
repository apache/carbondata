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

import org.apache.carbondata.spark.vectorreader.CarbonSparkVectorReader;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.ColumnVectorFactory;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigInteger;

public class CarbonVectorProxy implements CarbonSparkVectorReader {

    private ColumnarBatch columnarBatch;
    private WritableColumnVector[] writableColumnVectors;

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
        writableColumnVectors = ColumnVectorFactory
                .getColumnVector(memMode, new StructType(structFileds), rowNum);
        columnarBatch = new ColumnarBatch(writableColumnVectors);
    }

    public CarbonVectorProxy(MemoryMode memMode, int rowNum, StructType outputSchema) {
        writableColumnVectors = ColumnVectorFactory
                .getColumnVector(memMode, outputSchema, rowNum);
        columnarBatch = new ColumnarBatch(writableColumnVectors);
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
    public ColumnVector column(int ordinal) {
        return columnarBatch.column(ordinal);
    }

    /**
     * Resets this column for writing. The currently stored values are no longer accessible.
     */
    public void reset() {
        for (WritableColumnVector col : writableColumnVectors) {
            col.reset();
        }
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


    /**
     * This method will add the row to the corresponding column vector object
     *
     * @param rowId
     * @param value
     */
    public void putRowToColumnBatch(int rowId, Object value, int offset) {
        WritableColumnVector columnVector = null;
        if (columnarBatch.column(rowId) instanceof WritableColumnVector) {
            columnVector = (WritableColumnVector) columnarBatch.column(offset);
            org.apache.spark.sql.types.DataType t = columnVector.dataType();
            if (null == value) {
                columnVector.putNull(rowId);
            } else {
                if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
                    columnVector.putBoolean(rowId, (boolean) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
                    columnVector.putByte(rowId, (byte) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
                    columnVector.putShort(rowId, (short) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
                    columnVector.putInt(rowId, (int) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
                    columnVector.putLong(rowId, (long) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
                    columnVector.putFloat(rowId, (float) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
                    columnVector.putDouble(rowId, (double) value);
                } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
                    UTF8String v = (UTF8String) value;
                    columnVector.putByteArray(rowId, v.getBytes());
                } else if (t instanceof DecimalType) {
                    DecimalType dt = (DecimalType) t;
                    Decimal d = Decimal.fromDecimal(value);
                    if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
                        columnVector.putInt(rowId, (int) d.toUnscaledLong());
                    } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
                        columnVector.putLong(rowId, d.toUnscaledLong());
                    } else {
                        final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
                        byte[] bytes = integer.toByteArray();
                        columnVector.putByteArray(rowId, bytes, 0, bytes.length);
                    }
                } else if (t instanceof CalendarIntervalType) {
                    CalendarInterval c = (CalendarInterval) value;
                    columnVector.getChild(0).putInt(rowId, c.months);
                    columnVector.getChild(1).putLong(rowId, c.microseconds);
                } else if (t instanceof org.apache.spark.sql.types.DateType) {
                    columnVector.putInt(rowId, (int) value);
                } else if (t instanceof org.apache.spark.sql.types.TimestampType) {
                    columnVector.putLong(rowId, (long) value);
                }
            }
        }
    }

    public void putBoolean(int rowId, boolean value, int ordinal) {

        writableColumnVectors[ordinal].putBoolean(rowId, (boolean) value);
    }

    public void putByte(int rowId, byte value, int ordinal) {

        writableColumnVectors[ordinal].putByte(rowId, (byte) value);
    }

    public void putShort(int rowId, short value, int ordinal) {

        writableColumnVectors[ordinal].putShort(rowId, (short) value);
    }

    public void putInt(int rowId, int value, int ordinal) {

        writableColumnVectors[ordinal].putInt(rowId, (int) value);
    }

    public void putFloat(int rowId, float value, int ordinal) {

        writableColumnVectors[ordinal].putFloat(rowId, (float) value);
    }

    public void putLong(int rowId, long value, int ordinal) {

        writableColumnVectors[ordinal].putLong(rowId, (long) value);
    }

    public void putDouble(int rowId, double value, int ordinal) {

        writableColumnVectors[ordinal].putDouble(rowId, (double) value);
    }

    public void putByteArray(int rowId, byte[] value, int ordinal) {

        writableColumnVectors[ordinal].putByteArray(rowId, (byte[]) value);
    }

    public void putInts(int rowId, int count, int value, int ordinal) {

        writableColumnVectors[ordinal].putInts(rowId, count, value);
    }

    public void putShorts(int rowId, int count, short value, int ordinal) {

        writableColumnVectors[ordinal].putShorts(rowId, count, value);
    }

    public void putLongs(int rowId, int count, long value, int ordinal) {

        writableColumnVectors[ordinal].putLongs(rowId, count, value);
    }

    public void putDecimal(int rowId, Decimal value, int precision, int ordinal) {
        writableColumnVectors[ordinal].putDecimal(rowId, value, precision);

    }

    public void putDoubles(int rowId, int count, double value, int ordinal) {

        writableColumnVectors[ordinal].putDoubles(rowId, count, value);
    }

    public void putByteArray(int rowId, byte[] value, int offset, int length, int ordinal) {

        writableColumnVectors[ordinal].putByteArray(rowId, (byte[]) value, offset, length);
    }

    public void putNull(int rowId, int ordinal) {

        writableColumnVectors[ordinal].putNull(rowId);
    }

    public void putNulls(int rowId, int count, int ordinal) {

        writableColumnVectors[ordinal].putNulls(rowId, count);
    }

    public boolean isNullAt(int rowId, int ordinal) {

        return writableColumnVectors[ordinal].isNullAt(rowId);
    }

    public DataType dataType(int ordinal) {

        return writableColumnVectors[ordinal].dataType();
    }


}
