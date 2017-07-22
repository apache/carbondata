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

package org.apache.carbondata.core.datastore.page;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsVO;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.util.CarbonProperties;

import static org.apache.carbondata.core.metadata.datatype.DataType.BYTE;
import static org.apache.carbondata.core.metadata.datatype.DataType.BYTE_ARRAY;
import static org.apache.carbondata.core.metadata.datatype.DataType.DECIMAL;
import static org.apache.carbondata.core.metadata.datatype.DataType.DOUBLE;
import static org.apache.carbondata.core.metadata.datatype.DataType.FLOAT;
import static org.apache.carbondata.core.metadata.datatype.DataType.INT;
import static org.apache.carbondata.core.metadata.datatype.DataType.LONG;
import static org.apache.carbondata.core.metadata.datatype.DataType.SHORT;
import static org.apache.carbondata.core.metadata.datatype.DataType.SHORT_INT;

public abstract class ColumnPage {

  protected final int pageSize;
  protected final DataType dataType;
  protected int scale;
  protected int precision;

  // statistics of this column page
  private ColumnPageStatsVO stats;

  // The index of the rowId whose value is null, will be set to 1
  private BitSet nullBitSet;

  protected DecimalConverterFactory.DecimalConverter decimalConverter;

  protected static final boolean unsafe = Boolean.parseBoolean(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING,
          CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING_DEFAULT));

  protected ColumnPage(DataType dataType, int pageSize, int scale, int precision) {
    this.dataType = dataType;
    this.pageSize = pageSize;
    this.scale = scale;
    this.precision = precision;
    this.stats = new ColumnPageStatsVO(dataType);
    this.nullBitSet = new BitSet(pageSize);
    if (dataType == DECIMAL) {
      decimalConverter = DecimalConverterFactory.INSTANCE.getDecimalConverter(precision, scale);
    }
  }

  public DataType getDataType() {
    return dataType;
  }

  public ColumnPageStatsVO getStatistics() {
    return stats;
  }

  public int getPageSize() {
    return pageSize;
  }

  private static ColumnPage createVarLengthPage(DataType dataType, int pageSize, int scale,
      int precision) {
    if (unsafe) {
      try {
        return new UnsafeVarLengthColumnPage(dataType, pageSize, scale, precision);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeVarLengthColumnPage(dataType, pageSize, scale, precision);
    }
  }

  private static ColumnPage createFixLengthPage(DataType dataType, int pageSize, int scale,
      int precision) {
    if (unsafe) {
      try {
        return new UnsafeFixLengthColumnPage(dataType, pageSize, scale, precision);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeFixLengthColumnPage(dataType, pageSize, scale, pageSize);
    }
  }

  private static ColumnPage createPage(DataType dataType, int pageSize, int scale, int precision) {
    if (dataType.equals(BYTE_ARRAY) || dataType.equals(DECIMAL)) {
      return createVarLengthPage(dataType, pageSize, scale, precision);
    } else {
      return createFixLengthPage(dataType, pageSize, scale, precision);
    }
  }

  public static ColumnPage newVarLengthPage(DataType dataType, int pageSize) {
    return newVarLengthPage(dataType, pageSize, -1, -1);
  }

  private static ColumnPage newVarLengthPage(DataType dataType, int pageSize, int scale,
      int precision) {
    if (unsafe) {
      try {
        return new UnsafeVarLengthColumnPage(dataType, pageSize, scale, precision);
      } catch (MemoryException e) {
        throw new RuntimeException(e);
      }
    } else {
      return new SafeVarLengthColumnPage(dataType, pageSize, scale, precision);
    }
  }

  /**
   * Create a new page of dataType and number of row = pageSize
   */
  public static ColumnPage newPage(DataType dataType, int pageSize, int scale, int precision)
      throws MemoryException {
    ColumnPage instance;
    if (unsafe) {
      switch (dataType) {
        case BYTE:
        case SHORT:
        case SHORT_INT:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
          instance = new UnsafeFixLengthColumnPage(dataType, pageSize, scale, precision);
          break;
        case DECIMAL:
        case BYTE_ARRAY:
          instance = new UnsafeVarLengthColumnPage(dataType, pageSize, scale, precision);
          break;
        default:
          throw new RuntimeException("Unsupported data dataType: " + dataType);
      }
    } else {
      switch (dataType) {
        case BYTE:
          instance = newBytePage(new byte[pageSize]);
          break;
        case SHORT:
          instance = newShortPage(new short[pageSize]);
          break;
        case SHORT_INT:
          instance = newShortIntPage(new byte[pageSize * 3]);
          break;
        case INT:
          instance = newIntPage(new int[pageSize]);
          break;
        case LONG:
          instance = newLongPage(new long[pageSize]);
          break;
        case FLOAT:
          instance = newFloatPage(new float[pageSize]);
          break;
        case DOUBLE:
          instance = newDoublePage(new double[pageSize]);
          break;
        case DECIMAL:
          instance = newDecimalPage(new byte[pageSize][], scale, precision);
          break;
        default:
          throw new RuntimeException("Unsupported data dataType: " + dataType);
      }
    }
    return instance;
  }

  private static ColumnPage newBytePage(byte[] byteData) {
    ColumnPage columnPage = createPage(BYTE, byteData.length,  -1, -1);
    columnPage.setBytePage(byteData);
    return columnPage;
  }

  private static ColumnPage newShortPage(short[] shortData) {
    ColumnPage columnPage = createPage(SHORT, shortData.length,  -1, -1);
    columnPage.setShortPage(shortData);
    return columnPage;
  }

  private static ColumnPage newShortIntPage(byte[] shortIntData) {
    ColumnPage columnPage = createPage(SHORT_INT, shortIntData.length / 3,  -1, -1);
    columnPage.setShortIntPage(shortIntData);
    return columnPage;
  }

  private static ColumnPage newIntPage(int[] intData) {
    ColumnPage columnPage = createPage(INT, intData.length,  -1, -1);
    columnPage.setIntPage(intData);
    return columnPage;
  }

  private static ColumnPage newLongPage(long[] longData) {
    ColumnPage columnPage = createPage(LONG, longData.length,  -1, -1);
    columnPage.setLongPage(longData);
    return columnPage;
  }

  private static ColumnPage newFloatPage(float[] floatData) {
    ColumnPage columnPage = createPage(FLOAT, floatData.length,  -1, -1);
    columnPage.setFloatPage(floatData);
    return columnPage;
  }

  private static ColumnPage newDoublePage(double[] doubleData) {
    ColumnPage columnPage = createPage(DOUBLE, doubleData.length, -1, -1);
    columnPage.setDoublePage(doubleData);
    return columnPage;
  }

  private static ColumnPage newDecimalPage(byte[][] byteArray, int scale, int precision) {
    ColumnPage columnPage = createPage(DECIMAL, byteArray.length, scale, precision);
    columnPage.setByteArrayPage(byteArray);
    return columnPage;
  }

  private static ColumnPage newDecimalPage(byte[] lvEncodedByteArray, int scale, int precision)
      throws MemoryException {
    return VarLengthColumnPageBase.newDecimalColumnPage(lvEncodedByteArray, scale, precision);
  }

  private static ColumnPage newVarLengthPage(byte[] lvEncodedByteArray, int scale, int precision)
      throws MemoryException {
    return VarLengthColumnPageBase.newVarLengthColumnPage(lvEncodedByteArray, scale, precision);
  }

  /**
   * Set byte values to page
   */
  public abstract void setBytePage(byte[] byteData);

  /**
   * Set short values to page
   */
  public abstract void setShortPage(short[] shortData);

  /**
   * Set short int values to page
   */
  public abstract void setShortIntPage(byte[] shortIntData);

  /**
   * Set int values to page
   */
  public abstract void setIntPage(int[] intData);

  /**
   * Set long values to page
   */
  public abstract void setLongPage(long[] longData);

  /**
   * Set float values to page
   */
  public abstract void setFloatPage(float[] floatData);

  /**
   * Set double value to page
   */
  public abstract void setDoublePage(double[] doubleData);

  /**
   * Set byte array to page
   */
  public abstract void setByteArrayPage(byte[][] byteArray);

  /**
   * free memory as needed
   */
  public abstract void freeMemory();

  /**
   * Set value at rowId
   */
  public void putData(int rowId, Object value) {
    if (value == null) {
      putNull(rowId);
      stats.updateNull();
      return;
    }
    switch (dataType) {
      case BYTE:
        // TODO: change sort step to store as exact data type
        putByte(rowId, (byte) value);
        break;
      case SHORT:
        putShort(rowId, (short) value);
        break;
      case INT:
        putInt(rowId, (int) value);
        break;
      case LONG:
        putLong(rowId, (long) value);
        break;
      case DOUBLE:
        putDouble(rowId, (double) value);
        break;
      case DECIMAL:
        putDecimal(rowId, (BigDecimal) value);
        break;
      case BYTE_ARRAY:
        putBytes(rowId, (byte[]) value);
        break;
      default:
        throw new RuntimeException("unsupported data type: " + dataType);
    }
    stats.update(value);
  }

  /**
   * Set byte value at rowId
   */
  public abstract void putByte(int rowId, byte value);

  /**
   * Set short value at rowId
   */
  public abstract void putShort(int rowId, short value);

  /**
   * Set integer value at rowId
   */
  public abstract void putInt(int rowId, int value);

  /**
   * Set long value at rowId
   */
  public abstract void putLong(int rowId, long value);

  /**
   * Set double value at rowId
   */
  public abstract void putDouble(int rowId, double value);

  /**
   * Set byte array value at rowId
   */
  public abstract void putBytes(int rowId, byte[] bytes);

  /**
   * Set byte array value at rowId
   */
  public abstract void putDecimal(int rowId, BigDecimal decimal);

  /**
   * Type cast int value to 3 bytes value and set at rowId
   */
  public abstract void putShortInt(int rowId, int value);

  /**
   * Set byte array from offset to length at rowId
   */
  public abstract void putBytes(int rowId, byte[] bytes, int offset, int length);


  /**
   * Set null at rowId
   */
  private void putNull(int rowId) {
    nullBitSet.set(rowId);
    switch (dataType) {
      case BYTE:
        putByte(rowId, (byte) 0);
        break;
      case SHORT:
        putShort(rowId, (short) 0);
        break;
      case INT:
        putInt(rowId, 0);
        break;
      case LONG:
        putLong(rowId, 0L);
        break;
      case DOUBLE:
        putDouble(rowId, 0.0);
        break;
      case DECIMAL:
        putDecimal(rowId, BigDecimal.ZERO);
        break;
    }
  }

  /**
   * Get null bitset page
   */
  public BitSet getNullBitSet() {
    return nullBitSet;
  }

  /**
   * Get byte value at rowId
   */
  public abstract byte getByte(int rowId);

  /**
   * Get short value at rowId
   */
  public abstract short getShort(int rowId);

  /**
   * Get short int value at rowId
   */
  public abstract int getShortInt(int rowId);

  /**
   * Get int value at rowId
   */
  public abstract int getInt(int rowId);

  /**
   * Get long value at rowId
   */
  public abstract long getLong(int rowId);

  /**
   * Get float value at rowId
   */
  public abstract float getFloat(int rowId);

  /**
   * Get double value at rowId
   */
  public abstract double getDouble(int rowId);

  /**
   * Get decimal value at rowId
   */
  public abstract BigDecimal getDecimal(int rowId);

  /**
   * Get byte value page
   */
  public abstract byte[] getBytePage();

  /**
   * Get short value page
   */
  public abstract short[] getShortPage();

  /**
   * Get short int value page
   */
  public abstract byte[] getShortIntPage();

  /**
   * Get int value page
   */
  public abstract int[] getIntPage();

  /**
   * Get long value page
   */
  public abstract long[] getLongPage();

  /**
   * Get float value page
   */
  public abstract float[] getFloatPage();

  /**
   * Get double value page
   */
  public abstract double[] getDoublePage();

  /**
   * Get variable length page data
   */
  public abstract byte[][] getByteArrayPage();

  /**
   * For variable length page, get the flattened data
   */
  public abstract byte[] getFlattenedBytePage();

  /**
   * For decimals
   */
  public abstract byte[] getDecimalPage();

  /**
   * Encode the page data by codec (Visitor)
   */
  public abstract void encode(PrimitiveCodec codec);

  /**
   * Compress page data using specified compressor
   */
  public byte[] compress(Compressor compressor) throws MemoryException, IOException {
    switch (dataType) {
      case BYTE:
        return compressor.compressByte(getBytePage());
      case SHORT:
        return compressor.compressShort(getShortPage());
      case SHORT_INT:
        return compressor.compressByte(getShortIntPage());
      case INT:
        return compressor.compressInt(getIntPage());
      case LONG:
        return compressor.compressLong(getLongPage());
      case FLOAT:
        return compressor.compressFloat(getFloatPage());
      case DOUBLE:
        return compressor.compressDouble(getDoublePage());
      case DECIMAL:
        return compressor.compressByte(getDecimalPage());
      case BYTE_ARRAY:
        return compressor.compressByte(getFlattenedBytePage());
      default:
        throw new UnsupportedOperationException("unsupport compress column page: " + dataType);
    }
  }

  /**
   * Decompress data and create a column page using the decompressed data
   */
  public static ColumnPage decompress(Compressor compressor, DataType dataType,
      byte[] compressedData, int offset, int length, int scale, int precision)
      throws MemoryException {
    switch (dataType) {
      case BYTE:
        byte[] byteData = compressor.unCompressByte(compressedData, offset, length);
        return newBytePage(byteData);
      case SHORT:
        short[] shortData = compressor.unCompressShort(compressedData, offset, length);
        return newShortPage(shortData);
      case SHORT_INT:
        byte[] shortIntData = compressor.unCompressByte(compressedData, offset, length);
        return newShortIntPage(shortIntData);
      case INT:
        int[] intData = compressor.unCompressInt(compressedData, offset, length);
        return newIntPage(intData);
      case LONG:
        long[] longData = compressor.unCompressLong(compressedData, offset, length);
        return newLongPage(longData);
      case FLOAT:
        float[] floatData = compressor.unCompressFloat(compressedData, offset, length);
        return newFloatPage(floatData);
      case DOUBLE:
        double[] doubleData = compressor.unCompressDouble(compressedData, offset, length);
        return newDoublePage(doubleData);
      case DECIMAL:
        byte[] lvEncodedBytes = compressor.unCompressByte(compressedData, offset, length);
        return newDecimalPage(lvEncodedBytes, scale, precision);
      case BYTE_ARRAY:
        byte[] lvVarBytes = compressor.unCompressByte(compressedData, offset, length);
        return newVarLengthPage(lvVarBytes, scale, precision);
      default:
        throw new UnsupportedOperationException("unsupport uncompress column page: " + dataType);
    }
  }

}
