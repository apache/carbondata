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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsVO;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.DataTypeUtil;

import static org.apache.carbondata.core.metadata.datatype.DataType.BYTE;
import static org.apache.carbondata.core.metadata.datatype.DataType.BYTE_ARRAY;
import static org.apache.carbondata.core.metadata.datatype.DataType.DECIMAL;
import static org.apache.carbondata.core.metadata.datatype.DataType.DOUBLE;
import static org.apache.carbondata.core.metadata.datatype.DataType.FLOAT;
import static org.apache.carbondata.core.metadata.datatype.DataType.INT;
import static org.apache.carbondata.core.metadata.datatype.DataType.LONG;
import static org.apache.carbondata.core.metadata.datatype.DataType.SHORT;

/**
 * Represent a columnar data in one page for one column.
 */
public class ColumnPage {

  private final int pageSize;
  private DataType dataType;
  private ColumnPageStatsVO stats;

  // Only one of following fields will be used
  private byte[] byteData;
  private short[] shortData;
  private int[] intData;
  private long[] longData;
  private float[] floatData;
  private double[] doubleData;

  // for string and decimal data
  private byte[][] byteArrayData;

  // The index of the rowId whose value is null, will be set to 1
  private BitSet nullBitSet;

  protected ColumnPage(DataType dataType, int pageSize) {
    this.pageSize = pageSize;
    this.dataType = dataType;
  }

  // create a new page
  public static ColumnPage newPage(DataType dataType, int pageSize) {
    ColumnPage instance;
    switch (dataType) {
      case BYTE:
        instance = newBytePage(new byte[pageSize]);
        break;
      case SHORT:
        instance = newShortPage(new short[pageSize]);
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
        instance = newDecimalPage(new byte[pageSize][]);
        break;
      case BYTE_ARRAY:
        instance = newVarLengthPage(new byte[pageSize][]);
        break;
      default:
        throw new RuntimeException("Unsupported data dataType: " + dataType);
    }
    instance.stats = new ColumnPageStatsVO(dataType);
    instance.nullBitSet = new BitSet(pageSize);
    return instance;
  }

  protected static ColumnPage newBytePage(byte[] byteData) {
    ColumnPage columnPage = new ColumnPage(BYTE, byteData.length);
    columnPage.byteData = byteData;
    return columnPage;
  }

  protected static ColumnPage newShortPage(short[] shortData) {
    ColumnPage columnPage = new ColumnPage(SHORT, shortData.length);
    columnPage.shortData = shortData;
    return columnPage;
  }

  protected static ColumnPage newIntPage(int[] intData) {
    ColumnPage columnPage = new ColumnPage(INT, intData.length);
    columnPage.intData = intData;
    return columnPage;
  }

  protected static ColumnPage newLongPage(long[] longData) {
    ColumnPage columnPage = new ColumnPage(LONG, longData.length);
    columnPage.longData = longData;
    return columnPage;
  }

  protected static ColumnPage newFloatPage(float[] floatData) {
    ColumnPage columnPage = new ColumnPage(FLOAT, floatData.length);
    columnPage.floatData = floatData;
    return columnPage;
  }

  protected static ColumnPage newDoublePage(double[] doubleData) {
    ColumnPage columnPage = new ColumnPage(DOUBLE, doubleData.length);
    columnPage.doubleData = doubleData;
    return columnPage;
  }

  protected static ColumnPage newDecimalPage(byte[][] decimalData) {
    ColumnPage columnPage = new ColumnPage(DECIMAL, decimalData.length);
    columnPage.byteArrayData = decimalData;
    return columnPage;
  }

  protected void updateStatisticsLong(long value) {
    stats.updateLong(value);
  }

  protected void updateStatisticsDouble(double value) {
    stats.updateDouble(value);
  }

  protected void updateStatisticsDecimal(byte[] value) {
    stats.updateDecimal(value);
  }

  protected static ColumnPage newVarLengthPage(byte[][] stringData) {
    ColumnPage columnPage = new ColumnPage(BYTE_ARRAY, stringData.length);
    columnPage.byteArrayData = stringData;
    return columnPage;
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
        stats.updateLong((byte) value);
        break;
      case SHORT:
        putShort(rowId, (short) value);
        stats.updateLong((short) value);
        break;
      case INT:
        putInt(rowId, (int) value);
        stats.updateLong((int) value);
        break;
      case LONG:
        putLong(rowId, (long) value);
        stats.updateLong((long) value);
        break;
      case DOUBLE:
        putDouble(rowId, (double) value);
        stats.updateDouble((double) value);
        break;
      case DECIMAL:
        putDecimalBytes(rowId, (byte[]) value);
        stats.updateDecimal((byte[]) value);
        break;
      case BYTE_ARRAY:
        putBytes(rowId, (byte[]) value);
        break;
      default:
        throw new RuntimeException("unsupported data type: " + dataType);
    }
  }

  /**
   * Set byte value at rowId
   */
  public void putByte(int rowId, byte value) {
    byteData[rowId] = value;
  }

  /**
   * Set short value at rowId
   */
  public void putShort(int rowId, short value) {
    shortData[rowId] = value;
  }

  /**
   * Set integer value at rowId
   */
  public void putInt(int rowId, int value) {
    intData[rowId] = value;
  }

  /**
   * Set long value at rowId
   */
  public void putLong(int rowId, long value) {
    longData[rowId] = value;
  }

  /**
   * Set double value at rowId
   */
  public void putDouble(int rowId, double value) {
    doubleData[rowId] = value;
  }

  /**
   * Set decimal value at rowId
   */
  public void putDecimalBytes(int rowId, byte[] decimalInBytes) {
    // do LV (length value) coded of input bytes
    ByteBuffer byteBuffer = ByteBuffer.allocate(decimalInBytes.length +
        CarbonCommonConstants.INT_SIZE_IN_BYTE);
    byteBuffer.putInt(decimalInBytes.length);
    byteBuffer.put(decimalInBytes);
    byteBuffer.flip();
    byteArrayData[rowId] = byteBuffer.array();
  }

  /**
   * Set string value at rowId
   */
  public void putBytes(int rowId, byte[] bytes) {
    byteArrayData[rowId] = bytes;
  }

  /**
   * Set null at rowId
   */
  public void putNull(int rowId) {
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
        byte[] decimalInBytes = DataTypeUtil.bigDecimalToByte(BigDecimal.ZERO);
        putDecimalBytes(rowId, decimalInBytes);
        break;
    }
  }

  /**
   * Get byte value at rowId
   */
  public byte getByte(int rowId) {
    return byteData[rowId];
  }

  /**
   * Get short value at rowId
   */
  public short getShort(int rowId) {
    return shortData[rowId];
  }

  /**
   * Get int value at rowId
   */
  public int getInt(int rowId) {
    return intData[rowId];
  }

  /**
   * Get long value at rowId
   */
  public long getLong(int rowId) {
    return longData[rowId];
  }

  /**
   * Get float value at rowId
   */
  public float getFloat(int rowId) {
    return floatData[rowId];
  }

  /**
   * Get double value at rowId
   */
  public double getDouble(int rowId) {
    return doubleData[rowId];
  }

  /**
   * Get decimal value at rowId
   */
  public byte[] getDecimalBytes(int rowId) {
    return byteArrayData[rowId];
  }

  public BigDecimal getDecimal(int rowId) {
    byte[] bytes = getDecimalBytes(rowId);
    return DataTypeUtil.byteToBigDecimal(bytes);
  }

  /**
   * Get byte value page
   */
  public byte[] getBytePage() {
    return byteData;
  }

  /**
   * Get short value page
   */
  public short[] getShortPage() {
    return shortData;
  }

  /**
   * Get int value page
   */
  public int[] getIntPage() {
    return intData;
  }

  /**
   * Get long value page
   */
  public long[] getLongPage() {
    return longData;
  }

  /**
   * Get float value page
   */
  public float[] getFloatPage() {
    return floatData;
  }

  /**
   * Get double value page
   */
  public double[] getDoublePage() {
    return doubleData;
  }

  /**
   * Get decimal value page
   */
  public byte[][] getDecimalPage() {
    return byteArrayData;
  }

  /**
   * Get string page
   */
  public byte[][] getByteArrayPage() {
    return byteArrayData;
  }

  /**
   * Get null bitset page
   */
  public BitSet getNullBitSet() {
    return nullBitSet;
  }

  public void freeMemory() {
  }

  /**
   * apply encoding to page data
   * @param codec type of transformation
   */
  public void encode(PrimitiveCodec codec) {
    switch (dataType) {
      case BYTE:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, byteData[i]);
        }
        break;
      case SHORT:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, shortData[i]);
        }
        break;
      case INT:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, intData[i]);
        }
        break;
      case LONG:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, longData[i]);
        }
        break;
      case FLOAT:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, floatData[i]);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, doubleData[i]);
        }
        break;
      default:
        throw new UnsupportedOperationException("not support encode on " + dataType + " page");
    }
  }

  /**
   * compress page data using specified compressor
   */
  public byte[] compress(Compressor compressor) {
    switch (dataType) {
      case BYTE:
        return compressor.compressByte(getBytePage());
      case SHORT:
        return compressor.compressShort(getShortPage());
      case INT:
        return compressor.compressInt(getIntPage());
      case LONG:
        return compressor.compressLong(getLongPage());
      case FLOAT:
        return compressor.compressFloat(getFloatPage());
      case DOUBLE:
        return compressor.compressDouble(getDoublePage());
      case DECIMAL:
        byte[] flattenedDecimal = ByteUtil.flatten(getDecimalPage());
        return compressor.compressByte(flattenedDecimal);
      case BYTE_ARRAY:
        byte[] flattenedString = ByteUtil.flatten(getByteArrayPage());
        return compressor.compressByte(flattenedString);
      default:
        throw new UnsupportedOperationException("unsupport compress column page: " + dataType);
    }
  }

  /**
   * decompress data and create a column page using the decompressed data
   */
  public static ColumnPage decompress(Compressor compressor, DataType dataType,
      byte[] compressedData, int offset, int length) {
    switch (dataType) {
      case BYTE:
        byte[] byteData = compressor.unCompressByte(compressedData, offset, length);
        return newBytePage(byteData);
      case SHORT:
        short[] shortData = compressor.unCompressShort(compressedData, offset, length);
        return newShortPage(shortData);
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
        byte[] decompressed = compressor.unCompressByte(compressedData, offset, length);
        byte[][] decimal = deflatten(decompressed);
        return newDecimalPage(decimal);
      case BYTE_ARRAY:
        decompressed = compressor.unCompressByte(compressedData, offset, length);
        byte[][] string = deflatten(decompressed);
        return newVarLengthPage(string);
      default:
        throw new UnsupportedOperationException("unsupport uncompress column page: " + dataType);
    }
  }

  // input byte[] is LV encoded, this function can expand it into byte[][]
  private static byte[][] deflatten(byte[] input) {
    int pageSize = Integer.valueOf(
        CarbonProperties.getInstance().getProperty(
            CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE,
            CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT));
    int numRows = 0;
    // offset of value of each row in input data
    int[] offsetOfRow = new int[pageSize];
    ByteBuffer buffer = ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE);
    for (int currentLength = 0; currentLength < input.length;) {
      buffer.put(input, currentLength, CarbonCommonConstants.INT_SIZE_IN_BYTE);
      buffer.flip();
      int valueLength = buffer.getInt();
      offsetOfRow[numRows] = currentLength + CarbonCommonConstants.INT_SIZE_IN_BYTE;
      currentLength += CarbonCommonConstants.INT_SIZE_IN_BYTE + valueLength;
      buffer.clear();
      numRows++;
    }
    byte[][] byteArrayData = new byte[numRows][];
    for (int rowId = 0; rowId < numRows; rowId++) {
      int valueOffset = offsetOfRow[rowId];
      int valueLength;
      if (rowId != numRows - 1) {
        valueLength = offsetOfRow[rowId + 1] - valueOffset - CarbonCommonConstants.INT_SIZE_IN_BYTE;
      } else {
        // last row
        buffer.put(input, offsetOfRow[rowId] - CarbonCommonConstants.INT_SIZE_IN_BYTE,
            CarbonCommonConstants.INT_SIZE_IN_BYTE);
        buffer.flip();
        valueLength = buffer.getInt();
      }
      byte[] value = new byte[valueLength];
      System.arraycopy(input, valueOffset, value, 0, valueLength);
      byteArrayData[rowId] = value;
    }
    return byteArrayData;
  }
}
