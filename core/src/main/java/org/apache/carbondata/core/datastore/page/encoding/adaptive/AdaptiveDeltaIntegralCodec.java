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

package org.apache.carbondata.core.datastore.page.encoding.adaptive;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPageValueConverter;
import org.apache.carbondata.core.datastore.page.DecimalColumnPage;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;

/**
 * Codec for integer (byte, short, int, long) data type and floating data type (in case of
 * scale is 0).
 * This codec will calculate delta of page max value and page value,
 * and do type casting of the diff to make storage minimum.
 */
public class AdaptiveDeltaIntegralCodec extends AdaptiveCodec {

  private long max;

  public AdaptiveDeltaIntegralCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats, boolean isInvertedIndex) {
    super(srcDataType, targetDataType, stats, isInvertedIndex);
    if (srcDataType == DataTypes.BYTE) {
      this.max = (byte) stats.getMax();
    } else if (srcDataType == DataTypes.SHORT) {
      this.max = (short) stats.getMax();
    } else if (srcDataType == DataTypes.INT) {
      this.max = (int) stats.getMax();
    } else if (srcDataType == DataTypes.LONG || srcDataType == DataTypes.TIMESTAMP) {
      this.max = (long) stats.getMax();
    } else if (srcDataType == DataTypes.DOUBLE) {
      this.max = (long) (double) stats.getMax();
    } else if (DataTypes.isDecimal(srcDataType)) {
      this.max = ((BigDecimal) stats.getMax()).unscaledValue().longValue();
    } else {
      // this codec is for integer type only
      throw new UnsupportedOperationException(
          "unsupported data type for Delta compress: " + srcDataType);
    }
  }

  @Override
  public String getName() {
    return "DeltaIntegralCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new ColumnPageEncoder() {
      byte[] result = null;
      @Override
      protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
        if (encodedPage != null) {
          throw new IllegalStateException("already encoded");
        }
        Compressor compressor =
            CompressorFactory.getInstance().getCompressor(input.getColumnCompressorName());
        result = encodeAndCompressPage(input, converter, compressor);
        byte[] bytes = writeInvertedIndexIfRequired(result);
        encodedPage.freeMemory();
        if (bytes.length != 0) {
          return bytes;
        }
        return result;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new ColumnPageEncoderMeta(inputPage.getColumnSpec(), targetDataType,
            inputPage.getStatistics(), inputPage.getColumnCompressorName());
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
        encodings.add(Encoding.ADAPTIVE_DELTA_INTEGRAL);
        if (null != indexStorage && indexStorage.getRowIdPageLengthInBytes() > 0) {
          encodings.add(Encoding.INVERTED_INDEX);
        }
        return encodings;
      }

      @Override
      protected void fillLegacyFields(DataChunk2 dataChunk) throws IOException {
        fillLegacyFieldsIfRequired(dataChunk, result);
      }

    };
  }

  @Override
  public ColumnPageDecoder createDecoder(final ColumnPageEncoderMeta meta) {
    return new ColumnPageDecoder() {
      @Override
      public ColumnPage decode(byte[] input, int offset, int length)
          throws MemoryException, IOException {
        ColumnPage page = null;
        if (DataTypes.isDecimal(meta.getSchemaDataType())) {
          page = ColumnPage.decompressDecimalPage(meta, input, offset, length);
        } else {
          page = ColumnPage.decompress(meta, input, offset, length, false);
        }
        return LazyColumnPage.newPage(page, converter);
      }

      @Override
      public void decodeAndFillVector(byte[] input, int offset, int length,
          ColumnVectorInfo vectorInfo, BitSet nullBits, boolean isLVEncoded)
          throws MemoryException, IOException {
        ColumnPage page = null;
        if (DataTypes.isDecimal(meta.getSchemaDataType())) {
          page = ColumnPage.decompressDecimalPage(meta, input, offset, length);
          vectorInfo.decimalConverter = ((DecimalColumnPage) page).getDecimalConverter();
        } else {
          page = ColumnPage.decompress(meta, input, offset, length, isLVEncoded);
        }
        page.setNullBits(nullBits);
        converter.decodeAndFillVector(page, vectorInfo);
      }

      @Override
      public ColumnPage decode(byte[] input, int offset, int length, boolean isLVEncoded)
          throws MemoryException, IOException {
        return decode(input, offset, length);
      }
    };
  }

  private ColumnPageValueConverter converter = new ColumnPageValueConverter() {
    @Override
    public void encode(int rowId, byte value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, short value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) (max - value));
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, int value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) (max - value));
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) (max - value));
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, long value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) (max - value));
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.LONG) {
        encodedPage.putLong(rowId, max - value);
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, float value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) (max - value));
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.LONG) {
        encodedPage.putLong(rowId, (long) (max - value));
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, double value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) (max - value));
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) (max - value));
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) (max - value));
      } else if (targetDataType == DataTypes.LONG) {
        encodedPage.putLong(rowId, (long) (max - value));
      } else {
        throw new RuntimeException("internal error");
      }
    }

    @Override
    public long decodeLong(byte value) {
      return max - value;
    }

    @Override
    public long decodeLong(short value) {
      return max - value;
    }

    @Override
    public long decodeLong(int value) {
      return max - value;
    }

    @Override
    public double decodeDouble(byte value) {
      return max - value;
    }

    @Override
    public double decodeDouble(short value) {
      return max - value;
    }

    @Override
    public double decodeDouble(int value) {
      return max - value;
    }

    @Override
    public double decodeDouble(long value) {
      return max - value;
    }

    @Override
    public double decodeDouble(float value) {
      // this codec is for integer type only
      throw new RuntimeException("internal error");
    }

    @Override
    public double decodeDouble(double value) {
      // this codec is for integer type only
      throw new RuntimeException("internal error");
    }

    @Override
    public void decodeAndFillVector(ColumnPage columnPage, ColumnVectorInfo vectorInfo) {
      CarbonColumnVector vector = vectorInfo.vector;
      BitSet nullBits = columnPage.getNullBits();
      DataType vectorDataType = vector.getType();
      DataType pageDataType = columnPage.getDataType();
      int pageSize = columnPage.getPageSize();
      BitSet deletedRows = vectorInfo.deletedRows;
      fillVector(columnPage, vector, vectorDataType, pageDataType, pageSize, vectorInfo);
      if (deletedRows == null || deletedRows.isEmpty()) {
        for (int i = nullBits.nextSetBit(0); i >= 0; i = nullBits.nextSetBit(i + 1)) {
          vector.putNull(i);
        }
      }
    }

    private void fillVector(ColumnPage columnPage, CarbonColumnVector vector,
        DataType vectorDataType, DataType pageDataType, int pageSize, ColumnVectorInfo vectorInfo) {
      if (pageDataType == DataTypes.BOOLEAN || pageDataType == DataTypes.BYTE) {
        byte[] byteData = columnPage.getBytePage();
        if (vectorDataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(i, (short) (max - byteData[i]));
          }
        } else if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, (int) (max - byteData[i]));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (max - byteData[i]));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (max - byteData[i]) * 1000);
          }
        } else if (vectorDataType == DataTypes.BOOLEAN) {
          for (int i = 0; i < pageSize; i++) {
            vector.putByte(i, (byte) (max - byteData[i]));
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int precision = vectorInfo.measure.getMeasure().getPrecision();
          for (int i = 0; i < pageSize; i++) {
            BigDecimal decimal = decimalConverter.getDecimal(max - byteData[i]);
            vector.putDecimal(i, decimal, precision);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, (max - byteData[i]));
          }
        }
      } else if (pageDataType == DataTypes.SHORT) {
        short[] shortData = columnPage.getShortPage();
        if (vectorDataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(i, (short) (max - shortData[i]));
          }
        } else if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, (int) (max - shortData[i]));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (max - shortData[i]));
          }
        }  else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (max - shortData[i]) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int precision = vectorInfo.measure.getMeasure().getPrecision();
          for (int i = 0; i < pageSize; i++) {
            BigDecimal decimal = decimalConverter.getDecimal(max - shortData[i]);
            vector.putDecimal(i, decimal, precision);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, (max - shortData[i]));
          }
        }

      } else if (pageDataType == DataTypes.SHORT_INT) {
        byte[] shortIntPage = columnPage.getShortIntPage();
        if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            vector.putInt(i, (int) (max - shortInt));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            vector.putLong(i, (max - shortInt));
          }
        }  else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            vector.putLong(i, (max - shortInt) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int precision = vectorInfo.measure.getMeasure().getPrecision();
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            BigDecimal decimal = decimalConverter.getDecimal(max - shortInt);
            vector.putDecimal(i, decimal, precision);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            vector.putDouble(i, (max - shortInt));
          }
        }
      } else if (pageDataType == DataTypes.INT) {
        int[] intData = columnPage.getIntPage();
        if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, (int) (max - intData[i]));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (max - intData[i]));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (max - intData[i]) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int precision = vectorInfo.measure.getMeasure().getPrecision();
          for (int i = 0; i < pageSize; i++) {
            BigDecimal decimal = decimalConverter.getDecimal(max - intData[i]);
            vector.putDecimal(i, decimal, precision);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, (max - intData[i]));
          }
        }
      } else if (pageDataType == DataTypes.LONG) {
        long[] longData = columnPage.getLongPage();
        if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (max - longData[i]));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (max - longData[i]) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int precision = vectorInfo.measure.getMeasure().getPrecision();
          for (int i = 0; i < pageSize; i++) {
            BigDecimal decimal = decimalConverter.getDecimal(max - longData[i]);
            vector.putDecimal(i, decimal, precision);
          }
        }
      } else {
        throw new RuntimeException("Unsupported datatype : " + pageDataType);
      }
    }

  };
}
