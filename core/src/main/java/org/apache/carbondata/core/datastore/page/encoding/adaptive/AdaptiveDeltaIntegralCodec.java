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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.ReusableDataBuffer;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPageValueConverter;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ColumnarVectorWrapperDirectFactory;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ConvertibleVector;
import org.apache.carbondata.core.scan.result.vector.impl.directread.SequentialFill;
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
      ByteBuffer result = null;
      @Override
      protected ByteBuffer encodeData(ColumnPage input) throws IOException {
        if (encodedPage != null) {
          throw new IllegalStateException("already encoded");
        }
        Compressor compressor =
            CompressorFactory.getInstance().getCompressor(input.getColumnCompressorName());
        result = encodeAndCompressPage(input, converter, compressor);
        ByteBuffer bytes = writeInvertedIndexIfRequired(result);
        encodedPage.freeMemory();
        if (bytes.limit() != 0) {
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
      protected void fillLegacyFields(DataChunk2 dataChunk) {
        fillLegacyFieldsIfRequired(dataChunk, result);
      }

    };
  }

  @Override
  public ColumnPageDecoder createDecoder(final ColumnPageEncoderMeta meta) {
    return new ColumnPageDecoder() {
      @Override
      public ColumnPage decode(byte[] input, int offset, int length) {
        ColumnPage page = null;
        if (DataTypes.isDecimal(meta.getSchemaDataType())) {
          page = ColumnPage.decompressDecimalPage(meta, input, offset, length);
        } else {
          page = ColumnPage.decompress(meta, input, offset, length, false, false);
        }
        return LazyColumnPage.newPage(page, converter);
      }

      @Override
      public void decodeAndFillVector(byte[] input, int offset, int length,
          ColumnVectorInfo vectorInfo, BitSet nullBits, boolean isLVEncoded, int pageSize,
          ReusableDataBuffer reusableDataBuffer) {
        Compressor compressor =
            CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
        byte[] unCompressData;
        if (null != reusableDataBuffer && compressor.supportReusableBuffer()) {
          int uncompressedLength = compressor.unCompressedLength(input, offset, length);
          unCompressData = reusableDataBuffer.getDataBuffer(uncompressedLength);
          compressor.rawUncompress(input, offset, length, unCompressData);
        } else {
          unCompressData = compressor.unCompressByte(input, offset, length);
        }
        if (DataTypes.isDecimal(meta.getSchemaDataType())) {
          TableSpec.ColumnSpec columnSpec = meta.getColumnSpec();
          vectorInfo.decimalConverter = DecimalConverterFactory.INSTANCE
              .getDecimalConverter(columnSpec.getPrecision(), columnSpec.getScale());
        }
        converter.decodeAndFillVector(unCompressData, vectorInfo, nullBits, meta.getStoreDataType(),
            pageSize);
      }

      @Override
      public ColumnPage decode(byte[] input, int offset, int length, boolean isLVEncoded) {
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
    public void decodeAndFillVector(byte[] pageData, ColumnVectorInfo vectorInfo, BitSet nullBits,
        DataType pageDataType, int pageSize) {
      CarbonColumnVector vector = vectorInfo.vector;
      BitSet deletedRows = vectorInfo.deletedRows;
      fillVector(pageData, vector, pageDataType, pageSize, vectorInfo, nullBits);
      if ((deletedRows == null || deletedRows.isEmpty())
          && !(vectorInfo.vector instanceof SequentialFill)) {
        for (int i = nullBits.nextSetBit(0); i >= 0; i = nullBits.nextSetBit(i + 1)) {
          vector.putNull(i);
        }
      }
      if (vector instanceof ConvertibleVector) {
        ((ConvertibleVector) vector).convert();
      }
    }

    private void fillVector(byte[] pageData, CarbonColumnVector vector, DataType pageDataType,
        int pageSize, ColumnVectorInfo vectorInfo, BitSet nullBits) {
      // get the updated values if it is decode of child vector
      pageSize = ColumnVectorInfo.getUpdatedPageSizeForChildVector(vectorInfo, pageSize);
      vector = ColumnarVectorWrapperDirectFactory
          .getDirectVectorWrapperFactory(vectorInfo, vector, null, nullBits, vectorInfo.deletedRows,
              true, false);
      DataType vectorDataType = vector.getType();
      int newScale = 0;
      if (vectorInfo.measure != null) {
        newScale = vectorInfo.measure.getMeasure().getScale();
      }
      int rowId = 0;
      if (pageDataType == DataTypes.BOOLEAN || pageDataType == DataTypes.BYTE) {
        if (vectorDataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(i, (short) (max - pageData[i]));
          }
        } else if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, (int) (max - pageData[i]));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (max - pageData[i]));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (max - (long) pageData[i]) * 1000);
          }
        } else if (vectorDataType == DataTypes.BOOLEAN || vectorDataType == DataTypes.BYTE) {
          for (int i = 0; i < pageSize; i++) {
            vector.putByte(i, (byte) (max - pageData[i]));
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int precision = vectorInfo.measure.getMeasure().getPrecision();
          for (int i = 0; i < pageSize; i++) {
            BigDecimal decimal = decimalConverter.getDecimal(max - pageData[i]);
            if (decimal.scale() < newScale) {
              decimal = decimal.setScale(newScale);
            }
            vector.putDecimal(i, decimal, precision);
          }
        } else if (vectorDataType == DataTypes.FLOAT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putFloat(i, (int) (max - pageData[i]));
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, (max - pageData[i]));
          }
        }
      } else if (pageDataType == DataTypes.SHORT) {
        int shortSizeInBytes = DataTypes.SHORT.getSizeInBytes();
        int size = pageSize * shortSizeInBytes;
        if (vectorDataType == DataTypes.SHORT) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putShort(rowId++, (short) (max - ByteUtil.toShortLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putInt(rowId++, (int) (max - ByteUtil.toShortLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putLong(rowId++, (max - ByteUtil.toShortLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector
                .putLong(rowId++, (max - (long) ByteUtil.toShortLittleEndian(pageData, i)) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int precision = vectorInfo.measure.getMeasure().getPrecision();
          for (int i = 0; i < size; i += shortSizeInBytes) {
            BigDecimal decimal =
                decimalConverter.getDecimal(max - ByteUtil.toShortLittleEndian(pageData, i));
            if (decimal.scale() < newScale) {
              decimal = decimal.setScale(newScale);
            }
            vector.putDecimal(rowId++, decimal, precision);
          }
        } else if (vectorDataType == DataTypes.FLOAT) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putFloat(rowId++, (int) (max - ByteUtil.toShortLittleEndian(pageData, i)));
          }
        } else {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putDouble(rowId++, (max - ByteUtil.toShortLittleEndian(pageData, i)));
          }
        }
      } else if (pageDataType == DataTypes.SHORT_INT) {
        int shortIntSizeInBytes = DataTypes.SHORT_INT.getSizeInBytes();
        int size = pageSize * shortIntSizeInBytes;
        if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < size; i += shortIntSizeInBytes) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i);
            vector.putInt(rowId++, (int) (max - shortInt));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < size; i += shortIntSizeInBytes) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i);
            vector.putLong(rowId++, (max - shortInt));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < size; i += shortIntSizeInBytes) {
            vector.putLong(rowId++, (max - (long) ByteUtil.valueOf3Bytes(pageData, i)) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int precision = vectorInfo.measure.getMeasure().getPrecision();
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            BigDecimal decimal = decimalConverter.getDecimal(max - shortInt);
            if (decimal.scale() < newScale) {
              decimal = decimal.setScale(newScale);
            }
            vector.putDecimal(i, decimal, precision);
          }
        } else if (vectorDataType == DataTypes.FLOAT) {
          for (int i = 0; i < size; i += shortIntSizeInBytes) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i);
            vector.putFloat(rowId++, (int) (max - shortInt));
          }
        } else {
          for (int i = 0; i < size; i += shortIntSizeInBytes) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i);
            vector.putDouble(rowId++, (max - shortInt));
          }
        }
      } else if (pageDataType == DataTypes.INT) {
        int intSizeInBytes = DataTypes.INT.getSizeInBytes();
        int size = pageSize * intSizeInBytes;
        if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < size; i += intSizeInBytes) {
            vector.putInt(rowId++, (int) (max - ByteUtil.toIntLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < size; i += intSizeInBytes) {
            vector.putLong(rowId++, (max - ByteUtil.toIntLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < size; i += intSizeInBytes) {
            vector.putLong(rowId++, (max - (long) ByteUtil.toIntLittleEndian(pageData, i)) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int precision = vectorInfo.measure.getMeasure().getPrecision();
          for (int i = 0; i < size; i += intSizeInBytes) {
            BigDecimal decimal =
                decimalConverter.getDecimal(max - ByteUtil.toIntLittleEndian(pageData, i));
            if (decimal.scale() < newScale) {
              decimal = decimal.setScale(newScale);
            }
            vector.putDecimal(rowId++, decimal, precision);
          }
        } else {
          for (int i = 0; i < size; i += intSizeInBytes) {
            vector.putDouble(rowId++, (max - ByteUtil.toIntLittleEndian(pageData, i)));
          }
        }
      } else if (pageDataType == DataTypes.LONG) {
        int longSizeInBytes = DataTypes.LONG.getSizeInBytes();
        int size = pageSize * longSizeInBytes;
        if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < size; i += longSizeInBytes) {
            vector.putLong(rowId++, (max - ByteUtil.toLongLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < size; i += longSizeInBytes) {
            vector.putLong(rowId++, (max - ByteUtil.toLongLittleEndian(pageData, i)) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int precision = vectorInfo.measure.getMeasure().getPrecision();
          for (int i = 0; i < size; i += longSizeInBytes) {
            BigDecimal decimal =
                decimalConverter.getDecimal(max - ByteUtil.toLongLittleEndian(pageData, i));
            if (decimal.scale() < newScale) {
              decimal = decimal.setScale(newScale);
            }
            vector.putDecimal(rowId++, decimal, precision);
          }
        }
      } else {
        throw new RuntimeException("Unsupported datatype : " + pageDataType);
      }
    }

  };
}
