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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ReusableDataBuffer;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.columnar.UnBlockIndexer;
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
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ColumnarVectorWrapperDirectFactory;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ConvertibleVector;
import org.apache.carbondata.core.scan.result.vector.impl.directread.SequentialFill;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.Encoding;

/**
 * Codec for integer (byte, short, int, long) data type page.
 * This converter will do type casting on page data to make storage minimum.
 */
public class AdaptiveIntegralCodec extends AdaptiveCodec {

  private ColumnPage encodedPage;

  private CarbonDictionary dictionary = null;

  public AdaptiveIntegralCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats, CarbonDictionary dictionary) {
    super(srcDataType, targetDataType, stats);
    this.dictionary = dictionary;
  }

  public AdaptiveIntegralCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats) {
    super(srcDataType, targetDataType, stats);
  }

  @Override
  public String getName() {
    return "AdaptiveIntegralCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, Object> parameter) {
    return new ColumnPageEncoder() {
      @Override
      protected ByteBuffer encodeData(ColumnPage input) throws IOException {
        if (encodedPage != null) {
          throw new IllegalStateException("already encoded");
        }
        encodedPage = ColumnPage.newPage(
            new ColumnPageEncoderMeta(input.getColumnPageEncoderMeta().getColumnSpec(),
                targetDataType, input.getColumnPageEncoderMeta().getCompressorName()),
            input.getPageSize());
        Compressor compressor = CompressorFactory.getInstance().getCompressor(
            input.getColumnCompressorName());
        input.convertValue(converter);
        ByteBuffer result = encodedPage.compress(compressor);
        encodedPage.freeMemory();
        return result;
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<Encoding>();
        encodings.add(Encoding.ADAPTIVE_INTEGRAL);
        return encodings;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new ColumnPageEncoderMeta(inputPage.getColumnSpec(), targetDataType, stats,
            inputPage.getColumnCompressorName());
      }

    };
  }

  @Override
  public ColumnPageDecoder createDecoder(final ColumnPageEncoderMeta meta) {
    return new ColumnPageDecoder() {
      @Override
      public ColumnPage decode(byte[] input, int offset, int length, boolean isRLEEncoded,
          int rlePageLength) {
        ColumnPage page;
        if (DataTypes.isDecimal(meta.getSchemaDataType())) {
          page = ColumnPage.decompressDecimalPage(meta, input, offset, length);
        } else {
          page = ColumnPage
              .decompress(meta, input, offset, length, false, false, isRLEEncoded, rlePageLength);
        }
        return LazyColumnPage.newPage(page, converter);
      }

      @Override
      public void decodeAndFillVector(byte[] input, int offset, int length,
          ColumnVectorInfo vectorInfo, BitSet nullBits, boolean isLVEncoded, int pageSize,
          ReusableDataBuffer reusableDataBuffer, boolean isRLEEncoded, int rlePageLength) {
        Compressor compressor =
            CompressorFactory.getInstance().getCompressor(meta.getCompressorName());
        byte[] unCompressData;
        int uncompressedLength;
        if (null != reusableDataBuffer && compressor.supportReusableBuffer()) {
          uncompressedLength = compressor.unCompressedLength(input, offset, length);
          unCompressData = reusableDataBuffer.getDataBuffer(uncompressedLength);
          compressor.rawUncompress(input, offset, length, unCompressData);
        } else {
          unCompressData = compressor.unCompressByte(input, offset, length);
          uncompressedLength = unCompressData.length;
        }
        offset += length;
        int[] rlePage;
        // if rle is applied then read the rle block chunk and then uncompress
        //then actual data based on rle block
        if (isRLEEncoded) {
          rlePage = CarbonUtil.getIntArray(ByteBuffer.wrap(input), offset, rlePageLength);
          // uncompress the data with rle indexes
          unCompressData = UnBlockIndexer
              .uncompressData(unCompressData, rlePage, meta.getStoreDataType().getSizeInBytes(),
                  uncompressedLength);
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
      public ColumnPage decode(byte[] input, int offset, int length, boolean isLVEncoded,
          boolean isRLEEncoded, int rlePageLength) {
        return decode(input, offset, length, isRLEEncoded, rlePageLength);
      }
    };
  }

  // encoded value = (type cast page value to target data type)
  private ColumnPageValueConverter converter = new ColumnPageValueConverter() {
    @Override
    public void encode(int rowId, byte value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, short value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) value);
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, int value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) value);
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) value);
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, value);
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, long value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) value);
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) value);
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) value);
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) value);
      } else if (targetDataType == DataTypes.LONG) {
        encodedPage.putLong(rowId, value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, float value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) value);
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) value);
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) value);
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, double value) {
      if (targetDataType == DataTypes.BYTE) {
        encodedPage.putByte(rowId, (byte) value);
      } else if (targetDataType == DataTypes.SHORT) {
        encodedPage.putShort(rowId, (short) value);
      } else if (targetDataType == DataTypes.SHORT_INT) {
        encodedPage.putShortInt(rowId, (int) value);
      } else if (targetDataType == DataTypes.INT) {
        encodedPage.putInt(rowId, (int) value);
      } else if (targetDataType == DataTypes.LONG) {
        encodedPage.putLong(rowId, (long) value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public long decodeLong(byte value) {
      return value;
    }

    @Override
    public long decodeLong(short value) {
      return value;
    }

    @Override
    public long decodeLong(int value) {
      return value;
    }

    @Override
    public double decodeDouble(byte value) {
      return value;
    }

    @Override
    public double decodeDouble(short value) {
      return value;
    }

    @Override
    public double decodeDouble(int value) {
      return value;
    }

    @Override
    public double decodeDouble(long value) {
      return value;
    }

    @Override
    public double decodeDouble(float value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public double decodeDouble(double value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public void decodeAndFillVector(byte[] pageData, ColumnVectorInfo vectorInfo, BitSet nullBits,
        DataType pageDataType, int pageSize) {
      CarbonColumnVector vector = vectorInfo.vector;
      BitSet deletedRows = vectorInfo.deletedRows;
      BitSet nullBitset = new BitSet();
      if (null != dictionary && !dictionary.isDictionaryUsed()) {
        vector.setDictionary(dictionary);
        dictionary.setDictionaryUsed();
      }
      CarbonColumnVector dictionaryVector = ColumnarVectorWrapperDirectFactory
          .getDirectVectorWrapperFactory(vectorInfo, vector.getDictionaryVector(),
              vectorInfo.invertedIndex, nullBitset, vectorInfo.deletedRows, false, true);
      vector = ColumnarVectorWrapperDirectFactory
          .getDirectVectorWrapperFactory(vectorInfo, vector, null, nullBits,
              deletedRows, true, false);
      fillVector(pageData, vector, pageDataType, pageSize, vectorInfo, nullBits,
          dictionaryVector);
      if ((deletedRows == null || deletedRows.isEmpty())
          && !(vectorInfo.vector instanceof SequentialFill)) {
        for (int i = nullBits.nextSetBit(0); i >= 0; i = nullBits.nextSetBit(i + 1)) {
          vector.putNull(i);
        }
      }
      if (dictionaryVector instanceof ConvertibleVector) {
        ((ConvertibleVector) dictionaryVector).convert();
      }
      if (vector instanceof ConvertibleVector) {
        ((ConvertibleVector) vector).convert();
      }

    }

    private void fillVector(byte[] pageData, CarbonColumnVector vector, DataType pageDataType,
        int pageSize, ColumnVectorInfo vectorInfo, BitSet nullBits,
        CarbonColumnVector dictionaryVector) {
      // get the updated values if it is decode of child vector
      pageSize = ColumnVectorInfo.getUpdatedPageSizeForChildVector(vectorInfo, pageSize);
      DataType vectorDataType = vector.getType();
      int rowId = 0;
      if (pageDataType == DataTypes.BOOLEAN || pageDataType == DataTypes.BYTE) {
        if (vectorDataType == DataTypes.SHORT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putShort(i, pageData[i]);
          }
        } else if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, pageData[i]);
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, pageData[i]);
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, (long) pageData[i] * 1000);
          }
        } else if (vectorDataType == DataTypes.BOOLEAN || vectorDataType == DataTypes.BYTE) {
          vector.putBytes(0, pageSize, pageData, 0);
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
        } else if (vectorDataType == DataTypes.FLOAT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putFloat(i, pageData[i]);
          }
        } else if (vectorDataType == DataTypes.DOUBLE) {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, pageData[i]);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            if (pageData[i] == CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY) {
              dictionaryVector.putNull(i);
            } else {
              dictionaryVector.putInt(i, (int) pageData[i]);
            }
          }
        }
      } else if (pageDataType == DataTypes.SHORT) {
        int shortSizeInBytes = DataTypes.SHORT.getSizeInBytes();
        int size = pageSize * shortSizeInBytes;
        if (vectorDataType == DataTypes.SHORT) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putShort(rowId++, (ByteUtil.toShortLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putInt(rowId++, (ByteUtil.toShortLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putLong(rowId++, (ByteUtil.toShortLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putLong(rowId++, ((long) ByteUtil.toShortLittleEndian(pageData, i)) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
        } else if (vectorDataType == DataTypes.FLOAT) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putFloat(rowId++, (ByteUtil.toShortLittleEndian(pageData, i)));
          }
        } else if (vectorDataType == DataTypes.DOUBLE) {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            vector.putDouble(rowId++, ByteUtil.toShortLittleEndian(pageData, i));
          }
        } else {
          for (int i = 0; i < size; i += shortSizeInBytes) {
            short data = ByteUtil.toShortLittleEndian(pageData, i);
            if (data == CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY) {
              dictionaryVector.putNull(i);
            } else {
              dictionaryVector.putInt(rowId++, data);
            }
          }
        }

      } else if (pageDataType == DataTypes.SHORT_INT) {
        if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            vector.putInt(i, shortInt);
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            vector.putLong(i, shortInt);
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            vector.putLong(i, (long) shortInt * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter
              .fillVector(pageData, pageSize, vectorInfo, nullBits, DataTypes.SHORT_INT);
        } else if (vectorDataType == DataTypes.FLOAT) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            vector.putFloat(i, shortInt);
          }
        } else if (vectorDataType == DataTypes.DOUBLE) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            vector.putDouble(i, shortInt);
          }
        } else {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(pageData, i * 3);
            if (shortInt == CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY) {
              dictionaryVector.putNull(i);
            } else {
              dictionaryVector.putInt(i, shortInt);
            }
          }
        }
      } else if (pageDataType == DataTypes.INT) {
        int intSizeInBytes = DataTypes.INT.getSizeInBytes();
        int size = pageSize * intSizeInBytes;
        if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < size; i += intSizeInBytes) {
            vector.putInt(rowId++, ByteUtil.toIntLittleEndian(pageData, i));
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < size; i += intSizeInBytes) {
            vector.putLong(rowId++, ByteUtil.toIntLittleEndian(pageData, i));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < size; i += intSizeInBytes) {
            vector.putLong(rowId++, (long) ByteUtil.toIntLittleEndian(pageData, i) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
        } else if (vectorDataType == DataTypes.DOUBLE) {
          for (int i = 0; i < size; i += intSizeInBytes) {
            vector.putDouble(rowId++, ByteUtil.toIntLittleEndian(pageData, i));
          }
        } else {
          for (int i = 0; i < size; i += DataTypes.INT.getSizeInBytes()) {
            int data = ByteUtil.toIntLittleEndian(pageData, i);
            if (data == CarbonCommonConstants.MEMBER_DEFAULT_VAL_SURROGATE_KEY) {
              dictionaryVector.putNull(i);
            } else {
              dictionaryVector.putInt(rowId++, data);
            }
          }
        }
      } else if (pageDataType == DataTypes.LONG) {
        int longSizeInBytes = DataTypes.LONG.getSizeInBytes();
        int size = pageSize * longSizeInBytes;
        if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < size; i += longSizeInBytes) {
            vector.putLong(rowId++, ByteUtil.toLongLittleEndian(pageData, i));
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < size; i += longSizeInBytes) {
            vector.putLong(rowId++, ByteUtil.toLongLittleEndian(pageData, i) * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(pageData, pageSize, vectorInfo, nullBits, pageDataType);
        }
      } else {
        int doubleSizeInBytes = DataTypes.DOUBLE.getSizeInBytes();
        int size = pageSize * doubleSizeInBytes;
        for (int i = 0; i < size; i += doubleSizeInBytes) {
          vector.putDouble(rowId++, ByteUtil.toDoubleLittleEndian(pageData, i));
        }
      }
    }
  };

}
