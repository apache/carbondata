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
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;

/**
 * Codec for floating point (float, double) data type page.
 * This codec will calculate delta of page max value and page value and converts to Integer value,
 * and do type casting of the diff to make storage minimum.
 */
public class AdaptiveDeltaFloatingCodec extends AdaptiveCodec {

  private Double factor;
  private long max;

  public static ColumnPageCodec newInstance(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats, boolean isInvertedIndex) {
    return new AdaptiveDeltaFloatingCodec(srcDataType, targetDataType, stats,
        isInvertedIndex);
  }

  public AdaptiveDeltaFloatingCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats, boolean isInvertedIndex) {
    super(srcDataType, targetDataType, stats, isInvertedIndex);
    this.factor = Math.pow(10, stats.getDecimalCount());
    if (srcDataType == DataTypes.FLOAT) {
      this.max =
          (long) ((long) Math.pow(10, stats.getDecimalCount()) * ((float) stats.getMax()));
    } else {
      this.max = (long) ((long) Math.pow(10, stats.getDecimalCount()) * (double) stats.getMax());
    }
  }

  @Override
  public String getName() {
    return "AdaptiveDeltaFloatingCodec";
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
        Compressor compressor = CompressorFactory.getInstance().getCompressor(
            input.getColumnCompressorName());
        result = encodeAndCompressPage(input, converter, compressor);
        byte[] bytes = writeInvertedIndexIfRequired(result);
        encodedPage.freeMemory();
        if (bytes.length != 0) {
          return bytes;
        }
        return result;
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<Encoding>();
        encodings.add(Encoding.ADAPTIVE_DELTA_FLOATING);
        if (null != indexStorage && indexStorage.getRowIdPageLengthInBytes() > 0) {
          encodings.add(Encoding.INVERTED_INDEX);
        }
        return encodings;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new ColumnPageEncoderMeta(inputPage.getColumnSpec(), targetDataType, stats,
            inputPage.getColumnCompressorName());
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
        ColumnPage page = ColumnPage.decompress(meta, input, offset, length, false);
        return LazyColumnPage.newPage(page, converter);
      }

      @Override
      public void decodeAndFillVector(byte[] input, int offset, int length,
          ColumnVectorInfo vectorInfo, BitSet nullBits, boolean isLVEncoded)
          throws MemoryException, IOException {
        ColumnPage page = ColumnPage.decompress(meta, input, offset, length, isLVEncoded);
        page.setNullBits(nullBits);
        if (page instanceof DecimalColumnPage) {
          vectorInfo.decimalConverter = ((DecimalColumnPage) page).getDecimalConverter();
        }
        converter.decodeAndFillVector(page, vectorInfo);
      }

      @Override public ColumnPage decode(byte[] input, int offset, int length, boolean isLVEncoded)
          throws MemoryException, IOException {
        return decode(input, offset, length);
      }
    };
  }

  // encoded value = (10 power of decimal) * (page value)
  private ColumnPageValueConverter converter = new ColumnPageValueConverter() {
    @Override
    public void encode(int rowId, byte value) {
      // this codec is for floating point type only
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public void encode(int rowId, short value) {
      // this codec is for floating point type only
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public void encode(int rowId, int value) {
      // this codec is for floating point type only
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public void encode(int rowId, long value) {
      // this codec is for floating point type only
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public void encode(int rowId, float value) {
      if (targetDataType.equals(DataTypes.BYTE)) {
        encodedPage.putByte(rowId, (byte) (max - (value * factor)));
      } else if (targetDataType.equals(DataTypes.SHORT)) {
        encodedPage.putShort(rowId, (short) (max - (value * factor)));
      } else if (targetDataType.equals(DataTypes.SHORT_INT)) {
        encodedPage.putShortInt(rowId, (int) (max - (value * factor)));
      } else if (targetDataType.equals(DataTypes.INT)) {
        encodedPage.putInt(rowId, (int) (max - (value * factor)));
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, double value) {
      if (targetDataType.equals(DataTypes.BYTE)) {
        encodedPage.putByte(rowId, (byte) (max - Math.round(value * factor)));
      } else if (targetDataType.equals(DataTypes.SHORT)) {
        encodedPage.putShort(rowId, (short) (max - Math.round(value * factor)));
      } else if (targetDataType.equals(DataTypes.SHORT_INT)) {
        encodedPage.putShortInt(rowId, (int) (max - Math.round(value * factor)));
      } else if (targetDataType.equals(DataTypes.INT)) {
        encodedPage.putInt(rowId, (int) (max - Math.round(value * factor)));
      } else if (targetDataType.equals(DataTypes.DOUBLE)) {
        encodedPage.putDouble(rowId, value);
      } else {
        throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public long decodeLong(byte value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public long decodeLong(short value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public long decodeLong(int value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public double decodeDouble(byte value) {
      return (max - value) / factor;
    }

    @Override
    public double decodeDouble(short value) {
      return (max - value) / factor;
    }

    @Override
    public double decodeDouble(int value) {
      return (max - value) / factor;
    }

    @Override
    public double decodeDouble(long value) {
      return (max - value) / factor;
    }

    @Override
    public void decodeAndFillVector(ColumnPage columnPage, ColumnVectorInfo vectorInfo) {
      CarbonColumnVector vector = vectorInfo.vector;
      BitSet nullBits = columnPage.getNullBits();
      DataType pageDataType = columnPage.getDataType();
      int pageSize = columnPage.getPageSize();
      BitSet deletedRows = vectorInfo.deletedRows;
      DataType vectorDataType = vector.getType();
      if (vectorDataType == DataTypes.FLOAT) {
        float floatFactor = factor.floatValue();
        if (pageDataType == DataTypes.BOOLEAN || pageDataType == DataTypes.BYTE) {
          byte[] byteData = columnPage.getBytePage();
          for (int i = 0; i < pageSize; i++) {
            vector.putFloat(i, (max - byteData[i]) / floatFactor);
          }
        } else if (pageDataType == DataTypes.SHORT) {
          short[] shortData = columnPage.getShortPage();
          for (int i = 0; i < pageSize; i++) {
            vector.putFloat(i, (max - shortData[i]) / floatFactor);
          }

        } else if (pageDataType == DataTypes.SHORT_INT) {
          byte[] shortIntPage = columnPage.getShortIntPage();
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            vector.putFloat(i, (max - shortInt) / floatFactor);
          }
        } else {
          throw new RuntimeException("internal error: " + this.toString());
        }
      } else {
        if (pageDataType == DataTypes.BOOLEAN || pageDataType == DataTypes.BYTE) {
          byte[] byteData = columnPage.getBytePage();
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, (max - byteData[i]) / factor);
          }
        } else if (pageDataType == DataTypes.SHORT) {
          short[] shortData = columnPage.getShortPage();
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, (max - shortData[i]) / factor);
          }

        } else if (pageDataType == DataTypes.SHORT_INT) {
          byte[] shortIntPage = columnPage.getShortIntPage();
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            vector.putDouble(i, (max - shortInt) / factor);
          }
        } else if (pageDataType == DataTypes.INT) {
          int[] intData = columnPage.getIntPage();
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, (max - intData[i]) / factor);
          }
        } else {
          throw new RuntimeException("Unsupported datatype : " + pageDataType);
        }
      }

      if (deletedRows == null || deletedRows.isEmpty()) {
        for (int i = nullBits.nextSetBit(0); i >= 0; i = nullBits.nextSetBit(i + 1)) {
          vector.putNull(i);
        }
      }
    }

    @Override
    public double decodeDouble(float value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public double decodeDouble(double value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }
  };
}