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

package org.apache.carbondata.core.datastore.page.encoding.compress;

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
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.Encoding;

/**
 * This codec directly apply compression on the input data
 */
public class DirectCompressCodec implements ColumnPageCodec {

  private DataType dataType;

  public DirectCompressCodec(DataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public String getName() {
    return "DirectCompressCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new ColumnPageEncoder() {

      @Override
      protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
        Compressor compressor = CompressorFactory.getInstance().getCompressor(
            input.getColumnCompressorName());
        return input.compress(compressor);
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
        encodings.add(dataType == DataTypes.VARCHAR ?
            Encoding.DIRECT_COMPRESS_VARCHAR :
            Encoding.DIRECT_COMPRESS);
        return encodings;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new ColumnPageEncoderMeta(inputPage.getColumnSpec(), inputPage.getDataType(),
            inputPage.getStatistics(), inputPage.getColumnCompressorName());
      }
    };
  }

  @Override
  public ColumnPageDecoder createDecoder(final ColumnPageEncoderMeta meta) {
    return new ColumnPageDecoder() {

      @Override
      public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
        ColumnPage decodedPage;
        if (DataTypes.isDecimal(dataType)) {
          decodedPage = ColumnPage.decompressDecimalPage(meta, input, offset, length);
        } else {
          decodedPage = ColumnPage.decompress(meta, input, offset, length, false);
        }
        return LazyColumnPage.newPage(decodedPage, converter);
      }

      @Override
      public void decodeAndFillVector(byte[] input, int offset, int length,
          ColumnVectorInfo vectorInfo, BitSet nullBits, boolean isLVEncoded)
          throws MemoryException, IOException {
        ColumnPage decodedPage;
        if (DataTypes.isDecimal(dataType)) {
          decodedPage = ColumnPage.decompressDecimalPage(meta, input, offset, length);
          vectorInfo.decimalConverter = ((DecimalColumnPage) decodedPage).getDecimalConverter();
        } else {
          decodedPage = ColumnPage.decompress(meta, input, offset, length, isLVEncoded);
        }
        decodedPage.setNullBits(nullBits);
        converter.decodeAndFillVector(decodedPage, vectorInfo);
      }

      @Override public ColumnPage decode(byte[] input, int offset, int length, boolean isLVEncoded)
          throws MemoryException, IOException {
        return LazyColumnPage
            .newPage(ColumnPage.decompress(meta, input, offset, length, isLVEncoded), converter);
      }
    };
  }

  private ColumnPageValueConverter converter = new ColumnPageValueConverter() {
    @Override
    public void encode(int rowId, byte value) {
      throw new RuntimeException("internal error");
    }

    @Override
    public void encode(int rowId, short value) {
      throw new RuntimeException("internal error");
    }

    @Override
    public void encode(int rowId, int value) {
      throw new RuntimeException("internal error");
    }

    @Override
    public void encode(int rowId, long value) {
      throw new RuntimeException("internal error");
    }

    @Override
    public void encode(int rowId, float value) {
      throw new RuntimeException("internal error");
    }

    @Override
    public void encode(int rowId, double value) {
      throw new RuntimeException("internal error");
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
      return value;
    }

    @Override
    public double decodeDouble(double value) {
      return value;
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
            vector.putShort(i, (short) byteData[i]);
          }
        } else if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, (int) byteData[i]);
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, byteData[i]);
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, byteData[i] * 1000);
          }
        } else if (vectorDataType == DataTypes.BOOLEAN || vectorDataType == DataTypes.BYTE) {
          vector.putBytes(0, pageSize, byteData, 0);
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(byteData, pageSize, vectorInfo, columnPage.getNullBits());
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, byteData[i]);
          }
        }
      } else if (pageDataType == DataTypes.SHORT) {
        short[] shortData = columnPage.getShortPage();
        if (vectorDataType == DataTypes.SHORT) {
          vector.putShorts(0, pageSize, shortData, 0);
        } else if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            vector.putInt(i, (int) shortData[i]);
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, shortData[i]);
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, shortData[i] * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(shortData, pageSize, vectorInfo, columnPage.getNullBits());
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, shortData[i]);
          }
        }

      } else if (pageDataType == DataTypes.SHORT_INT) {
        byte[] shortIntPage = columnPage.getShortIntPage();
        if (vectorDataType == DataTypes.INT) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            vector.putInt(i, shortInt);
          }
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            vector.putLong(i, shortInt);
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            vector.putLong(i, shortInt * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          int[] shortIntData = ByteUtil.toIntArray(shortIntPage, pageSize);
          decimalConverter.fillVector(shortIntData, pageSize, vectorInfo, columnPage.getNullBits());
        } else {
          for (int i = 0; i < pageSize; i++) {
            int shortInt = ByteUtil.valueOf3Bytes(shortIntPage, i * 3);
            vector.putDouble(i, shortInt);
          }
        }
      } else if (pageDataType == DataTypes.INT) {
        int[] intData = columnPage.getIntPage();
        if (vectorDataType == DataTypes.INT) {
          vector.putInts(0, pageSize, intData, 0);
        } else if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, intData[i]);
          }
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, intData[i] * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(intData, pageSize, vectorInfo, columnPage.getNullBits());
        } else {
          for (int i = 0; i < pageSize; i++) {
            vector.putDouble(i, intData[i]);
          }
        }
      }  else if (pageDataType == DataTypes.LONG) {
        long[] longData = columnPage.getLongPage();
        if (vectorDataType == DataTypes.LONG) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, longData[i]);
          }
          vector.putLongs(0, pageSize, longData, 0);
        } else if (vectorDataType == DataTypes.TIMESTAMP) {
          for (int i = 0; i < pageSize; i++) {
            vector.putLong(i, longData[i] * 1000);
          }
        } else if (DataTypes.isDecimal(vectorDataType)) {
          DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
          decimalConverter.fillVector(longData, pageSize, vectorInfo, columnPage.getNullBits());
        }
      } else if (DataTypes.isDecimal(pageDataType)) {
        DecimalConverterFactory.DecimalConverter decimalConverter = vectorInfo.decimalConverter;
        decimalConverter.fillVector(columnPage.getByteArrayPage(), pageSize, vectorInfo,
            columnPage.getNullBits());
      } else {
        double[] doubleData = columnPage.getDoublePage();
        vector.putDoubles(0, pageSize, doubleData, 0);
      }
    }
  };

}
