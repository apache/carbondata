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
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPageValueConverter;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.format.Encoding;

/**
 * Codec for integer (byte, short, int, long) data type page.
 * This converter will do type casting on page data to make storage minimum.
 */
public class AdaptiveIntegralCodec extends AdaptiveCodec {

  private ColumnPage encodedPage;

  public AdaptiveIntegralCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats) {
    super(srcDataType, targetDataType, stats);
  }

  @Override
  public String getName() {
    return "AdaptiveIntegralCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    final Compressor compressor = CompressorFactory.getInstance().getCompressor();
    return new ColumnPageEncoder() {
      @Override
      protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
        if (encodedPage != null) {
          throw new IllegalStateException("already encoded");
        }
        encodedPage = ColumnPage.newPage(targetDataType, input.getPageSize());
        input.convertValue(converter);
        byte[] result = encodedPage.compress(compressor);
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
        return new AdaptiveIntegralEncoderMeta(targetDataType, stats, compressor.getName());
      }

    };
  }

  @Override
  public ColumnPageDecoder createDecoder(ColumnPageEncoderMeta meta) {
    AdaptiveIntegralEncoderMeta codecMeta = (AdaptiveIntegralEncoderMeta) meta;
    final Compressor compressor = CompressorFactory.getInstance().getCompressor(
        codecMeta.getCompressorName());
    final DataType targetDataType = codecMeta.getTargetDataType();
    return new ColumnPageDecoder() {
      @Override
      public ColumnPage decode(byte[] input, int offset, int length)
          throws MemoryException, IOException {
        ColumnPage page = ColumnPage.decompress(compressor, targetDataType, input, offset, length);
        return LazyColumnPage.newPage(page, converter);
      }
    };
  }

  // encoded value = (type cast page value to target data type)
  private ColumnPageValueConverter converter = new ColumnPageValueConverter() {
    @Override
    public void encode(int rowId, byte value) {
      switch (targetDataType) {
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, short value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, int value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) value);
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short) value);
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId, value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, long value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) value);
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short) value);
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId, (int) value);
          break;
        case INT:
          encodedPage.putInt(rowId, (int) value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, float value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) value);
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short) value);
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId, (int) value);
          break;
        case INT:
          encodedPage.putInt(rowId, (int) value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, double value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) value);
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short) value);
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId, (int) value);
          break;
        case INT:
          encodedPage.putInt(rowId, (int) value);
          break;
        case LONG:
          encodedPage.putLong(rowId, (long) value);
          break;
        default:
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
  };

}
