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
 * Codec for integer (byte, short, int, long) data type and floating data type (in case of
 * scale is 0).
 * This codec will calculate delta of page max value and page value,
 * and do type casting of the diff to make storage minimum.
 */
public class AdaptiveDeltaIntegralCodec extends AdaptiveCodec {

  private ColumnPage convertedPage;
  private long max;

  public AdaptiveDeltaIntegralCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats) {
    super(srcDataType, targetDataType, stats);
    switch (srcDataType) {
      case BYTE:
        this.max = (byte) stats.getMax();
        break;
      case SHORT:
        this.max = (short) stats.getMax();
        break;
      case TIMESTAMP:
      case DATE:
      case INT:
        this.max = (int) stats.getMax();
        break;
      case LONG:
        this.max = (long) stats.getMax();
        break;
      case DOUBLE:
        this.max = (long) (double) stats.getMax();
        break;
      default:
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
      final Compressor compressor = CompressorFactory.getInstance().getCompressor();

      @Override
      protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
        if (convertedPage != null) {
          throw new IllegalStateException("already encoded");
        }
        convertedPage =
            ColumnPage.newPage(input.getColumnSpec(), targetDataType, input.getPageSize());
        input.convertValue(converter);
        byte[] result = convertedPage.compress(compressor);
        convertedPage.freeMemory();
        return result;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new AdaptiveDeltaIntegralEncoderMeta(inputPage.getColumnSpec(),
            compressor.getName(), targetDataType, inputPage.getStatistics());
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
        encodings.add(Encoding.ADAPTIVE_DELTA_INTEGRAL);
        return encodings;
      }

    };
  }

  @Override
  public ColumnPageDecoder createDecoder(final ColumnPageEncoderMeta meta) {
    assert meta instanceof AdaptiveDeltaIntegralEncoderMeta;
    AdaptiveDeltaIntegralEncoderMeta codecMeta = (AdaptiveDeltaIntegralEncoderMeta) meta;
    final Compressor compressor = CompressorFactory.getInstance().getCompressor(
        codecMeta.getCompressorName());
    return new ColumnPageDecoder() {
      @Override
      public ColumnPage decode(byte[] input, int offset, int length)
          throws MemoryException, IOException {
        ColumnPage page = ColumnPage.decompress(
            meta.getColumnSpec(), compressor, targetDataType, input, offset, length);
        return LazyColumnPage.newPage(srcDataType, page, converter);
      }
    };
  }

  private ColumnPageValueConverter converter = new ColumnPageValueConverter() {
    @Override
    public void encode(int rowId, byte value) {
      switch (targetDataType) {
        case BYTE:
          convertedPage.putByte(rowId, (byte)(max - value));
          break;
        default:
          throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, short value) {
      switch (targetDataType) {
        case BYTE:
          convertedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          convertedPage.putShort(rowId, (short)(max - value));
          break;
        default:
          throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, int value) {
      switch (targetDataType) {
        case BYTE:
          convertedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          convertedPage.putShort(rowId, (short)(max - value));
          break;
        case SHORT_INT:
          convertedPage.putShortInt(rowId, (int)(max - value));
          break;
        case INT:
          convertedPage.putInt(rowId, (int)(max - value));
          break;
        default:
          throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, long value) {
      switch (targetDataType) {
        case BYTE:
          convertedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          convertedPage.putShort(rowId, (short)(max - value));
          break;
        case SHORT_INT:
          convertedPage.putShortInt(rowId, (int)(max - value));
          break;
        case INT:
          convertedPage.putInt(rowId, (int)(max - value));
          break;
        case LONG:
          convertedPage.putLong(rowId, max - value);
          break;
        default:
          throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, float value) {
      switch (targetDataType) {
        case BYTE:
          convertedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          convertedPage.putShort(rowId, (short)(max - value));
          break;
        case SHORT_INT:
          convertedPage.putShortInt(rowId, (int)(max - value));
          break;
        case INT:
          convertedPage.putInt(rowId, (int)(max - value));
          break;
        case LONG:
          convertedPage.putLong(rowId, (long)(max - value));
          break;
        default:
          throw new RuntimeException("internal error");
      }
    }

    @Override
    public void encode(int rowId, double value) {
      switch (targetDataType) {
        case BYTE:
          convertedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          convertedPage.putShort(rowId, (short)(max - value));
          break;
        case SHORT_INT:
          convertedPage.putShortInt(rowId, (int)(max - value));
          break;
        case INT:
          convertedPage.putInt(rowId, (int)(max - value));
          break;
        case LONG:
          convertedPage.putLong(rowId, (long)(max - value));
          break;
        default:
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
  };
}
