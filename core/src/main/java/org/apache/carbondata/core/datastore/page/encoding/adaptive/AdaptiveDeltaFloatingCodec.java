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
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.format.Encoding;

/**
 * Codec for floating point (float, double) data type page.
 * This codec will calculate delta of page max value and page value and converts to Integer value,
 * and do type casting of the diff to make storage minimum.
 */
public class AdaptiveDeltaFloatingCodec extends AdaptiveCodec {

  private ColumnPage encodedPage;
  private Double factor;
  private long max;

  public static ColumnPageCodec newInstance(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats) {
    return new AdaptiveDeltaFloatingCodec(srcDataType, targetDataType, stats);
  }

  public AdaptiveDeltaFloatingCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats) {
    super(srcDataType, targetDataType, stats);
    this.factor = Math.pow(10, stats.getDecimalCount());
    this.max = (long) (Math.pow(10, stats.getDecimalCount()) * (double) stats.getMax());
  }

  @Override
  public String getName() {
    return "AdaptiveDeltaFloatingCodec";
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
        encodedPage = ColumnPage.newPage(input.getColumnSpec(), targetDataType,
            input.getPageSize());
        input.convertValue(converter);
        byte[] result = encodedPage.compress(compressor);
        encodedPage.freeMemory();
        return result;
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<Encoding>();
        encodings.add(Encoding.ADAPTIVE_DELTA_FLOATING);
        return encodings;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new ColumnPageEncoderMeta(inputPage.getColumnSpec(), targetDataType, stats,
            compressor.getName());
      }

    };
  }

  @Override
  public ColumnPageDecoder createDecoder(final ColumnPageEncoderMeta meta) {
    return new ColumnPageDecoder() {
      @Override
      public ColumnPage decode(byte[] input, int offset, int length)
          throws MemoryException, IOException {
        ColumnPage page = ColumnPage.decompress(meta, input, offset, length);
        return LazyColumnPage.newPage(page, converter);
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
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) (max - (value * factor)));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short) (max - (value * factor)));
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId, (int) (max - (value * factor)));
          break;
        case INT:
          encodedPage.putInt(rowId, (int) (max - (value * factor)));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, double value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte) (max - (value * factor)));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short) (max - (value * factor)));
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId, (int) (max - (value * factor)));
          break;
        case INT:
          encodedPage.putInt(rowId, (int) (max - (value * factor)));
          break;
        case DOUBLE:
          encodedPage.putDouble(rowId, value);
          break;
        default:
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
    public double decodeDouble(float value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public double decodeDouble(double value) {
      throw new RuntimeException("internal error: " + debugInfo());
    }
  };
}