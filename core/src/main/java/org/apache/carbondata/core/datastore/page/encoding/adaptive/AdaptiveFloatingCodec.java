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
 * This codec will upscale the diff from page max value to integer value,
 * and do type casting to make storage minimum.
 */
public class AdaptiveFloatingCodec extends AdaptiveCodec {

  private ColumnPage encodedPage;
  private BigDecimal factor;

  public static ColumnPageCodec newInstance(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats) {
    return new AdaptiveFloatingCodec(srcDataType, targetDataType, stats);
  }

  public AdaptiveFloatingCodec(DataType srcDataType, DataType targetDataType,
      SimpleStatsResult stats) {
    super(srcDataType, targetDataType, stats);
    this.factor = BigDecimal.valueOf(Math.pow(10, stats.getDecimalCount()));
  }

  @Override
  public String getName() {
    return "AdaptiveFloatingCodec";
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
        encodings.add(Encoding.ADAPTIVE_FLOATING);
        return encodings;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new AdaptiveFloatingEncoderMeta(targetDataType, stats, compressor.getName());
      }

    };
  }

  @Override
  public ColumnPageDecoder createDecoder(ColumnPageEncoderMeta meta) {
    assert meta instanceof AdaptiveFloatingEncoderMeta;
    AdaptiveFloatingEncoderMeta codecMeta = (AdaptiveFloatingEncoderMeta) meta;
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
          encodedPage.putByte(rowId,
              BigDecimal.valueOf(value).multiply(factor).byteValue());
          break;
        case SHORT:
          encodedPage.putShort(rowId,
              BigDecimal.valueOf(value).multiply(factor).shortValue());
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId,
              BigDecimal.valueOf(value).multiply(factor).intValue());
          break;
        case INT:
          encodedPage.putInt(rowId,
              BigDecimal.valueOf(value).multiply(factor).intValue());
          break;
        case LONG:
          encodedPage.putLong(rowId,
              BigDecimal.valueOf(value).multiply(factor).longValue());
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, double value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId,
              BigDecimal.valueOf(value).multiply(factor).byteValue());
          break;
        case SHORT:
          encodedPage.putShort(rowId,
              BigDecimal.valueOf(value).multiply(factor).shortValue());
          break;
        case SHORT_INT:
          encodedPage.putShortInt(rowId,
              BigDecimal.valueOf(value).multiply(factor).intValue());
          break;
        case INT:
          encodedPage.putInt(rowId,
              BigDecimal.valueOf(value).multiply(factor).intValue());
          break;
        case LONG:
          encodedPage.putLong(rowId,
              BigDecimal.valueOf(value).multiply(factor).longValue());
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
      return BigDecimal.valueOf(value).divide(factor).doubleValue();
    }

    @Override
    public double decodeDouble(short value) {
      return BigDecimal.valueOf(value).divide(factor).doubleValue();
    }

    @Override
    public double decodeDouble(int value) {
      return BigDecimal.valueOf(value).divide(factor).doubleValue();
    }

    @Override
    public double decodeDouble(long value) {
      return BigDecimal.valueOf(value).divide(factor).doubleValue();
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