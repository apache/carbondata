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
import java.util.Map;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.PrimitiveCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodecMeta;
import org.apache.carbondata.core.datastore.page.encoding.Decoder;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedMeasurePage;
import org.apache.carbondata.core.datastore.page.encoding.Encoder;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Codec for integer (byte, short, int, long) data type page.
 * This codec will do type casting on page data to make storage minimum.
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
  public Encoder createEncoder(Map<String, String> parameter) {
    final Compressor compressor = CompressorFactory.getInstance().getCompressor();
    return new Encoder() {
      @Override
      public EncodedColumnPage encode(ColumnPage input)
          throws MemoryException, IOException {
        encodedPage = ColumnPage
            .newPage(targetDataType, input.getPageSize(), stats.getScale(), stats.getPrecision());
        input.encode(codec);
        byte[] result = encodedPage.compress(compressor);
        encodedPage.freeMemory();
        return new EncodedMeasurePage(
            input.getPageSize(),
            result,
            new AdaptiveIntegralCodecMeta(targetDataType, stats, compressor.getName()),
            input.getStatistics().getNullBits());
      }

      @Override
      public EncodedColumnPage[] encodeComplexColumn(ComplexColumnPage input) {
        return AdaptiveIntegralCodec.super.encodeComplexColumn(input);
      }
    };
  }

  @Override
  public Decoder createDecoder(ColumnPageCodecMeta meta) {
    AdaptiveIntegralCodecMeta codecMeta = (AdaptiveIntegralCodecMeta) meta;
    final Compressor compressor = CompressorFactory.getInstance().getCompressor(
        codecMeta.getCompressorName());
    final DataType targetDataType = codecMeta.getTargetDataType();
    final int scale = codecMeta.getScale();
    final int precision = codecMeta.getPrecision();
    return new Decoder() {
      @Override
      public ColumnPage decode(byte[] input, int offset, int length)
          throws MemoryException, IOException {
        ColumnPage page = ColumnPage.decompress(compressor, targetDataType, input, offset, length,
            scale, precision);
        return LazyColumnPage.newPage(page, codec);
      }
    };
  }

  // encoded value = (type cast page value to target data type)
  private PrimitiveCodec codec = new PrimitiveCodec() {
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
