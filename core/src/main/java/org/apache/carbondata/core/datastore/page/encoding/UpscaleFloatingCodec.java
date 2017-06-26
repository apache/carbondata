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

package org.apache.carbondata.core.datastore.page.encoding;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.PrimitiveCodec;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsVO;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Codec for floating point (float, double) data type page.
 * This codec will upscale the diff from page max value to integer value,
 * and do type casting to make storage minimum.
 */
public class UpscaleFloatingCodec extends AdaptiveCompressionCodec {

  private ColumnPage encodedPage;
  private double factor;

  public static ColumnPageCodec newInstance(DataType srcDataType, DataType targetDataType,
      ColumnPageStatsVO stats, Compressor compressor) {
    return new UpscaleFloatingCodec(srcDataType, targetDataType, stats, compressor);
  }

  private UpscaleFloatingCodec(DataType srcDataType, DataType targetDataType,
      ColumnPageStatsVO stats, Compressor compressor) {
    super(srcDataType, targetDataType, stats, compressor);
    this.factor = Math.pow(10, stats.getDecimal());
  }

  @Override
  public String getName() {
    return "UpscaleFloatingCodec";
  }

  @Override
  public byte[] encode(ColumnPage input) throws MemoryException {
    if (targetDataType.equals(srcDataType)) {
      return input.compress(compressor);
    } else {
      encodedPage = ColumnPage.newPage(targetDataType, input.getPageSize());
      input.encode(codec);
      byte[] result = encodedPage.compress(compressor);
      encodedPage.freeMemory();
      return result;
    }
  }


  @Override
  public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
    if (srcDataType.equals(targetDataType)) {
      return ColumnPage.decompress(compressor, targetDataType, input, offset, length);
    } else {
      ColumnPage page = ColumnPage.decompress(compressor, targetDataType, input, offset, length);
      return LazyColumnPage.newPage(page, codec);
    }
  }

  // encoded value = (10 power of decimal) * (page value)
  private PrimitiveCodec codec = new PrimitiveCodec() {
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
          encodedPage.putByte(rowId, (byte)(Math.round(factor * value)));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(Math.round(factor * value)));
          break;
        case INT:
          encodedPage.putInt(rowId, (int)(Math.round(factor * value)));
          break;
        case LONG:
          encodedPage.putLong(rowId, (long)(Math.round(factor * value)));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, double value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(Math.round(factor * value)));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(Math.round(factor * value)));
          break;
        case INT:
          encodedPage.putInt(rowId, (int)(Math.round(factor * value)));
          break;
        case LONG:
          encodedPage.putLong(rowId, (long)(Math.round(factor * value)));
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
      return value / factor;
    }

    @Override
    public double decodeDouble(short value) {
      return value / factor;
    }

    @Override
    public double decodeDouble(int value) {
      return value / factor;
    }

    @Override
    public double decodeDouble(long value) {
      return value / factor;
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
