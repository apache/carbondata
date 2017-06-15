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
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Codec for integer (byte, short, int, long) data type page.
 * This codec will calculate delta of page max value and page value,
 * and do type casting of the diff to make storage minimum.
 */
public class DeltaIntegerCodec extends AdaptiveCompressionCodec {

  private ColumnPage encodedPage;

  private long max;

  public static DeltaIntegerCodec newInstance(DataType srcDataType, DataType targetDataType,
      ColumnPageStatsVO stats, Compressor compressor) {
    return new DeltaIntegerCodec(srcDataType, targetDataType, stats, compressor);
  }

  private DeltaIntegerCodec(DataType srcDataType, DataType targetDataType,
      ColumnPageStatsVO stats, Compressor compressor) {
    super(srcDataType, targetDataType, stats, compressor);
    switch (srcDataType) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        max = (long) stats.getMax();
        break;
      case FLOAT:
      case DOUBLE:
        max = (long)((double) stats.getMax());
        break;
    }
  }

  @Override
  public String getName() {
    return "DeltaIntegerCodec";
  }

  @Override
  public byte[] encode(ColumnPage input) {
    encodedPage = ColumnPage.newPage(targetDataType, input.getPageSize());
    input.encode(codec);
    return encodedPage.compress(compressor);
  }

  @Override
  public ColumnPage decode(byte[] input, int offset, int length) {
    ColumnPage page = ColumnPage.decompress(compressor, targetDataType, input, offset, length);
    return LazyColumnPage.newPage(page, codec);
  }

  // encoded value = (max value of page) - (page value)
  private PrimitiveCodec codec = new PrimitiveCodec() {
    @Override
    public void encode(int rowId, byte value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, short value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(max - value));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, int value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(max - value));
          break;
        case INT:
          encodedPage.putInt(rowId, (int)(max - value));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, long value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(max - value));
          break;
        case INT:
          encodedPage.putInt(rowId, (int)(max - value));
          break;
        case LONG:
          encodedPage.putLong(rowId, max - value);
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, float value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(max - value));
          break;
        case INT:
          encodedPage.putInt(rowId, (int)(max - value));
          break;
        case LONG:
          encodedPage.putLong(rowId, (long)(max - value));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
      }
    }

    @Override
    public void encode(int rowId, double value) {
      switch (targetDataType) {
        case BYTE:
          encodedPage.putByte(rowId, (byte)(max - value));
          break;
        case SHORT:
          encodedPage.putShort(rowId, (short)(max - value));
          break;
        case INT:
          encodedPage.putInt(rowId, (int)(max - value));
          break;
        case LONG:
          encodedPage.putLong(rowId, (long)(max - value));
          break;
        default:
          throw new RuntimeException("internal error: " + debugInfo());
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
      throw new RuntimeException("internal error: " + debugInfo());
    }

    @Override
    public double decodeDouble(double value) {
      // this codec is for integer type only
      throw new RuntimeException("internal error: " + debugInfo());
    }
  };
}
