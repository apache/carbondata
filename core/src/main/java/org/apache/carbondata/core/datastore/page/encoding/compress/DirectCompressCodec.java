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
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.PrimitiveCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodecMeta;
import org.apache.carbondata.core.datastore.page.encoding.Decoder;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedMeasurePage;
import org.apache.carbondata.core.datastore.page.encoding.Encoder;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;

/**
 * This codec directly apply compression on the input data
 */
public class DirectCompressCodec implements ColumnPageCodec {

  private SimpleStatsResult stats;

  public DirectCompressCodec(SimpleStatsResult stats) {
    this.stats = stats;
  }

  @Override
  public String getName() {
    return "DirectCompressCodec";
  }

  @Override
  public Encoder createEncoder(Map<String, String> parameter) {
    // TODO: make compressor configurable in create table
    return new DirectCompressor(CarbonCommonConstants.DEFAULT_COMPRESSOR);
  }

  @Override
  public Decoder createDecoder(ColumnPageCodecMeta meta) {
    DirectCompressorCodecMeta codecMeta = (DirectCompressorCodecMeta) meta;
    return new DirectDecompressor(codecMeta.getCompressorName());
  }

  private class DirectCompressor implements Encoder {

    private Compressor compressor;

    DirectCompressor(String compressorName) {
      this.compressor = CompressorFactory.getInstance().getCompressor(compressorName);
    }

    @Override
    public EncodedColumnPage encode(ColumnPage input) throws IOException, MemoryException {
      byte[] result = input.compress(compressor);
      return new EncodedMeasurePage(
          input.getPageSize(),
          result,
          new DirectCompressorCodecMeta(compressor.getName(), stats.getDataType(), stats),
          input.getNullBits());
    }

    @Override
    public EncodedColumnPage[] encodeComplexColumn(ComplexColumnPage input) {
      throw new UnsupportedOperationException("internal error");
    }
  }

  private class DirectDecompressor implements Decoder {

    private Compressor compressor;

    DirectDecompressor(String compressorName) {
      this.compressor = CompressorFactory.getInstance().getCompressor(compressorName);
    }

    @Override
    public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
      ColumnPage decodedPage = ColumnPage.decompress(compressor, stats.getDataType(), input,
          offset, length, stats.getScale(), stats.getPrecision());
      return LazyColumnPage.newPage(decodedPage, codec);
    }

    private PrimitiveCodec codec = new PrimitiveCodec() {
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
        throw new RuntimeException("internal error");
      }

      @Override
      public double decodeDouble(double value) {
        return value;
      }
    };
  }

}
