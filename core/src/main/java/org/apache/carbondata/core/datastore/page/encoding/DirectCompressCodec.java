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

import java.io.IOException;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.PrimitiveCodec;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;

/**
 * This codec directly apply compression on the input data
 */
public class DirectCompressCodec implements ColumnPageCodec {

  private Compressor compressor;
  private SimpleStatsResult stats;

  private DirectCompressCodec(SimpleStatsResult stats, Compressor compressor) {
    this.compressor = compressor;
    this.stats = stats;
  }

  public static DirectCompressCodec newInstance(SimpleStatsResult stats, Compressor compressor) {
    return new DirectCompressCodec(stats, compressor);
  }

  @Override
  public String getName() {
    return "DirectCompressCodec";
  }

  @Override
  public EncodedColumnPage encode(ColumnPage input) throws IOException, MemoryException {
    byte[] result = input.compress(compressor);
    return new EncodedMeasurePage(input.getPageSize(), result,
        ValueEncoderMeta.newInstance(stats, stats.getDataType()));
  }

  @Override
  public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
    ColumnPage page = ColumnPage.decompress(compressor, stats.getDataType(), input, offset, length);
    return LazyColumnPage.newPage(page, codec);
  }

  private PrimitiveCodec codec = new PrimitiveCodec() {
    @Override
    public void encode(int rowId, byte value) {
    }

    @Override
    public void encode(int rowId, short value) {
    }

    @Override
    public void encode(int rowId, int value) {
    }

    @Override
    public void encode(int rowId, long value) {
    }

    @Override
    public void encode(int rowId, float value) {
    }

    @Override
    public void encode(int rowId, double value) {
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
  };
}
