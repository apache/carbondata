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
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPageValueConverter;
import org.apache.carbondata.core.datastore.page.LazyColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
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
    // TODO: make compressor configurable in create table
    return new DirectCompressor(CarbonCommonConstants.DEFAULT_COMPRESSOR);
  }

  @Override
  public ColumnPageDecoder createDecoder(ColumnPageEncoderMeta meta) {
    assert meta instanceof DirectCompressorEncoderMeta;
    DirectCompressorEncoderMeta codecMeta = (DirectCompressorEncoderMeta) meta;
    return new DirectDecompressor(meta.getColumnSpec(), codecMeta.getCompressorName(),
        codecMeta.getScale(), codecMeta.getPrecision());
  }

  private static class DirectCompressor extends ColumnPageEncoder {

    private Compressor compressor;

    DirectCompressor(String compressorName) {
      this.compressor = CompressorFactory.getInstance().getCompressor(compressorName);
    }

    @Override
    protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
      return input.compress(compressor);
    }

    @Override
    protected List<Encoding> getEncodingList() {
      List<Encoding> encodings = new ArrayList<>();
      encodings.add(Encoding.DIRECT_COMPRESS);
      return encodings;
    }

    @Override
    protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
      return new DirectCompressorEncoderMeta(inputPage.getColumnSpec(),
          compressor.getName(), inputPage.getDataType(), inputPage.getStatistics());
    }

  }

  private class DirectDecompressor implements ColumnPageDecoder {

    private TableSpec.ColumnSpec columnSpec;
    private Compressor compressor;
    private int scale;
    private int precision;

    DirectDecompressor(TableSpec.ColumnSpec columnSpec, String compressorName, int scale,
        int precision) {
      this.columnSpec = columnSpec;
      this.compressor = CompressorFactory.getInstance().getCompressor(compressorName);
      this.scale = scale;
      this.precision = precision;
    }

    @Override
    public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
      ColumnPage decodedPage;
      if (dataType == DataType.DECIMAL) {
        decodedPage = ColumnPage.decompressDecimalPage(
            columnSpec, compressor, input, offset, length, scale, precision);
      } else {
        decodedPage = ColumnPage.decompress(
            columnSpec, compressor, dataType, input, offset, length);
      }
      return LazyColumnPage.newPage(dataType, decodedPage, converter);
    }
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
  };

}
