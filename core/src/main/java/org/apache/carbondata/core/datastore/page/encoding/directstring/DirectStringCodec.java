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

package org.apache.carbondata.core.datastore.page.encoding.directstring;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.VarLengthColumnPageBase;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageCodec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.format.Encoding;

/**
 * This encoding accepts ColumnPage of String type and output two byte arrays, one for
 * string content and another for string length. These two byte arrays are compressed
 * separated. The string length byte array is stored in encoder meta (DirectStringEncoderMeta)
 */
public class DirectStringCodec implements ColumnPageCodec {
  @Override
  public String getName() {
    return "DirectStringCodec";
  }

  @Override
  public ColumnPageEncoder createEncoder(Map<String, String> parameter) {
    return new ColumnPageEncoder() {
      Compressor compressor = CompressorFactory.getInstance().getCompressor();
      short[] lengthOfString;

      @Override
      protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
        assert (input.getDataType() == DataType.STRING);
        lengthOfString = ((VarLengthColumnPageBase)input).getLengths();
        byte[] flattened = input.getDirectFlattenedBytePage();
        return compressor.compressByte(flattened);
      }

      @Override
      protected List<Encoding> getEncodingList() {
        List<Encoding> encodings = new ArrayList<>();
        encodings.add(Encoding.DIRECT_STRING);
        return encodings;
      }

      @Override
      protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
        return new DirectStringEncoderMeta(
            inputPage.getColumnSpec(), lengthOfString, compressor.getName());
      }
    };
  }

  @Override
  public ColumnPageDecoder createDecoder(final ColumnPageEncoderMeta meta) {
    final DirectStringEncoderMeta encoderMeta = (DirectStringEncoderMeta) meta;
    final Compressor compressor = CompressorFactory.getInstance().getCompressor(
        encoderMeta.getCompressorName());
    return new ColumnPageDecoder() {
      @Override
      public ColumnPage decode(byte[] input, int offset, int length)
          throws MemoryException, IOException {
        return ColumnPage.decompressStringPage(encoderMeta.getColumnSpec(), compressor, input,
            offset, length, encoderMeta.getLengthOfString());
      }
    };
  }
}
