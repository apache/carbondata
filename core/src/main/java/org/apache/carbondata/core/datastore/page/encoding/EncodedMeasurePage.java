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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.metadata.ColumnPageCodecMeta;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.PresenceMeta;

/**
 * Encoded measure page that include data and statistics
 */
public class EncodedMeasurePage extends EncodedColumnPage {

  private ValueEncoderMeta metaData;

  public EncodedMeasurePage(int pageSize, byte[] encodedData, ValueEncoderMeta metaData,
      BitSet nullBitSet) throws IOException {
    super(pageSize, encodedData);
    this.metaData = metaData;
    this.nullBitSet = nullBitSet;
    this.dataChunk2 = buildDataChunk2();
  }

  @Override
  public DataChunk2 buildDataChunk2() throws IOException {
    DataChunk2 dataChunk = new DataChunk2();
    dataChunk.min_max = new BlockletMinMaxIndex();
    dataChunk.setChunk_meta(CarbonMetadataUtil.getSnappyChunkCompressionMeta());
    dataChunk.setNumberOfRowsInpage(pageSize);
    dataChunk.setData_page_length(encodedData.length);
    dataChunk.setRowMajor(false);
    // TODO : Change as per this encoders.
    List<Encoding> encodings = new ArrayList<Encoding>();
    encodings.add(Encoding.DELTA);
    dataChunk.setEncoders(encodings);
    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setPresent_bit_streamIsSet(true);
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    presenceMeta.setPresent_bit_stream(compressor.compressByte(nullBitSet.toByteArray()));
    dataChunk.setPresence(presenceMeta);
    List<ByteBuffer> encoderMetaList = new ArrayList<ByteBuffer>();
    if (metaData instanceof ColumnPageCodecMeta) {
      ColumnPageCodecMeta meta = (ColumnPageCodecMeta) metaData;
      encoderMetaList.add(ByteBuffer.wrap(meta.serialize()));
      dataChunk.min_max.addToMax_values(ByteBuffer.wrap(meta.getMaxAsBytes()));
      dataChunk.min_max.addToMin_values(ByteBuffer.wrap(meta.getMinAsBytes()));
    } else {
      encoderMetaList.add(ByteBuffer.wrap(CarbonUtil.serializeEncodeMetaUsingByteBuffer(metaData)));
      dataChunk.min_max.addToMax_values(ByteBuffer.wrap(CarbonUtil.getMaxValueAsBytes(metaData)));
      dataChunk.min_max.addToMin_values(ByteBuffer.wrap(CarbonUtil.getMinValueAsBytes(metaData)));
    }
    dataChunk.setEncoder_meta(encoderMetaList);
    return dataChunk;
  }

  public ValueEncoderMeta getMetaData() {
    return metaData;
  }
}