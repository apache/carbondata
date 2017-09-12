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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.ComplexColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.compress.DirectCompressCodec;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.PresenceMeta;

public abstract class ColumnPageEncoder {

  protected abstract byte[] encodeData(ColumnPage input) throws MemoryException, IOException;

  protected abstract List<Encoding> getEncodingList();

  protected abstract ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage);

  /**
   * Return a encoded column page by encoding the input page
   * The encoded binary data and metadata are wrapped in encoding column page
   */
  public EncodedColumnPage encode(ColumnPage inputPage) throws IOException, MemoryException {
    byte[] encodedBytes = encodeData(inputPage);
    DataChunk2 pageMetadata = buildPageMetadata(inputPage, encodedBytes);
    return new EncodedColumnPage(pageMetadata, encodedBytes, inputPage.getStatistics());
  }

  private DataChunk2 buildPageMetadata(ColumnPage inputPage, byte[] encodedBytes)
      throws IOException {
    DataChunk2 dataChunk = new DataChunk2();
    dataChunk.setData_page_length(encodedBytes.length);
    fillBasicFields(inputPage, dataChunk);
    fillNullBitSet(inputPage, dataChunk);
    fillEncoding(inputPage, dataChunk);
    fillMinMaxIndex(inputPage, dataChunk);
    fillLegacyFields(dataChunk);
    return dataChunk;
  }

  private void fillBasicFields(ColumnPage inputPage, DataChunk2 dataChunk) {
    dataChunk.setChunk_meta(CarbonMetadataUtil.getSnappyChunkCompressionMeta());
    dataChunk.setNumberOfRowsInpage(inputPage.getPageSize());
    dataChunk.setRowMajor(false);
  }

  private void fillNullBitSet(ColumnPage inputPage, DataChunk2 dataChunk) {
    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setPresent_bit_streamIsSet(true);
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    presenceMeta.setPresent_bit_stream(
        compressor.compressByte(inputPage.getNullBits().toByteArray()));
    dataChunk.setPresence(presenceMeta);
  }

  private void fillEncoding(ColumnPage inputPage, DataChunk2 dataChunk) throws IOException {
    dataChunk.setEncoders(getEncodingList());
    dataChunk.setEncoder_meta(buildEncoderMeta(inputPage));
  }

  private List<ByteBuffer> buildEncoderMeta(ColumnPage inputPage) throws IOException {
    ColumnPageEncoderMeta meta = getEncoderMeta(inputPage);
    List<ByteBuffer> metaDatas = new ArrayList<>();
    if (meta != null) {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(stream);
      meta.write(out);
      metaDatas.add(ByteBuffer.wrap(stream.toByteArray()));
    }
    return metaDatas;
  }

  private void fillMinMaxIndex(ColumnPage inputPage, DataChunk2 dataChunk) {
    dataChunk.setMin_max(buildMinMaxIndex(inputPage));
  }

  private BlockletMinMaxIndex buildMinMaxIndex(ColumnPage inputPage) {
    BlockletMinMaxIndex index = new BlockletMinMaxIndex();
    byte[] bytes = CarbonUtil.getValueAsBytes(
        inputPage.getDataType(), inputPage.getStatistics().getMax());
    ByteBuffer max = ByteBuffer.wrap(
        bytes);
    ByteBuffer min = ByteBuffer.wrap(
        CarbonUtil.getValueAsBytes(inputPage.getDataType(), inputPage.getStatistics().getMin()));
    index.addToMax_values(max);
    index.addToMin_values(min);
    return index;
  }

  /**
   * `buildPageMetadata` will call this for backward compatibility
   */
  protected void fillLegacyFields(DataChunk2 dataChunk)
      throws IOException {
    // Subclass should override this to update datachunk2 if any backward compatibility if required,
    // For example, when using IndexStorageCodec, rle_page_length and rowid_page_length need to be
    // updated
  }

  /**
   * Apply encoding algorithm for complex column page and return the coded data
   * TODO: remove this interface after complex column page is unified with column page
   */
  public static EncodedColumnPage[] encodeComplexColumn(ComplexColumnPage input)
      throws IOException, MemoryException {
    EncodedColumnPage[] encodedPages = new EncodedColumnPage[input.getDepth()];
    int index = 0;
    Iterator<byte[][]> iterator = input.iterator();
    while (iterator.hasNext()) {
      byte[][] subColumnPage = iterator.next();
      encodedPages[index++] = encodeChildColumn(subColumnPage);
    }
    return encodedPages;
  }

  private static EncodedColumnPage encodeChildColumn(byte[][] data)
      throws IOException, MemoryException {
    ColumnPage page = ColumnPage.wrapByteArrayPage(data);
    ColumnPageEncoder encoder = new DirectCompressCodec(DataType.BYTE_ARRAY).createEncoder(null);
    return encoder.encode(page);
  }

}
