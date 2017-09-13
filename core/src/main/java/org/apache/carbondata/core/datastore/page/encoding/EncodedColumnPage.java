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

import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk2;

/**
 * An column page after encoding.
 */
public class EncodedColumnPage {

  // encoded and compressed column page data
  protected final byte[] encodedData;

  // metadata of this page
  private DataChunk2 pageMetadata;

  // stats of this page
  private SimpleStatsResult stats;

  private ColumnType columnType;

  /**
   * Constructor
   * @param pageMetadata metadata of the encoded page
   * @param encodedData encoded data for this page
   */
  public EncodedColumnPage(ColumnType columnType, DataChunk2 pageMetadata, byte[] encodedData,
      SimpleStatsResult stats) {
    if (pageMetadata == null) {
      throw new IllegalArgumentException("data chunk2 must not be null");
    }
    if (encodedData == null) {
      throw new IllegalArgumentException("encoded data must not be null");
    }
    this.columnType = columnType;
    this.pageMetadata = pageMetadata;
    this.encodedData = encodedData;
    this.stats = stats;
  }

  /**
   * return the encoded data as ByteBuffer
   */
  public ByteBuffer getEncodedData() {
    return ByteBuffer.wrap(encodedData);
  }

  public DataChunk2 getPageMetadata() {
    return pageMetadata;
  }

  /**
   * Return the total size of serialized data and metadata
   */
  public int getTotalSerializedSize() {
    int metadataSize = CarbonUtil.getByteArray(pageMetadata).length;
    int dataSize = encodedData.length;
    return metadataSize + dataSize;
  }

  public SimpleStatsResult getStats() {
    return stats;
  }

  public ColumnType getColumnType() {
    return columnType;
  }
}