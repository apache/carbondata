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

import org.apache.carbondata.format.DataChunk2;

/**
 * An column page after encoding and compression.
 */
public abstract class EncodedColumnPage {

  // number of row of this page
  protected int pageSize;

  // encoded and compressed column page data
  protected byte[] encodedData;

  // metadata of this page
  protected DataChunk2 dataChunk2;

  EncodedColumnPage(int pageSize, byte[] encodedData) {
    this.pageSize = pageSize;
    this.encodedData = encodedData;
  }

  public abstract DataChunk2 buildDataChunk2();

  /**
   * return the encoded and compressed data page
   */
  public byte[] getEncodedData() {
    return encodedData;
  }

  /**
   * return the size of the s
   */
  public int getSerializedSize() {
    return encodedData.length;
  }

  public ByteBuffer serialize() {
    return ByteBuffer.wrap(encodedData);
  }

  public DataChunk2 getDataChunk2() {
    return dataChunk2;
  }

}