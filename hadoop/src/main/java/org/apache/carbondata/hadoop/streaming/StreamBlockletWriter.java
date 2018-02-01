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

package org.apache.carbondata.hadoop.streaming;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.BlockletHeader;
import org.apache.carbondata.format.BlockletInfo;
import org.apache.carbondata.format.MutationType;
import org.apache.carbondata.hadoop.api.CarbonStreamOutputFormat;

/**
 * stream blocklet writer
 */
public class StreamBlockletWriter {
  private byte[] buffer;
  private int maxSize;
  private int maxRowNum;
  private int rowSize;
  private int count = 0;
  private int rowIndex = -1;
  private Compressor compressor = CompressorFactory.getInstance().getCompressor();

  StreamBlockletWriter(int maxSize, int maxRowNum, int rowSize) {
    buffer = new byte[maxSize];
    this.maxSize = maxSize;
    this.maxRowNum = maxRowNum;
    this.rowSize = rowSize;
  }

  private void ensureCapacity(int space) {
    int newcount = space + count;
    if (newcount > buffer.length) {
      byte[] newbuf = new byte[Math.max(newcount, buffer.length + rowSize)];
      System.arraycopy(buffer, 0, newbuf, 0, count);
      buffer = newbuf;
    }
  }

  void reset() {
    count = 0;
    rowIndex = -1;
  }

  byte[] getBytes() {
    return buffer;
  }

  int getCount() {
    return count;
  }

  int getRowIndex() {
    return rowIndex;
  }

  void nextRow() {
    rowIndex++;
  }

  boolean isFull() {
    return rowIndex == maxRowNum || count >= maxSize;
  }

  void writeBoolean(boolean val) {
    ensureCapacity(1);
    buffer[count] = (byte) (val ? 1 : 0);
    count += 1;
  }

  void writeShort(int val) {
    ensureCapacity(2);
    buffer[count + 1] = (byte) (val);
    buffer[count] = (byte) (val >>> 8);
    count += 2;
  }

  void writeInt(int val) {
    ensureCapacity(4);
    buffer[count + 3] = (byte) (val);
    buffer[count + 2] = (byte) (val >>> 8);
    buffer[count + 1] = (byte) (val >>> 16);
    buffer[count] = (byte) (val >>> 24);
    count += 4;
  }

  void writeLong(long val) {
    ensureCapacity(8);
    buffer[count + 7] = (byte) (val);
    buffer[count + 6] = (byte) (val >>> 8);
    buffer[count + 5] = (byte) (val >>> 16);
    buffer[count + 4] = (byte) (val >>> 24);
    buffer[count + 3] = (byte) (val >>> 32);
    buffer[count + 2] = (byte) (val >>> 40);
    buffer[count + 1] = (byte) (val >>> 48);
    buffer[count] = (byte) (val >>> 56);
    count += 8;
  }

  void writeDouble(double val) {
    writeLong(Double.doubleToLongBits(val));
  }

  void writeBytes(byte[] b) {
    writeBytes(b, 0, b.length);
  }

  void writeBytes(byte[] b, int off, int len) {
    ensureCapacity(len);
    System.arraycopy(b, off, buffer, count, len);
    count += len;
  }

  void appendBlocklet(DataOutputStream outputStream) throws IOException {
    outputStream.write(CarbonStreamOutputFormat.getSyncMarker());

    BlockletInfo blockletInfo = new BlockletInfo();
    blockletInfo.setNum_rows(getRowIndex() + 1);
    BlockletHeader blockletHeader = new BlockletHeader();
    blockletHeader.setBlocklet_length(getCount());
    blockletHeader.setMutation(MutationType.INSERT);
    blockletHeader.setBlocklet_info(blockletInfo);
    byte[] headerBytes = CarbonUtil.getByteArray(blockletHeader);
    outputStream.writeInt(headerBytes.length);
    outputStream.write(headerBytes);

    byte[] compressed = compressor.compressByte(getBytes(), getCount());
    outputStream.writeInt(compressed.length);
    outputStream.write(compressed);
  }

  void close() {
  }
}
