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

package org.apache.carbondata.hadoop.stream;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.BlockletHeader;

/**
 * stream blocklet reader
 */
public class StreamBlockletReader {

  private byte[] buffer;
  private int offset;
  private final byte[] syncMarker;
  private final byte[] syncBuffer;
  private final int syncLen;
  private long pos = 0;
  private final InputStream in;
  private final long limitStart;
  private final long limitEnd;
  private boolean isAlreadySync = false;
  private Compressor compressor;
  private int rowNums = 0;
  private int rowIndex = 0;
  private boolean isHeaderPresent;

  public StreamBlockletReader(byte[] syncMarker, InputStream in, long limit,
      boolean isHeaderPresent, String compressorName) {
    this.syncMarker = syncMarker;
    syncLen = syncMarker.length;
    syncBuffer = new byte[syncLen];
    this.in = in;
    limitStart = limit;
    limitEnd = limitStart + syncLen;
    this.isHeaderPresent = isHeaderPresent;
    this.compressor = CompressorFactory.getInstance().getCompressor(compressorName);
  }

  private void ensureCapacity(int capacity) {
    if (buffer == null || capacity > buffer.length) {
      buffer = new byte[capacity];
    }
  }

  /**
   * find the first position of sync_marker in input stream
   */
  private boolean sync() throws IOException {
    if (!readBytesFromStream(syncBuffer, 0, syncLen)) {
      return false;
    }
    boolean skipHeader = false;
    for (int i = 0; i < limitStart; i++) {
      int j = 0;
      for (; j < syncLen; j++) {
        if (syncMarker[j] != syncBuffer[(i + j) % syncLen]) break;
      }
      if (syncLen == j) {
        if (isHeaderPresent) {
          if (skipHeader) {
            return true;
          } else {
            skipHeader = true;
          }
        } else {
          return true;
        }
      }
      int value = in.read();
      if (-1 == value) {
        return false;
      }
      syncBuffer[i % syncLen] = (byte) value;
      pos++;
    }
    return false;
  }

  public BlockletHeader readBlockletHeader() throws IOException {
    int len = readIntFromStream();
    byte[] b = new byte[len];
    if (!readBytesFromStream(b, 0, len)) {
      throw new EOFException("Failed to read blocklet header");
    }
    BlockletHeader header = CarbonUtil.readBlockletHeader(b);
    rowNums = header.getBlocklet_info().getNum_rows();
    rowIndex = 0;
    return header;
  }

  public void readBlockletData(BlockletHeader header) throws IOException {
    ensureCapacity(header.getBlocklet_length());
    offset = 0;
    int len = readIntFromStream();
    byte[] b = new byte[len];
    if (!readBytesFromStream(b, 0, len)) {
      throw new EOFException("Failed to read blocklet data");
    }
    compressor.rawUncompress(b, buffer);
  }

  public void skipBlockletData(boolean reset) throws IOException {
    int len = readIntFromStream();
    skip(len);
    pos += len;
    if (reset) {
      this.rowNums = 0;
      this.rowIndex = 0;
    }
  }

  private void skip(int len) throws IOException {
    long remaining = len;
    do {
      long skipLen = in.skip(remaining);
      remaining -= skipLen;
    } while (remaining > 0);
  }

  /**
   * find the next blocklet
   */
  public boolean nextBlocklet() throws IOException {
    if (pos >= limitStart) {
      return false;
    }
    if (isAlreadySync) {
      if (!readBytesFromStream(syncBuffer, 0, syncLen)) {
        return false;
      }
    } else {
      isAlreadySync = true;
      if (!sync()) {
        return false;
      }
    }

    return pos < limitEnd;
  }

  public boolean hasNext() throws IOException {
    return rowIndex < rowNums;
  }

  public void nextRow() {
    rowIndex++;
  }

  public int readIntFromStream() throws IOException {
    int ch1 = in.read();
    int ch2 = in.read();
    int ch3 = in.read();
    int ch4 = in.read();
    if ((ch1 | ch2 | ch3 | ch4) < 0) throw new EOFException();
    pos += 4;
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  /**
   * Reads <code>len</code> bytes of data from the input stream into
   * an array of bytes.
   * @return <code>true</code> if reading data successfully, or
   * <code>false</code> if there is no more data because the end of the stream has been reached.
   */
  public boolean readBytesFromStream(byte[] b, int offset, int len) throws IOException {
    int readLen = in.read(b, offset, len);
    if (readLen < 0) {
      return false;
    }
    pos += readLen;
    if (readLen < len) {
      return readBytesFromStream(b, offset + readLen, len - readLen);
    } else {
      return true;
    }
  }

  public boolean readBoolean() {
    return (buffer[offset++]) != 0;
  }

  public short readShort() {
    short v =  (short) ((buffer[offset + 1] & 255) +
        ((buffer[offset]) << 8));
    offset += 2;
    return v;
  }

  public byte[] copy(int len) {
    byte[] b = new byte[len];
    System.arraycopy(buffer, offset, b, 0, len);
    return b;
  }

  public int readInt() {
    int v = ((buffer[offset + 3] & 255) +
        ((buffer[offset + 2] & 255) << 8) +
        ((buffer[offset + 1] & 255) << 16) +
        ((buffer[offset]) << 24));
    offset += 4;
    return v;
  }

  public long readLong() {
    long v = ((long)(buffer[offset + 7] & 255)) +
        ((long) (buffer[offset + 6] & 255) << 8) +
        ((long) (buffer[offset + 5] & 255) << 16) +
        ((long) (buffer[offset + 4] & 255) << 24) +
        ((long) (buffer[offset + 3] & 255) << 32) +
        ((long) (buffer[offset + 2] & 255) << 40) +
        ((long) (buffer[offset + 1] & 255) << 48) +
        ((long) (buffer[offset]) << 56);
    offset += 8;
    return v;
  }

  public double readDouble() {
    return Double.longBitsToDouble(readLong());
  }

  public byte[] readBytes(int len) {
    byte[] b = new byte[len];
    System.arraycopy(buffer, offset, b, 0, len);
    offset += len;
    return b;
  }

  public void skipBytes(int len) {
    offset += len;
  }

  public int getRowNums() {
    return rowNums;
  }

  public void close() {
    CarbonUtil.closeStreams(in);
  }
}
