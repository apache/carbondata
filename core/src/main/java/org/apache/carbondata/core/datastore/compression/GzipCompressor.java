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

package org.apache.carbondata.core.datastore.compression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

/**
 * Codec Class for performing Gzip Compression
 */
public class GzipCompressor extends AbstractCompressor {

  @Override public String getName() {
    return "gzip";
  }

  /**
   * This method takes the Byte Array data and Compresses in gzip format
   *
   * @param data Data Byte Array passed for compression
   * @return Compressed Byte Array
   */
  private byte[] compressData(byte[] data) {
    int initialSize = (data.length / 2) == 0 ? data.length : data.length / 2;
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(initialSize);
    try {
      GzipCompressorOutputStream gzipCompressorOutputStream =
          new GzipCompressorOutputStream(byteArrayOutputStream);
      try {
        /**
         * Below api will write bytes from specified byte array to the gzipCompressorOutputStream
         * The output stream will compress the given byte array.
         */
        gzipCompressorOutputStream.write(data);
      } catch (IOException e) {
        throw new RuntimeException("Error during Compression writing step ", e);
      } finally {
        gzipCompressorOutputStream.close();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error during Compression step ", e);
    }
    return byteArrayOutputStream.toByteArray();
  }

  /**
   * This method takes the Byte Array data and Decompresses in gzip format
   *
   * @param data   Data Byte Array for Compression
   * @param offset Start value of Data Byte Array
   * @param length Size of Byte Array
   * @return
   */
  private byte[] decompressData(byte[] data, int offset, int length) {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data, offset, length);
    ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
    try {
      GzipCompressorInputStream gzipCompressorInputStream =
          new GzipCompressorInputStream(byteArrayInputStream);
      int initialSize = (data.length * 2) < Integer.MAX_VALUE ? (data.length * 2) : data.length;
      byte[] buffer = new byte[initialSize];
      int len;
      /**
       * Reads the next byte of the data from the input stream and stores them into buffer
       * Data is then read from the buffer and put into byteOutputStream from a offset.
       */
      while ((len = gzipCompressorInputStream.read(buffer)) != -1) {
        byteOutputStream.write(buffer, 0, len);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error during Decompression step ", e);
    }
    return byteOutputStream.toByteArray();
  }

  @Override public byte[] compressByte(byte[] unCompInput) {
    return compressData(unCompInput);
  }

  @Override public byte[] compressByte(byte[] unCompInput, int byteSize) {
    return compressData(unCompInput);
  }

  @Override public byte[] unCompressByte(byte[] compInput) {
    return decompressData(compInput, 0, compInput.length);
  }

  @Override public byte[] unCompressByte(byte[] compInput, int offset, int length) {
    return decompressData(compInput, offset, length);
  }

  @Override public long rawUncompress(byte[] input, byte[] output) {
    //gzip api doesnt have rawUncompress yet.
    throw new RuntimeException("Not implemented rawUcompress for gzip yet");
  }

  @Override public long maxCompressedLength(long inputSize) {
    // Check if input size is lower than the max possible size
    if (inputSize < Integer.MAX_VALUE) {
      return inputSize;
    } else {
      throw new RuntimeException("compress input oversize for gzip");
    }
  }

  @Override public int unCompressedLength(byte[] data, int offset, int length) {
    //gzip api doesnt have UncompressedLength
    throw new RuntimeException("Unsupported operation Exception");
  }

  @Override public int rawUncompress(byte[] data, int offset, int length, byte[] output) {
    //gzip api doesnt have rawUncompress yet.
    throw new RuntimeException("Not implemented rawUcompress for gzip yet");
  }
}
