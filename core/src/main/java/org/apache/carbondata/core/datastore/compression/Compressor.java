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

import java.io.IOException;
import java.nio.ByteBuffer;

public interface Compressor {

  String getName();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1371

  ByteBuffer compressByte(ByteBuffer compInput);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3731

  ByteBuffer compressByte(byte[] unCompInput);

  byte[] compressByte(byte[] unCompInput, int byteSize);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1572

  byte[] unCompressByte(byte[] compInput);

  byte[] unCompressByte(byte[] compInput, int offset, int length);

  ByteBuffer compressShort(short[] unCompInput);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3731

  short[] unCompressShort(byte[] compInput, int offset, int length);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852

  ByteBuffer compressInt(int[] unCompInput);

  int[] unCompressInt(byte[] compInput, int offset, int length);

  ByteBuffer compressLong(long[] unCompInput);

  long[] unCompressLong(byte[] compInput, int offset, int length);

  ByteBuffer compressFloat(float[] unCompInput);

  float[] unCompressFloat(byte[] compInput, int offset, int length);

  ByteBuffer compressDouble(double[] unCompInput);

  double[] unCompressDouble(byte[] compInput, int offset, int length);

  long rawCompress(long inputAddress, int inputSize, long outputAddress) throws IOException;

  long rawUncompress(byte[] input, byte[] output) throws IOException;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1572

  long maxCompressedLength(long inputSize);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2851
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2852

  /**
   * Whether this compressor support zero-copy during compression.
   * Zero-copy means that the compressor support receiving memory address (pointer)
   * and returning result in memory address (pointer).
   * Currently not all java version of the compressors support this feature.
   * @return true if it supports, otherwise return false
   */
  boolean supportUnsafe();

  int unCompressedLength(byte[] data, int offset, int length);

  int rawUncompress(byte[] data, int offset, int length, byte[] output);

  boolean supportReusableBuffer();
}
