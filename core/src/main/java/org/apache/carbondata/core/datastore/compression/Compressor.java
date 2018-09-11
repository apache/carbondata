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

public interface Compressor {

  String getName();

  byte[] compressByte(byte[] unCompInput);

  byte[] compressByte(byte[] unCompInput, int byteSize);

  byte[] unCompressByte(byte[] compInput);

  byte[] unCompressByte(byte[] compInput, int offset, int length);

  byte[] compressShort(short[] unCompInput);

  short[] unCompressShort(byte[] compInput, int offset, int length);

  byte[] compressInt(int[] unCompInput);

  int[] unCompressInt(byte[] compInput, int offset, int length);

  byte[] compressLong(long[] unCompInput);

  long[] unCompressLong(byte[] compInput, int offset, int length);

  byte[] compressFloat(float[] unCompInput);

  float[] unCompressFloat(byte[] compInput, int offset, int length);

  byte[] compressDouble(double[] unCompInput);

  double[] unCompressDouble(byte[] compInput, int offset, int length);

  long rawCompress(long inputAddress, int inputSize, long outputAddress) throws IOException;

  long rawUncompress(byte[] input, byte[] output) throws IOException;

  long maxCompressedLength(long inputSize);

  /**
   * Whether this compressor support zero-copy during compression.
   * Zero-copy means that the compressor support receiving memory address (pointer)
   * and returning result in memory address (pointer).
   * Currently not all java version of the compressors support this feature.
   * @return true if it supports, otherwise return false
   */
  boolean supportUnsafe();
}
