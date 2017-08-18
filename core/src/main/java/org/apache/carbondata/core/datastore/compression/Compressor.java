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

  byte[] unCompressByte(byte[] compInput);

  byte[] unCompressByte(byte[] compInput, int offset, int length);

  byte[] compressShort(short[] unCompInput);

  short[] unCompressShort(byte[] compInput);

  short[] unCompressShort(byte[] compInput, int offset, int lenght);

  byte[] compressInt(int[] unCompInput);

  int[] unCompressInt(byte[] compInput);

  int[] unCompressInt(byte[] compInput, int offset, int length);

  byte[] compressLong(long[] unCompInput);

  long[] unCompressLong(byte[] compInput);

  long[] unCompressLong(byte[] compInput, int offset, int length);

  byte[] compressFloat(float[] unCompInput);

  float[] unCompressFloat(byte[] compInput);

  float[] unCompressFloat(byte[] compInput, int offset, int length);

  byte[] compressDouble(double[] unCompInput);

  double[] unCompressDouble(byte[] compInput);

  double[] unCompressDouble(byte[] compInput, int offset, int length);

  long rawCompress(long inputAddress, int inputSize, long outputAddress) throws IOException;

  int maxCompressedLength(int inputSize);
}
