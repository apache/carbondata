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

/**
 * This compressor actually will not compress or decompress anything.
 * It is used for speed up the data loading by skip the compression.
 */

public class NoneCompressor extends AbstractCompressor {

    @Override
    public String getName() {
        return "none";
    }

    @Override
    public byte[] compressByte(byte[] unCompInput) {
        return unCompInput;
    }

    @Override
    public byte[] compressByte(byte[] unCompInput, int byteSize) {
        return unCompInput;
    }

    @Override
    public byte[] unCompressByte(byte[] compInput) {
        return compInput;
    }

    @Override
    public byte[] unCompressByte(byte[] compInput, int offset, int length) {
        return compInput;
    }

    @Override
    public long rawUncompress(byte[] input, byte[] output) throws IOException {
        throw new RuntimeException("Not implemented rawCompress for noneCompressor yet");
    }

    @Override
    public long maxCompressedLength(long inputSize) {
        return inputSize;
    }

    @Override
    public int unCompressedLength(byte[] data, int offset, int length) {
        throw new RuntimeException("Unsupported operation Exception");
    }

    @Override
    public int rawUncompress(byte[] data, int offset, int length, byte[] output) {
        throw new RuntimeException("Not implemented rawCompress for noneCompressor yet");
    }
}
