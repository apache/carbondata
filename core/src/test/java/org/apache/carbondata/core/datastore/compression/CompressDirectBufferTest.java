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

import static junit.framework.TestCase.assertEquals;

import org.apache.carbondata.core.util.ByteUtil;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;

public class CompressDirectBufferTest {
  private static ByteBuffer uncompressedContentByteBuffer;
  private static byte[] uncompressedContentByteArray;
  private static Logger _logger = Logger.getLogger(CompressDirectBufferTest.class);

  @BeforeClass
  public static void setUp() throws Exception  {
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < 20; ++i) {
      s.append("Hello world!");
    }
    String uncompressedContent = s.toString();
    uncompressedContentByteArray = uncompressedContent.getBytes();
    uncompressedContentByteBuffer = ByteBuffer.allocateDirect(uncompressedContentByteArray.length);
    uncompressedContentByteBuffer.put(uncompressedContentByteArray);
    uncompressedContentByteBuffer.flip();

    _logger.info("input size: " + uncompressedContentByteArray.length);
    assertEquals(0, uncompressedContentByteBuffer.position());
    assertEquals(uncompressedContentByteArray.length, uncompressedContentByteBuffer.remaining());
    assertEquals(uncompressedContentByteArray.length, uncompressedContentByteBuffer.limit());
  }

  private static void testCompressDirectBuffer(String compressorName) throws Exception {
    // 1. init compressor
    Compressor compressor = CompressorFactory.getInstance().getCompressor(compressorName);

    // 2. compress direct byte buffer using snappy
    uncompressedContentByteBuffer.rewind();
    ByteBuffer compressedContentByteBuffer = compressor.compressByte(uncompressedContentByteBuffer);

    // 3. convert bytebuffer to bytearray
    byte[] compressedContentByteArray;
    if (compressedContentByteBuffer.isDirect()) {
      compressedContentByteArray = new byte[compressedContentByteBuffer.remaining()];
      compressedContentByteBuffer.get(compressedContentByteArray);
    } else {
      compressedContentByteArray = compressedContentByteBuffer.array();
    }

    // 3. decompress
    byte[] decompressedContentByteArray = compressor.unCompressByte(compressedContentByteArray);

    // 4. verify the correctness of compression and decompression
    assertEquals(uncompressedContentByteArray.length, decompressedContentByteArray.length);
    assertEquals(ByteUtil.compare(uncompressedContentByteArray, decompressedContentByteArray), 0);
    _logger.debug("uncompressed length: " + decompressedContentByteArray.length);
  }

  @Test
  public void testCompressDirectBufferUsingSnappy() throws Exception {
    testCompressDirectBuffer("SNAPPY");
  }

  @Test
  public void testCompressDirectBufferUsingZstd() throws Exception {
    testCompressDirectBuffer("ZSTD");
  }

  @Test
  public void testCompressDirectBufferUsingGzip() throws Exception {
    testCompressDirectBuffer("GZIP");
  }
}