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

package org.apache.carbondata.sdk.file.arrow;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.TimeZone;

import org.apache.carbondata.core.stream.ExtendedByteArrayOutputStream;
import org.apache.carbondata.sdk.file.Schema;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

public class ArrowConverter {

  private final BufferAllocator allocator;
  private VectorSchemaRoot root;
  private ArrowWriter arrowWriter;
  private org.apache.arrow.vector.types.pojo.Schema arrowSchema;
  private ExtendedByteArrayOutputStream out;
  private ArrowFileWriter writer;

  public ArrowConverter(Schema schema, int initialSize) {
    this.arrowSchema = ArrowUtils.toArrowSchema(schema, TimeZone.getDefault().getID());
    this.allocator =
        ArrowUtils.rootAllocator.newChildAllocator("toArrowBuffer", initialSize, Long.MAX_VALUE);
    this.root = VectorSchemaRoot.create(arrowSchema, allocator);
    this.arrowWriter = ArrowWriter.create(root);
    // currently blocklet level read and set initial value to 32 MB.
    this.out = new ExtendedByteArrayOutputStream(32 * 1024 * 1024);
    this.writer = new ArrowFileWriter(root, null, Channels.newChannel(out));
  }

  /**
   * write batch of row objects to Arrow vectors
   *
   * @param data
   */
  public void addToArrowBuffer(Object[] data) {
    int i = 0;
    while (i < data.length) {
      arrowWriter.write((Object[]) data[i]);
      i += 1;
    }
  }

  /**
   * To serialize arrow vectors to byte[]
   *
   * @return
   * @throws IOException
   */
  public byte[] toSerializeArray() throws IOException {
    arrowWriter.finish();
    writer.writeBatch();
    writer.close();
    arrowWriter.reset();
    root.close();
    byte[] bytes = out.toByteArray();
    allocator.close();
    out.close();
    return bytes;
  }

  /**
   * To copy arrow vectors to unsafe memory
   *
   * @return
   * @throws IOException
   */
  public long copySerializeArrayToOffHeap() throws IOException {
    arrowWriter.finish();
    writer.writeBatch();
    writer.close();
    arrowWriter.reset();
    root.close();
    long address = out.copyToAddress();
    allocator.close();
    out.close();
    return address;
  }

  /**
   * Utility API to convert back the arrow byte[] to arrow ArrowRecordBatch.
   * User need to close the ArrowRecordBatch after usage by calling ArrowRecordBatch.close()
   *
   * @param batchBytes input byte array
   * @param bufferAllocator arrow buffer allocator
   * @return ArrowRecordBatch
   * @throws IOException
   */
  public static ArrowRecordBatch byteArrayToArrowBatch(byte[] batchBytes,
      BufferAllocator bufferAllocator)
      throws IOException {
    ByteArrayReadableSeekableByteChannel in = new ByteArrayReadableSeekableByteChannel(batchBytes);
    ArrowFileReader reader = new ArrowFileReader(in, bufferAllocator);
    try {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      VectorUnloader unloader = new VectorUnloader(root);
      reader.loadNextBatch();
      return unloader.getRecordBatch();
    } finally {
      reader.close();
    }
  }

  /**
   * To get the arrow vectors directly after filling from carbondata
   *
   * @return Arrow VectorSchemaRoot. which contains array of arrow vectors.
   */
  public VectorSchemaRoot getArrowVectors() throws IOException {
    arrowWriter.finish();
    writer.writeBatch();
    writer.close();
    return root;
  }
}
