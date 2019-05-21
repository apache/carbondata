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

import org.apache.carbondata.sdk.file.Schema;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
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
    this.writer.close();
    arrowWriter.reset();
    writer.close();
    this.root.close();
    return out.toByteArray();
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
    this.writer.close();
    arrowWriter.reset();
    writer.close();
    this.root.close();
    return out.copyToAddress();
  }

  /**
   * Utility API to convert back the arrow byte[] to arrow VectorSchemaRoot.
   *
   * @param batchBytes
   * @return
   * @throws IOException
   */
  public VectorSchemaRoot byteArrayToVector(byte[] batchBytes) throws IOException {
    ByteArrayReadableSeekableByteChannel in = new ByteArrayReadableSeekableByteChannel(batchBytes);
    ArrowFileReader reader = new ArrowFileReader(in, allocator);
    try {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      VectorUnloader unloader = new VectorUnloader(root);
      reader.loadNextBatch();
      VectorSchemaRoot arrowRoot = VectorSchemaRoot.create(arrowSchema, allocator);
      VectorLoader vectorLoader = new VectorLoader(arrowRoot);
      vectorLoader.load(unloader.getRecordBatch());
      return arrowRoot;
    } catch (IOException e) {
      reader.close();
      throw e;
    }
  }

  /**
   * To get the arrow vectors directly after filling from carbondata
   *
   * @return
   */
  public VectorSchemaRoot getArrowVectors() throws IOException {
    arrowWriter.finish();
    writer.writeBatch();
    this.writer.close();
    writer.close();
    return root;
  }
}
