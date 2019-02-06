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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.TimeZone;

import org.apache.carbondata.sdk.file.Schema;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

public class ArrowConverter {

  private final BufferAllocator allocator;
  private VectorSchemaRoot root;
  private ArrowWriter arrowWriter;
  private org.apache.arrow.vector.types.pojo.Schema arrowSchema;
  private ByteArrayOutputStream out;
  private ArrowFileWriter writer;

  public ArrowConverter(Schema schema, int initalSize) {
    this.arrowSchema = ArrowUtils.toArrowSchema(schema, TimeZone.getDefault().getID());
    this.allocator =
        ArrowUtils.rootAllocator.newChildAllocator("toArrowBuffer", initalSize, Long.MAX_VALUE);
    this.root = VectorSchemaRoot.create(arrowSchema, allocator);
    this.arrowWriter = ArrowWriter.create(root);
    this.out = new ByteArrayOutputStream();
    this.writer = new ArrowFileWriter(root, null, Channels.newChannel(out));
  }

  public void addToArrowBuffer(Object[] data) {
    int i = 0;
    while (i < data.length) {
      arrowWriter.write((Object[]) data[i]);
      i += 1;
    }
  }

  public byte[] toSerializeArray() throws IOException {
    arrowWriter.finish();
    writer.writeBatch();
    this.writer.close();
    arrowWriter.reset();
    writer.close();
    this.root.close();
    return out.toByteArray();
  }

  public void close() {
    //    this.root.close();
    //    this.arrowWriter.finish();
    //    this.allocator.close();
  }
}
