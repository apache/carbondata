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

package org.apache.carbondata.core.datastore.page.encoding.directstring;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.metadata.schema.table.Writable;

public class DirectStringEncoderMeta extends ColumnPageEncoderMeta implements Writable {

  private short[] lengthOfString;
  private String compressorName;

  public DirectStringEncoderMeta() {
  }

  public DirectStringEncoderMeta(TableSpec.ColumnSpec columnSpec, short[] lengthOfString,
      String compressorName) {
    super(columnSpec, columnSpec.getSchemaDataType(), null);
    this.lengthOfString = lengthOfString;
    this.compressorName = compressorName;
  }

  public short[] getLengthOfString() {
    return lengthOfString;
  }

  public String getCompressorName() {
    return compressorName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    // TODO: use RLE to compress lengthOfString instead of compressor
    Compressor compressor = CompressorFactory.getInstance().getCompressor();
    byte[] compressed = compressor.compressShort(lengthOfString);
    out.writeInt(compressed.length);
    out.write(compressed);
    out.writeUTF(compressorName);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    int length = in.readInt();
    byte[] compressed = new byte[length];
    in.readFully(compressed);
    this.compressorName = in.readUTF();
    Compressor compressor = CompressorFactory.getInstance().getCompressor(compressorName);
    this.lengthOfString = compressor.unCompressShort(compressed);
  }
}
