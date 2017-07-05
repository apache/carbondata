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

package org.apache.carbondata.core.datastore.page.encoding;

import java.io.IOException;

import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * This codec directly apply compression on the input data
 */
public class DirectCompressCodec implements ColumnPageCodec {

  private Compressor compressor;
  private DataType dataType;

  private DirectCompressCodec(DataType dataType, Compressor compressor) {
    this.compressor = compressor;
    this.dataType = dataType;
  }

  public static DirectCompressCodec newInstance(DataType dataType, Compressor compressor) {
    return new DirectCompressCodec(dataType, compressor);
  }

  @Override
  public String getName() {
    return "DirectCompressCodec";
  }

  @Override
  public byte[] encode(ColumnPage input) throws IOException, MemoryException {
    return input.compress(compressor);
  }

  @Override
  public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
    return ColumnPage.decompress(compressor, dataType, input, offset, length);
  }
}
