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

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * This codec convert input data into byte array without any compression
 */
public class SimpleCodec implements ColumnPageCodec {

  private SimpleCodec() {
  }

  public static SimpleCodec newInstance() {
    return new SimpleCodec();
  }

  @Override
  public String getName() {
    return "SimpleCodec";
  }

  @Override
  public byte[] encode(ColumnPage input) {
    double[] data = input.getDoublePage();
    byte[] out = new byte[data.length * 8];
    for (int i = 0; i < data.length; i++) {
      System.arraycopy(ByteUtil.toBytes(data[i]), 0, out, i * 8, 8);
    }
    return out;
  }

  @Override
  public ColumnPage decode(byte[] input, int offset, int length) throws MemoryException {
    double[] data = new double[length / 8];
    for (int i = 0; i < data.length; i++) {
      data[i] = ByteUtil.toDouble(input, offset + i * 8);
    }
    return ColumnPage.newDoublePage(data);
  }
}
