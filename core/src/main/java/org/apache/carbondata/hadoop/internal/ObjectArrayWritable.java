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

package org.apache.carbondata.hadoop.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;

/**
 * A Object sequence that is usable as a key or value.
 */
public class ObjectArrayWritable implements Writable {
  private Object[] values;

  public void set(Object[] values) {
    this.values = values;
  }

  public Object[] get() {
    return values;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    values = new Object[length];
    for (int i = 0; i < length; i++) {
      byte[] b = new byte[in.readInt()];
      in.readFully(b);
      values[i] = new String(b, Charset.defaultCharset());
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(values.length);                 // write values
    for (int i = 0; i < values.length; i++) {
      byte[] b = values[i].toString().getBytes(Charset.defaultCharset());
      out.writeInt(b.length);
      out.write(b);
    }
  }

  @Override
  public String toString() {
    return Arrays.toString(values);
  }
}
