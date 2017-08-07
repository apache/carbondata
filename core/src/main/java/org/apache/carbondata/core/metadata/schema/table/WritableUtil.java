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

package org.apache.carbondata.core.metadata.schema.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableUtil {

  public static void writeByteArray(DataOutput out, byte[] bytes) throws IOException {
    if (bytes == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }

  public static byte[] readByteArray(DataInput in) throws IOException {
    int length = in.readInt();
    if (length == -1) {
      return null;
    } else {
      byte[] b = new byte[length];
      in.readFully(b);
      return b;
    }
  }
}
