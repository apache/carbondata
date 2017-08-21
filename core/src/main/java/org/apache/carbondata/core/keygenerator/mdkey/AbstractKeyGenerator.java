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

package org.apache.carbondata.core.keygenerator.mdkey;

import org.apache.carbondata.core.keygenerator.KeyGenerator;

public abstract class AbstractKeyGenerator implements KeyGenerator {

  private static final long serialVersionUID = -6675293078575359769L;

  @Override public int compare(byte[] byte1, byte[] byte2) {
    // Short circuit equal case
    if (byte1 == byte2) {
      return 0;
    }
    // Bring WritableComparator code local
    int i = 0;
    int j = 0;
    for (; i < byte1.length && j < byte2.length; i++, j++) {
      int a = (byte1[i] & 0xff);
      int b = (byte2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return 0;
  }

  public int compare(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
      int length2) {
    length1 += offset1;
    length2 += offset2;
    // Bring WritableComparator code local
    for (; offset1 < length1 && offset2 < length2; offset1++, offset2++) {
      int a = (buffer1[offset1] & 0xff);
      int b = (buffer2[offset2] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return 0;
  }

  @Override public void setProperty(Object key, Object value) {
    /*
     * No implementation required.
     */
  }

  @Override public int getKeySizeInBytes() {
    return 0;
  }

  @Override public int getDimCount() {
    return 0;
  }

}
