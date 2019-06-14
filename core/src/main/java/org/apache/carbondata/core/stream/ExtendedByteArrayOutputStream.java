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
package org.apache.carbondata.core.stream;

import java.io.ByteArrayOutputStream;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.CarbonUnsafe;

public class ExtendedByteArrayOutputStream extends ByteArrayOutputStream {

  public ExtendedByteArrayOutputStream() {

  }

  public ExtendedByteArrayOutputStream(int initialSize) {
    super(initialSize);
  }

  public byte[] getBuffer() {
    return buf;
  }

  public long copyToAddress() {
    final long address =
        CarbonUnsafe.getUnsafe().allocateMemory(CarbonCommonConstants.INT_SIZE_IN_BYTE + count);
    CarbonUnsafe.getUnsafe().putInt(address, count);
    CarbonUnsafe.getUnsafe().copyMemory(buf, CarbonUnsafe.BYTE_ARRAY_OFFSET, null,
        address + CarbonCommonConstants.INT_SIZE_IN_BYTE, count);
    return address;
  }
}
