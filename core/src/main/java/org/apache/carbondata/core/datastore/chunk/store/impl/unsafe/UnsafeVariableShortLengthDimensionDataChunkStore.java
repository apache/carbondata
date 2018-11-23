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

package org.apache.carbondata.core.datastore.chunk.store.impl.unsafe;

import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * Below class is responsible to store variable length dimension data chunk in
 * memory Memory occupied can be on heap or offheap using unsafe interface
 */
public class UnsafeVariableShortLengthDimensionDataChunkStore
    extends UnsafeVariableLengthDimensionDataChunkStore {
  public UnsafeVariableShortLengthDimensionDataChunkStore(long totalSize, boolean isInvertedIdex,
      int numberOfRows, int dataLength) {
    super(totalSize, isInvertedIdex, numberOfRows, dataLength);
  }

  @Override
  protected int getLengthSize() {
    return CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
  }

  @Override
  protected int getLengthFromBuffer(ByteBuffer byteBuffer) {
    return byteBuffer.getShort();
  }
}
