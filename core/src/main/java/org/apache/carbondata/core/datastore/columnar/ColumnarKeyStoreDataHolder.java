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

package org.apache.carbondata.core.datastore.columnar;

import java.nio.ByteBuffer;

public class ColumnarKeyStoreDataHolder {
  private byte[] keyblockData;
  private ColumnarKeyStoreMetadata columnarKeyStoreMetadata;

  public ColumnarKeyStoreDataHolder(final byte[] keyblockData,
      final ColumnarKeyStoreMetadata columnarKeyStoreMetadata) {
    this.keyblockData = keyblockData;
    this.columnarKeyStoreMetadata = columnarKeyStoreMetadata;
  }

  public ColumnarKeyStoreDataHolder(final ColumnarKeyStoreMetadata columnarKeyStoreMetadata) {
    this.columnarKeyStoreMetadata = columnarKeyStoreMetadata;
  }

  public int getSurrogateKey(int columnIndex) {
    byte[] actual = new byte[4];
    int startIndex;
    if (null != columnarKeyStoreMetadata.getColumnReverseIndex()) {
      startIndex =
          columnarKeyStoreMetadata.getColumnReverseIndex()[columnIndex] * columnarKeyStoreMetadata
              .getEachRowSize();
    } else {
      startIndex = columnIndex * columnarKeyStoreMetadata.getEachRowSize();
    }
    int destPos = 4 - columnarKeyStoreMetadata.getEachRowSize();
    System.arraycopy(keyblockData, startIndex, actual, destPos,
        columnarKeyStoreMetadata.getEachRowSize());
    return ByteBuffer.wrap(actual).getInt();
  }

}
