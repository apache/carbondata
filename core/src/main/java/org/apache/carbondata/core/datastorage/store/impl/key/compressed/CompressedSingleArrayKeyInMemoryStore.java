/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.datastorage.store.impl.key.compressed;

import org.apache.carbondata.core.datastorage.store.FileHolder;

public class CompressedSingleArrayKeyInMemoryStore extends AbstractCompressedSingleArrayStore {
  /**
   * @param size
   * @param elementSize
   */
  public CompressedSingleArrayKeyInMemoryStore(int size, int elementSize) {
    super(size, elementSize);
  }

  /**
   * @param size
   * @param elementSize
   * @param offset
   * @param filePath
   * @param fileHolder
   * @param length
   */
  public CompressedSingleArrayKeyInMemoryStore(int size, int elementSize, long offset,
      String filePath, FileHolder fileHolder, int length) {
    this(size, elementSize);
    datastore = fileHolder.readByteArray(filePath, offset, length);
  }
}
