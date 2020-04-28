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

package org.apache.carbondata.sdk.file.cache;

import org.apache.carbondata.core.cache.Cacheable;

/**
 * A Cacheable object containing all the rows in a blocklet
 */
public class BlockletRows implements Cacheable {

  // rows in this blocklet
  private Object[] rows;

  // this is considering other bloklets. This index represents total rows till previous blocklet.
  private long rowIdStartIndex;

  // size of blocklet in bytes
  private long blockletSizeInBytes;

  public BlockletRows(long rowIdStartIndex, long blockletSizeInBytes, Object[] rows) {
    this.rowIdStartIndex = rowIdStartIndex;
    this.blockletSizeInBytes = blockletSizeInBytes;
    this.rows = rows;
  }

  @Override
  public int getAccessCount() {
    return 0;
  }

  @Override
  public long getMemorySize() {
    return blockletSizeInBytes;
  }

  @Override
  public void invalidate() {
    // TODO: not applicable ?
  }

  public Object[] getRows() {
    return rows;
  }

  public int getRowsCount() {
    return rows.length;
  }

  public long getRowIdStartIndex() {
    return rowIdStartIndex;
  }

}