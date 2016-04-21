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

package org.carbondata.query.executer.pagination.lru;

public class LRUCacheValue {

  private long size;

  private long rowCount;

  private LRUCacheKey cacheKey;

  /**
   * @return the size
   */
  public long getSize() {
    return size;
  }

  /**
   * @param size the size to set
   */
  public void setSize(long size) {
    this.size = size;
  }

  /**
   * @return the rowCount
   */
  public long getRowCount() {
    return rowCount;
  }

  /**
   * @param rowCount the rowCount to set
   */
  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  /**
   * @return the cacheKey
   */
  public LRUCacheKey getCacheKey() {
    return cacheKey;
  }

  /**
   * @param cacheKey the cacheKey to set
   */
  public void setCacheKey(LRUCacheKey cacheKey) {
    this.cacheKey = cacheKey;
  }

}
