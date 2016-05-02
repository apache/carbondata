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
package org.carbondata.processing.store.colgroup;

import java.util.concurrent.Callable;

import org.carbondata.core.datastorage.store.columnar.IndexStorage;

/**
 * it is holder of row blocks data and also evaluate min max for row block data
 */
public class RowBlockStorage implements IndexStorage, Callable<IndexStorage> {

  private RowStoreDataHolder rowStoreDataHolder;

  public RowBlockStorage(DataHolder rowStoreDataHolder) {
    this.rowStoreDataHolder = (RowStoreDataHolder) rowStoreDataHolder;
  }

  /**
   * sorting is not required for row block storage and hence return true
   */
  @Override public boolean isAlreadySorted() {
    return true;
  }

  /**
   * for row block storage its not required
   */
  @Override public RowStoreDataHolder getDataAfterComp() {
    //not required for Row store
    return null;
  }

  /**
   * for row block storage its not required
   */
  @Override public RowStoreDataHolder getIndexMap() {
    // not required for row store
    return null;
  }

  /**
   * for row block storage its not required
   */
  @Override public byte[][] getKeyBlock() {
    return rowStoreDataHolder.getData();
  }

  /**
   * for row block storage its not required
   */
  @Override public RowStoreDataHolder getDataIndexMap() {
    //not required for row store
    return null;
  }

  /**
   * for row block storage its not required
   */
  @Override public int getTotalSize() {
    return rowStoreDataHolder.getTotalSize();
  }

  /**
   * Get min max of row block store
   */
  @Override public byte[] getMin() {
    return rowStoreDataHolder.getMin();
  }

  @Override public byte[] getMax() {
    return rowStoreDataHolder.getMax();
  }

  /**
   * return
   */
  @Override public IndexStorage call() throws Exception {
    return this;
  }
}