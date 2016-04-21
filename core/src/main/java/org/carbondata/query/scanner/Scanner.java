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

package org.carbondata.query.scanner;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.query.datastorage.storeinterface.DataStore;
import org.carbondata.query.datastorage.storeinterface.DataStoreBlock;
import org.carbondata.query.datastorage.storeinterface.KeyValue;

/**
 * Scanner is the interface provide the way to scan the data source
 */
public interface Scanner {

  /**
   * It requires isDone to be called before calling this interface
   *
   * @return
   */
  KeyValue getNext();

  /**
   * It ensures the next valid key is ready to be returned on call of getNext
   *
   * @return true only when the scan is done
   */
  boolean isDone();

  /**
   * Set the data store and data store block information.
   *
   * @param dataStore
   * @param block
   * @param currIndex
   */
  void setDataStore(DataStore dataStore, DataStoreBlock block, int currIndex);

  /**
   * Get file holder
   *
   * @return file holder
   */
  FileHolder getFileHolder();

}
