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

package org.apache.carbondata.core.datastorage.store;

public interface NodeKeyStore {
  /**
   * This method will be used to get the actual mdkeys array present in the
   * store store
   *
   * @param fileHolder
   * @return mdkey
   */
  byte[] getBackArray(FileHolder fileHolder);

  /**
   * This method will be used to insert mdkey to store
   *
   * @param index index of mdkey
   * @param value mdkey
   */
  void put(int index, byte[] value);

  /**
   * This method will be used to get the writable key array.
   * writable key array will hold below information:
   * <size of key array><key array>
   * total length will be 4 bytes for size + key array length
   *
   * @return writable array (compressed or normal)
   */
  byte[] getWritableKeyArray();

  /**
   * This method will be used to get the mdkkey array based on index
   *
   * @param index      index in store
   * @param fileHolder file holder will be used to read the file
   * @return mdkey
   */
  byte[] get(int index, FileHolder fileHolder);

  /**
   * This method will clear the store and create the new empty store
   */
  void clear();

}
