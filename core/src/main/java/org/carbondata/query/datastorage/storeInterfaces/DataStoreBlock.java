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

package org.carbondata.query.datastorage.storeInterfaces;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.MeasureDataWrapper;
import org.carbondata.core.datastorage.store.columnar.ColumnarKeyStoreDataHolder;

public interface DataStoreBlock {

    KeyValue getNextKeyValue(int index);

    DataStoreBlock getNext();

    int getnKeys();

    byte[] getBackKeyArray(FileHolder fileHolder);

    ColumnarKeyStoreDataHolder[] getColumnarKeyStore(FileHolder fileHolder, int[] blockIndex,
            boolean[] needCompressedData,int[] noDictionaryColIndexes);

    ColumnarKeyStoreDataHolder getColumnarKeyStore(FileHolder fileHolder, int blockIndex,
            boolean needCompressedData,int[] noDictionaryColIndexes);

    MeasureDataWrapper getNodeMsrDataWrapper(int[] cols, FileHolder fileHolder);

    MeasureDataWrapper getNodeMsrDataWrapper(int cols, FileHolder fileHolder);

    short getValueSize();

    long getNodeNumber();

    /**
     * This will give maximum value of given column
     *
     * @param colIndex
     * @return
     */
    byte[][] getBlockMaxData();

    /**
     * It will give minimum value of given column
     *
     * @param colIndex
     * @return
     */
    byte[][] getBlockMinData();

}
