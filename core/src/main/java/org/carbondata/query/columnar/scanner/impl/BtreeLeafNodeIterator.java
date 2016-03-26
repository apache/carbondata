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

package org.carbondata.query.columnar.scanner.impl;

import org.carbondata.core.iterator.MolapIterator;
import org.carbondata.query.datastorage.storeInterfaces.DataStoreBlock;

public class BtreeLeafNodeIterator implements MolapIterator<DataStoreBlock> {
    /**
     * data store block
     */
    protected DataStoreBlock datablock;
    private int blockCounter;
    private boolean hasNext = true;
    private long totalNumberOfBlocksToScan;

    public BtreeLeafNodeIterator(DataStoreBlock datablock, long totalNumberOfBlocksToScan) {
        this.datablock = datablock;
        this.totalNumberOfBlocksToScan = totalNumberOfBlocksToScan;
    }

    @Override public boolean hasNext() {
        return hasNext;
    }

    @Override public DataStoreBlock next() {
        DataStoreBlock datablockTemp = datablock;
        datablock = datablock.getNext();
        blockCounter++;
        if (null == datablock || blockCounter >= this.totalNumberOfBlocksToScan) {
            hasNext = false;
        }
        return datablockTemp;
    }
}
