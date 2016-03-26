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
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.query.datastorage.storeInterfaces.DataStore;
import org.carbondata.query.datastorage.storeInterfaces.DataStoreBlock;
import org.carbondata.query.datastorage.storeInterfaces.KeyValue;

public abstract class BTreeScanner implements Scanner {

    /**
     *
     */
    protected DataStoreBlock block;

    /**
     *
     */
    protected DataStore store;

    /**
     *
     */
    protected int index = -1;

    /**
     *
     */
    protected byte[] endKey;

    /**
     *
     */
    protected KeyValue currKey;

    /**
     *
     */
    protected KeyGenerator keyGenerator;

    /**
     *
     */
    //    protected int[] msrs;

    /**
     *
     */
    protected FileHolder fileHolder;
    /**
     *
     */
    protected int blockKeys;

    public BTreeScanner(byte[] startKey, byte[] endKey, KeyGenerator keyGenerator, KeyValue currKey,
            int[] msrs, FileHolder fileHolder) {
        this.endKey = endKey;
        this.keyGenerator = keyGenerator;
        currKey.setKeyLength(keyGenerator.getKeySizeInBytes());
        currKey.setMsrCols(msrs);
        this.currKey = currKey;
        //        this.msrs = msrs;
        this.fileHolder = fileHolder;
    }

    @Override
    public KeyValue getNext() {
        return currKey;
    }

    @Override
    public boolean isDone() {
        return !hasNext();
    }

    /**
     * Should set the currKey so that after calling of hasNext when getNext() is
     * called new KeyValue pair is provided
     *
     * @return true if there is any value is present in the tree
     */
    protected abstract boolean hasNext();

    @Override
    public void setDataStore(DataStore dataStore, DataStoreBlock block, int currIndex) {
        this.store = dataStore;
        this.block = block;
        if (block != null) {
            blockKeys = block.getnKeys() - 1;
            currKey.setBlock(block, fileHolder);
            currKey.reset();
            currKey.setRow(currIndex);
            currKey.setValueLength(block.getValueSize());
        }
        this.index = currIndex;
    }

    public FileHolder getFileHolder() {
        return fileHolder;
    }
    /*
    public static void main(String args[])
    {
        Object test = null;
        long beginTime = System.currentTimeMillis();
        for(long i = 0;i < 40000000l;i++)
        {
            if(test != null)
            {

            }
        }
        System.out.println((System.currentTimeMillis() - beginTime));
    }*/

}
