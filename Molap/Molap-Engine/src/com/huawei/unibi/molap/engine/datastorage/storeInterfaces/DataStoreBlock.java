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

/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdh/HjOjN0Brs7b7TRorj6S6iAIeaqK90lj7BAM
GSGxBlG+ojQOTUi5OsxrimNc3toy3wEBLRwcRkrxiumlBnZqjZfqtzlivtBVgI8BkLqhHuTO
pElwwGVIU3dnZBREMH3xDzb/1H1SwAFBCneo17C+redNynD+J+HDesGU5WmVXg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.datastorage.storeInterfaces;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.MeasureDataWrapper;
import com.huawei.unibi.molap.datastorage.store.columnar.ColumnarKeyStoreDataHolder;

/**
 * @author R00900208
 * 
 */
public interface DataStoreBlock
{

    KeyValue getNextKeyValue(int index);

    DataStoreBlock getNext();

    int getnKeys();

    byte[] getBackKeyArray(FileHolder fileHolder);

    ColumnarKeyStoreDataHolder[] getColumnarKeyStore(FileHolder fileHolder, int[] blockIndex,
            boolean[] needCompressedData);

    ColumnarKeyStoreDataHolder getColumnarKeyStore(FileHolder fileHolder, int blockIndex, boolean needCompressedData);

    MeasureDataWrapper getNodeMsrDataWrapper(int[] cols, FileHolder fileHolder);

    MeasureDataWrapper getNodeMsrDataWrapper(int cols, FileHolder fileHolder);

    short getValueSize();

    long getNodeNumber();
    
    /**
     * This will give maximum value of given column
     * @param colIndex
     * @return
     */
    byte[][] getBlockMaxData();
    
    /**
     * It will give minimum value of given column
     * @param colIndex
     * @return
     */
    byte[][] getBlockMinData();

}
