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
GSGxBqJpDcEkXsi7zPFjWj3BhWELoLyB/iyIxrxhchK/TkoHm/1Ryiy+kzK2AuOlRvIr1cAT
AA5wiEVQGyOBBrREZqC6zRaKzWcG27sUNDJ+92bkaYK4mUFdUF3k1yMJdNXUYg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.datastorage.storeInterfaces;

import java.util.List;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.datastorage.store.compression.ValueCompressionModel;
import com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream;
import com.huawei.unibi.molap.engine.scanner.Scanner;
import com.huawei.unibi.molap.metadata.MolapMetadata.Cube;

/**
 * @author R00900208
 * 
 */
public interface DataStore
{

    KeyValue get(byte[] key, Scanner scanner);

    KeyValue getNext(byte[] key, Scanner scanner);
    
    DataStoreBlock getBlock(byte[] startKey, FileHolder fileHolderImpl, boolean isFirst);
    
//    public KeyValue getNext(byte[] key);

    /*
     * public Scanner<K, V> getFilterScanner(K startKey,K endKey);
     * 
     * public Scanner<K, V> getNonFilterScanner(K startKey,K endKey);
     */

    // public void insertSortedData(K key,V value);

//    public void insert(byte[] key, double[] value);

    long size();

    long getRangeSplitValue();

   // public void build(DataInputStream factStream, boolean hasFactCount);

    void build(List<DataInputStream> factStream, boolean hasFactCount);
    
    void buildColumnar(List<DataInputStream> factStream, boolean hasFactCount,Cube cube);

    //public void build(List<DataInputStream> factStreams, List<String> aggregateNames, boolean hasFact_count);

    /**
     * Gives different ranges based on number of keys
     * 
     * @return
     */
    long[][] getRanges();

    ValueCompressionModel getCompressionModel();
    
    void build(DataInputStream factStream, boolean hasFactCount);
}
