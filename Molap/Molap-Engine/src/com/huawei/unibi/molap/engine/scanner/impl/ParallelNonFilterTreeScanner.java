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
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfQVwqh74rUY6n+OZ2pUrkn1TkkvO60rFu08DZa
JnQq9Ad8w54SlcHM15fM6xhXlv5mo86YdvvrE5IxxzP2QUV1Clbdn/eb4jmsgI6l0se1ToQX
dBtl1zLLe5CL6XbOumVrjWL2Ag4utTMyUiL/LxRu69KBa/f6aPEwRVZ8qfP0bA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.scanner.impl;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.scanner.BTreeScanner;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;

/**
 * This scanner is used when there is no query in the filter but is executed in
 * parallel. So this class does comparison for finite number of rows which is
 * configured.
 * 
 * @author N71324
 * 
 */
public class ParallelNonFilterTreeScanner extends BTreeScanner
{
    /**
     * 
     */
    private long numOfNodesToCompare;


    /**
     * 
     */
  //  private long currentNumOfNodes;

    public ParallelNonFilterTreeScanner(byte[] startKey, byte[] endKey, KeyGenerator keyGenerator,
            long numOfNodesToCompare, KeyValue currKey, int[] msrs, FileHolder fileHolder)
    {
        super(startKey, endKey, keyGenerator, currKey, msrs, fileHolder);
        this.numOfNodesToCompare = numOfNodesToCompare;
    }

    /**
     * @return
     */
    public Long getNumOfNodesToCompare()
    {
        return numOfNodesToCompare;
    }
    
    /**
     * This method will check whether data is there to be read.
     * @return true if the element is there to be read.
     * 
     */
    protected boolean hasNext()
    {
        // boolean hasNext = false;
        if(block == null)
        {
            return false;
        }
        // while(!hasNext)
        // {
        if(currKey.isReset)
        {
            currKey.setReset(false);
        }
        else if(index >= blockKeys)
        {
            block = block.getNext();
            index = 0;
            if(block == null)
            {
                return false;
            }
            blockKeys = block.getnKeys() - 1;
            currKey.setBlock(block,fileHolder);
            currKey.resetOffsets();
        }
        else
        {
            currKey.increment();
            index++;
        }

//        if(currentNumOfNodes >= numOfNodesToCompare)
//        {
//            // numOfNodesToCompare will not be null in case scanner is run in
//            // parallel
//            // for a slice
//            return false;
//        }
        if(endKey != null
                && keyGenerator.compare(currKey.backKeyArray, currKey.keyOffset, currKey.keyLength, endKey,
                        0, endKey.length) > 0)
        {
            // endKey will not be null in case filter is present or scanner
            // is run parallel for a slice
            return false;
        }

        // got the desired element break now, but increment the index so
        // that scanner reads the next key when hasnext is called again;
        // hasNext = true;

//        currentNumOfNodes++;
        // break;
        // }
        return true;
    }

}
