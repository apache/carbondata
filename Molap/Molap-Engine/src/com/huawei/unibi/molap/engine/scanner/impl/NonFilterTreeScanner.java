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

package com.huawei.unibi.molap.engine.scanner.impl;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.scanner.BTreeScanner;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;

/**
 * 
 * Class Description : This class will be used for scanning full data store
 * Version 1.0
 */
public class NonFilterTreeScanner extends BTreeScanner
{
    public NonFilterTreeScanner(byte[] startKey, byte[] endKey, KeyGenerator keyGenerator, KeyValue currKey,
            int[] msrs, FileHolder fileHolder)
    {
        super(startKey, endKey, keyGenerator, currKey, msrs, fileHolder);
    }
    
    //TODO SIMIAN
    /**
     * This method will check whether data is there to be read.
     * @return true if the element is there to be read.
     * 
     */
    protected boolean hasNext()
    {
        if(block == null)
        {
            return false;
        }
        if(currKey.isReset())
        {
            //
            currKey.setReset(false);
        }
        else if(index >= blockKeys)
        {
            index = 0;
            block = block.getNext();
            if(block == null)
            {
                return false;
            }
            //
            blockKeys = block.getnKeys() - 1;
            currKey.setBlock(block,fileHolder);
            currKey.resetOffsets();
        }
        else
        {
            currKey.increment();
            index++;
        }
        return true;
    }
}
