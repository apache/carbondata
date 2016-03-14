/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfQVwqh74rUY6n+OZ2pUrkn1TkkvO60rFu08DZa
JnQq9HMZppXR6FKgK1mJSicGLJhbrEhZcW5SIEXH5w9PqgP7T48o4UZdFd6x70/+rsBmuJ6p
Pw4rxD/fRpGxx+o0mE1txdc0I+vcBgwyHDQxZBArAI3MyO+m/nco3fDD3Z8Png==*/
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
 * 
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :16-May-2013 10:39:48 PM
 * FileName : NonFilterTreeScanner.java
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
