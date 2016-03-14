/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfQVwqh74rUY6n+OZ2pUrkn1TkkvO60rFu08DZa
JnQq9HokyaDKDOdxh6ZKaphzZ1Ol8YzJqZUPfJ3NCaVEFchRmUA9ltqQgRZF8UwgOEB4zenO
4PyBKw4+wvbj/VUioAxPAw/0DxH6pzF4GpL9YW7t91oIZptVq/MeiM5ejbyBPQ==*/
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
package com.huawei.unibi.molap.engine.scanner;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStore;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.DataStoreBlock;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;

/**
 * Abstract implementation for Btree scanner
 * 
 * @author R00900208
 * 
 */
public abstract class BTreeScanner implements Scanner
{

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
    public BTreeScanner(byte[] startKey, byte[] endKey, KeyGenerator keyGenerator, KeyValue currKey, int[] msrs,
           FileHolder fileHolder)
    {
        this.endKey = endKey;
        this.keyGenerator = keyGenerator;
        currKey.setKeyLength(keyGenerator.getKeySizeInBytes());
        currKey.setMsrCols(msrs);
        this.currKey = currKey;
//        this.msrs = msrs;
        this.fileHolder = fileHolder;
    }

    @Override
    public KeyValue getNext()
    {
        return currKey;
    }

    @Override
    public boolean isDone()
    {
        return !hasNext();
    }

    /**
     * Should set the currKey so that after calling of hasNext when getNext() is
     * called new KeyValue pair is provided
     * 
     * @return true if there is any value is present in the tree
     * 
     */
    protected abstract boolean hasNext();

    /**
     * 
     */
    protected int blockKeys;

    @Override
    public void setDataStore(DataStore dataStore, DataStoreBlock block, int currIndex)
    {
        this.store = dataStore;
        this.block = block;
        if(block != null)
        {
            blockKeys = block.getnKeys()-1;
            currKey.setBlock(block,fileHolder);
            currKey.reset();
            currKey.setRow(currIndex);
            currKey.setValueLength(block.getValueSize());
        }
        this.index = currIndex;
    }
    
    public FileHolder getFileHolder()
    {
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
