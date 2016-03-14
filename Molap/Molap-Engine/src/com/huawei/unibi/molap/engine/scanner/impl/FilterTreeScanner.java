/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwfQVwqh74rUY6n+OZ2pUrkn1TkkvO60rFu08DZa
JnQq9PBPR+wIF7NCuys82h/tYziiwf6tBl4oi36wl19BAoBdf0dsBJSWsEO3Sd62JCJ3c3pU
hf71GI5yjjxX3/fQgeFZL0tAGq5pvkb3oBoFrDRdvjIbSYrgpZf6+hFgqE1/DQ==*/
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

//import org.apache.log4j.Logger;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;

import com.huawei.unibi.molap.datastorage.store.FileHolder;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.filters.InMemoryFilter;
import com.huawei.unibi.molap.engine.scanner.BTreeScanner;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;

/**
 * 
 * Project Name NSE V3R7C00 
 * Module Name : 
 * Author V00900840
 * Created Date :16-May-2013 10:43:09 PM
 * FileName : FilterTreeScanner.java
 * This class will be used to scan data store based on filter present in the query
 * Class Description :
 * Version 1.0
 */
public class FilterTreeScanner extends BTreeScanner
{
    /**
     * 
     */
    protected InMemoryFilter filter;

    /**
     * 
     */
    protected boolean filterPresent;
    
    private boolean smartJump;
    
    /**
     * 
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(FilterTreeScanner.class.getName());

    public FilterTreeScanner(byte[] startKey, byte[] endKey, KeyGenerator keyGenerator, KeyValue currKey, int[] msrs,
            FileHolder fileHolder, boolean smartJump)
    {
        super(startKey, endKey, keyGenerator, currKey, msrs, fileHolder);
        this.smartJump = smartJump;
    }

    /**
     * This method will check whether data is there to be read.
     * @return true if the element is there to be read.
     * 
     */
    protected boolean hasNext()
    {
        boolean hasNext = false;
        if(block == null)
        {
            return false;
        }
        while(!hasNext)
        {
            if(currKey.isReset())
            {
                currKey.setReset(false);
            }
            else if(index >= blockKeys)
            {
                block = block.getNext();
                index = 0;
                if(block == null)
                {
                    break;
                }
                blockKeys = block.getnKeys() - 1;
                currKey.setBlock(block, fileHolder);
                currKey.resetOffsets();
            }
            else
            {
                currKey.increment();
                index++;
            }

            // currKey = block.getNextKeyValue(index);
            // if(currKey == null)
            // {
            // break;
            // }

            if(endKey != null
                    && keyGenerator.compare(currKey.getArray(), currKey.getKeyOffset(), currKey.getKeyLength(), endKey,
                            0, endKey.length) > 0)
            {
                // endKey will not be null in case filter is present or scanner
                // is run parallel for a slice
                break;
            }
            if(!filter.filterKey(currKey))
            {
                if(smartJump)
                {
                    byte[] key = filter.getNextJump(currKey);
                    if(key != null)
                    {
                        // Check style fix
                        if(!searchInternal(key))
                        {
                            return false;
                        }
                    }
                    else if(index < blockKeys)
                    {
                        index++;
                        currKey.increment();
                        currKey.setReset(true);
                    }
                }
                continue;
            }
            // got the desired element break now, but increment the index so
            // that scanner reads the next key when hasnext is called again;
            hasNext = true;
            break;
        }
        return hasNext;
    }

    /**
     * @param key
     * @return
     */
    private boolean searchInternal(byte[] key)
    {
        int pos = currKey.searchInternal(key, keyGenerator);
        if(pos < 0)
        {
            store.getNext(key, this);
            if(block == null)
            {
                // if the search for the jump key doesn't return any
                // block then there is no more parsing
                return false;
            }
        }
        else
        {
            index = pos;
            currKey.setRow(index);
            currKey.setReset(true);
        }
        return true;
    }


    /**
     * @param filter
     */
    public void setFilter(InMemoryFilter filter)
    {
        this.filter = filter;
        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "BTreeScanner: filterPresent = " + filterPresent);
    }

}
