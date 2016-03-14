/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/os2n9LNxuZM6tzag+BWpBdu58tb1PsVZksul+7lRoAOx7yjGWjVD99JaDdgYB8XwjwV
zqPnZQ0RqybzNo9822++nvqthwX4BYC0jUtzzXSImO8rEqEXyvz119M8g8MGEg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2014
 * =====================================
 */
package com.huawei.unibi.molap.surrogatekeysgenerator.lru;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Data Processor 
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :LRUCache.java 
 * Class Description : LRUCache class 
 * Version 1.0
 */
public final class LRUCache
{
    /**
     * instance
     */
    private static final LRUCache INSTANCE = new LRUCache();

    /**
     * cache size
     */
    private int lruCacheSize;

    /**
     * cache
     */
    private Map<String, MolapSeqGenCacheHolder> cache;

    /**
     * LRUCache constructor
     */
    private LRUCache()
    {
        try
        {
            lruCacheSize = Integer
                    .parseInt(MolapProperties
                            .getInstance()
                            .getProperty(
                                    MolapCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE,
                                    MolapCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE_DEFAULT_VALUE));
        }
        catch(NumberFormatException e)
        {
            lruCacheSize = Integer
                    .parseInt(MolapCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE_DEFAULT_VALUE);
        }
        createCache();
    }

    public static LRUCache getIntance()
    {
        return INSTANCE;
    }

    /**
     * below method will be used to create the cache 
     */
    private void createCache()
    {
        cache = Collections.synchronizedMap(
        // true = use access order instead of insertion order
                new LinkedHashMap<String, MolapSeqGenCacheHolder>(lruCacheSize+1,
                        1.0f, true)
                {
                    //CHECKSTYLE:OFF    Approval No:Approval-398
                    /**
                     * serialVersionUID
                     */
                    private static final long serialVersionUID = 1L;
                    //CHECKSTYLE:ON
                    /**
                     * size
                     */
//                    private int size;

                    @Override
                    public boolean removeEldestEntry(
                            Map.Entry<String, MolapSeqGenCacheHolder> eldest)
                    {
                        if(size() > lruCacheSize)
                        {
                            cache.remove(eldest.getKey());
                            return true;
                        }
                        // when to remove the eldest entry
                        return false; // size exceeded the max allowed
                    }
                    @Override
                    public MolapSeqGenCacheHolder get(Object key)
                    {
                        MolapSeqGenCacheHolder m = super.get(key);
                        if(null!=m)
                        {
                            m.setLastAccessTime(System.currentTimeMillis());
                        }
                        return m;
                    }
                });
    }
    
    /**
     * below method will be used to put the data into the cache 
     * @param key
     * @param value
     */
    public void put(String key, MolapSeqGenCacheHolder value)
    {
         value.setLastAccessTime(System.currentTimeMillis());
         cache.put(key, value);
    }
    
    /**
     * below method will be used to get the data from the cache
     * @param key
     * @return
     */
    public MolapSeqGenCacheHolder get(String key)
    {
        return cache.get(key);
    }
    
    /**
     * below method will be used to remove the entry from the cache 
     * @param key
     */
    public void remove(String key)
    {
        cache.remove(key);
    }
    
    public void flush()
    {
        cache.clear();
    }
}
