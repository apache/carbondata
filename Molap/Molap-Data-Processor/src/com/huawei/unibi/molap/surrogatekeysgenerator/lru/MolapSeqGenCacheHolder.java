/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcfJtSMNYgnOYiEQwbS13nxM8hk/dmbY4B4u+tG
aRAl/lo0gdOd2bV4b00rp5Iuy83AirE1udZDUYxMDaGBnRcoMalbXD092+0Q+HA4SHxR1WQO
RWpVi4a3Y2huSidO3/1Y3D4TxmyAltGmv1gHDDmsiz6cRZoe261ELL4B9XLwvg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.surrogatekeysgenerator.lru;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.HashMap;
import java.util.Map;

import com.huawei.unibi.molap.schema.metadata.ArrayWrapper;

public class MolapSeqGenCacheHolder
{
    /**
     * Cache should be map only. because, multiple levels can map to same
     * database column. This case duplicate storage should be avoided.
     */
    private Map<String, Map<String, Integer>> memberCache;
    
    /**
     * Year Cache
     */
    private Map<String, Map<String, Integer>> timeDimCache;
    
    /**
     * max
     */
    private int[] max;
    
    /**
     * timeDimMax
     */
    private int[] timDimMax;
    
    /**
     * lastAccessTime
     */
    private long lastAccessTime;
    
    /**
     * hierCache
     */
    private Map<String, Int2ObjectMap<int[]>> hierCache = new HashMap<String, Int2ObjectMap<int[]>>(16);
    
    /**
     * hierCacheReverse
     */
    private Map<String, Map<ArrayWrapper,Integer>> hierCacheReverse = new HashMap<String, Map<ArrayWrapper,Integer>>(16);
    
    /**
     * Measure max surrogate key map
     */
    protected Map<String ,Integer> measureMaxSurroagetMap;
    
    /**
     * @return the memberCache
     */
    public Map<String, Map<String, Integer>> getMemberCache()
    {
        return memberCache;
    }

    /**
     * @param memberCache the memberCache to set
     */
    public void setMemberCache(Map<String, Map<String, Integer>> memberCache)
    {
        this.memberCache = memberCache;
    }

    /**
     * @return the timeDimCache
     */
    public Map<String, Map<String, Integer>> getTimeDimCache()
    {
        return timeDimCache;
    }

    /**
     * @param timeDimCache the timeDimCache to set
     */
    public void setTimeDimCache(
            Map<String, Map<String, Integer>> timeDimCache)
    {
        this.timeDimCache = timeDimCache;
    }

    /**
     * @return the max
     */
    public int[] getMax()
    {
        return max;
    }

    /**
     * @param max the max to set
     */
    public void setMax(int[] max)
    {
        this.max = max;
    }

    /**
     * @return the timDimMax
     */
    public int[] getTimDimMax()
    {
        return timDimMax;
    }

    /**
     * @param timDimMax the timDimMax to set
     */
    public void setTimDimMax(int[] timDimMax)
    {
        this.timDimMax = timDimMax;
    }

    /**
     * @return the hierCache
     */
    public Map<String, Int2ObjectMap<int[]>> getHierCache()
    {
        return hierCache;
    }

    /**
     * @param hierCache the hierCache to set
     */
    public void setHierCache(Map<String, Int2ObjectMap<int[]>> hierCache)
    {
        this.hierCache = hierCache;
    }

    /**
     * @return the hierCacheReverse
     */
    public Map<String, Map<ArrayWrapper, Integer>> getHierCacheReverse()
    {
        return hierCacheReverse;
    }

    /**
     * @param hierCacheReverse the hierCacheReverse to set
     */
    public void setHierCacheReverse(
            Map<String, Map<ArrayWrapper, Integer>> hierCacheReverse)
    {
        this.hierCacheReverse = hierCacheReverse;
    }

    /**
     * @return the lastAccessTime
     */
    public long getLastAccessTime()
    {
        return lastAccessTime;
    }

    /**
     * @param lastAccessTime the lastAccessTime to set
     */
    public void setLastAccessTime(long lastAccessTime)
    {
        this.lastAccessTime = lastAccessTime;
    }

    /**
     * @return the measureMaxSurroagetMap
     */
    public Map<String, Integer> getMeasureMaxSurroagetMap()
    {
        return measureMaxSurroagetMap;
    }

    /**
     * @param measureMaxSurroagetMap the measureMaxSurroagetMap to set
     */
    public void setMeasureMaxSurroagetMap(
            Map<String, Integer> measureMaxSurroagetMap)
    {
        this.measureMaxSurroagetMap = measureMaxSurroagetMap;
    }
}
