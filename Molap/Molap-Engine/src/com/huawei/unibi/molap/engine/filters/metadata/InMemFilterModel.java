/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3eWblnmBNcQ6Udxx0zywDGKXerFT+sNLvxXdr8oLXXJH7QN++fWkte+YHxC/ClI2HAhC
oa+UHVHeoAzNboQHmdq5JDKPaBXYGovwlshCgJouIdbTweKNVY6BaHsle+YeqA==*/
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
package com.huawei.unibi.molap.engine.filters.metadata;

/**
 * Parallel Implementation for FilterModel including other members required for
 * exclude filters
 * 
 * @author N71324
 */
public class InMemFilterModel extends FilterModel
{
    /**
     * 
     */
    private static final long serialVersionUID = 2779048394090554034L;

    public InMemFilterModel()
    {
        super(null, null, 0);
    }

    public InMemFilterModel(byte[][][] filter, byte[][] maxKey, int maxSize)
    {
        super(filter, maxKey, maxSize);
    }

    /**
     * 3D byte array 1 : Dimension index 2 : Filter index 3 : byte array of the
     * filter surrogate value Comment for <code>excludeFilter</code>
     * 
     */
    private byte[][][] excludeFilter;
    
    /**
     * includeFilterOr
     */
    private byte[][][] includeFilterOr;

    /**
     * 2D Long array 1 : Dimension index 2 : Array of surrogate filterValues
     * Comment for <code>includePredicateKeys</code>
     * 
     */
    private long[][] includePredicateKeys;

    /**
     * 
     */
    private long[][] excludePredicateKeys;
    
    /**
     * includePredicateKeysOr
     */
    private long[][] includePredicateKeysOr;

    /**
     * Key Index/ordinal for the dimensions having filter Comment for
     * <code>colIncludeDimOffset</code>
     * 
     */
    private int[] colIncludeDimOffset;

    /**
     * 
     */
    private int[] colExcludeDimOffset;
    
    /**
     * colIncludeDimOffsetOr
     */
    private int[] colIncludeDimOffsetOr;
    
    /**
     * 
     */
    private byte[][] maxKeyExclude;
    
    /**
     * maxKeyIncludeOr
     */
    private byte[][] maxKeyIncludeOr;

    /**
     * maskedByteRanges
     */
    private int[][] maskedByteRanges;
    
    /**
     * maskedByteRangesExclude
     */
    private int[][] maskedByteRangesExclude;
       
    /**
     * maskedByteRangesExclude or
     */
    private int[][] maskedByteRangesIncludeOr;
    
    /**
     * Groups
     */
    private int[][] groups;
    
    /**
     * @return
     */
    public byte[][][] getExcludeFilter()
    {
        return excludeFilter;
    }

    /**
     * @param excludeFilter
     */
    public void setExcludeFilter(byte[][][] excludeFilter)
    {
        this.excludeFilter = excludeFilter;
    }

    /**
     * @return
     */
    public long[][] getIncludePredicateKeys()
    {
        return includePredicateKeys;
    }

    /**
     * @param includePredicateKeys
     */
    public void setIncludePredicateKeys(long[][] includePredicateKeys)
    {
        this.includePredicateKeys = includePredicateKeys;
    }

    /**
     * @return
     */
    public long[][] getExcludePredicateKeys()
    {
        return excludePredicateKeys;
    }

    /**
     * @param excludePredicateKeys
     */
    public void setExcludePredicateKeys(long[][] excludePredicateKeys)
    {
        this.excludePredicateKeys = excludePredicateKeys;
    }

    /**
     * @return
     */
    public int[] getColIncludeDimOffset()
    {
        return colIncludeDimOffset;
    }

    /**
     * @param colIncludeDimOffset
     */
    public void setColIncludeDimOffset(int[] colIncludeDimOffset)
    {
        this.colIncludeDimOffset = colIncludeDimOffset;
    }

    /**
     * @return
     */
    public int[] getColExcludeDimOffset()
    {
        return colExcludeDimOffset;
    }

    /**
     * @param colExcludeDimOffset
     */
    public void setColExcludeDimOffset(int[] colExcludeDimOffset)
    {
        this.colExcludeDimOffset = colExcludeDimOffset;
    }

    /**
     * @return the maxKeyExclude
     */
    public byte[][] getMaxKeyExclude()
    {
        return maxKeyExclude;
    }

    /**
     * @param maxKeyExclude the maxKeyExclude to set
     */
    public void setMaxKeyExclude(byte[][] maxKeyExclude)
    {
        this.maxKeyExclude = maxKeyExclude;
    }

    /**
     * @return the maskedByteRanges
     */
    public int[][] getMaskedByteRanges()
    {
        return maskedByteRanges;
    }

    /**
     * @param maskedByteRanges the maskedByteRanges to set
     */
    public void setMaskedByteRanges(int[][] maskedByteRanges)
    {
        this.maskedByteRanges = maskedByteRanges;
    }

    /**
     * @return the maskedByteRangesExclude
     */
    public int[][] getMaskedByteRangesExclude()
    {
        return maskedByteRangesExclude;
    }

    /**
     * @param maskedByteRangesExclude the maskedByteRangesExclude to set
     */
    public void setMaskedByteRangesExclude(int[][] maskedByteRangesExclude)
    {
        this.maskedByteRangesExclude = maskedByteRangesExclude;
    }

    /**
     * @return the groups
     */
    public int[][] getGroups()
    {
        return groups;
    }

    /**
     * @param groups the groups to set
     */
    public void setGroups(int[][] groups)
    {
        this.groups = groups;
    }

    /**
     * @return the includeFilterOr
     */
    public byte[][][] getIncludeFilterOr()
    {
        return includeFilterOr;
    }

    /**
     * @param includeFilterOr the includeFilterOr to set
     */
    public void setIncludeFilterOr(byte[][][] includeFilterOr)
    {
        this.includeFilterOr = includeFilterOr;
    }

    /**
     * @return the includePredicateKeysOr
     */
    public long[][] getIncludePredicateKeysOr()
    {
        return includePredicateKeysOr;
    }

    /**
     * @param includePredicateKeysOr the includePredicateKeysOr to set
     */
    public void setIncludePredicateKeysOr(long[][] includePredicateKeysOr)
    {
        this.includePredicateKeysOr = includePredicateKeysOr;
    }

    /**
     * @return the colIncludeDimOffsetOr
     */
    public int[] getColIncludeDimOffsetOr()
    {
        return colIncludeDimOffsetOr;
    }

    /**
     * @param colIncludeDimOffsetOr the colIncludeDimOffsetOr to set
     */
    public void setColIncludeDimOffsetOr(int[] colIncludeDimOffsetOr)
    {
        this.colIncludeDimOffsetOr = colIncludeDimOffsetOr;
    }

    /**
     * @return the maxKeyIncludeOr
     */
    public byte[][] getMaxKeyIncludeOr()
    {
        return maxKeyIncludeOr;
    }

    /**
     * @param maxKeyIncludeOr the maxKeyIncludeOr to set
     */
    public void setMaxKeyIncludeOr(byte[][] maxKeyIncludeOr)
    {
        this.maxKeyIncludeOr = maxKeyIncludeOr;
    }

    /**
     * @return the maskedByteRangesIncludeOr
     */
    public int[][] getMaskedByteRangesIncludeOr()
    {
        return maskedByteRangesIncludeOr;
    }

    /**
     * @param maskedByteRangesIncludeOr the maskedByteRangesIncludeOr to set
     */
    public void setMaskedByteRangesIncludeOr(int[][] maskedByteRangesIncludeOr)
    {
        this.maskedByteRangesIncludeOr = maskedByteRangesIncludeOr;
    }
}