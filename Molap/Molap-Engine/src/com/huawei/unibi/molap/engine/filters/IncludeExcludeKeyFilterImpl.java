/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d1LaskW4lKMv79ja0idGxl5wCe7eMGc/XdulrDCRGvet8PFCeWBQiRjIM9vF2ikGBfGO
PZoKE0yJ7qLlAfaSyJlgu6rabpkRyOISurOjJZk2o+tklpcRTGY2nDkQU5VMVw==*/
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
package com.huawei.unibi.molap.engine.filters;

import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.filters.metadata.InMemFilterModel;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;

/**
 * Filter implementation to scan the data store based on exclude filter applied in the dimensions.
 * 
 */
public class IncludeExcludeKeyFilterImpl extends KeyFilterImpl
{
    
    private int[] dimensionOffsetExclude;
    
    private byte[][][] excludeFilters;
    
    private int[][] excludeRanges;
    
    private byte[][] maxKeyExclude;
    
    private int[] lengthsExc;
    
    private byte[][] tempsExc = null;
    
    public IncludeExcludeKeyFilterImpl(InMemFilterModel filterModel, KeyGenerator keyGenerator, long[] maxKey)
    {
        this.filterModel = filterModel;
        this.keyGenerator = keyGenerator;
        dimensionOffsetExclude = filterModel.getColExcludeDimOffset();
        excludeFilters = filterModel.getExcludeFilter();
        excludeRanges = filterModel.getMaskedByteRangesExclude();
        maxKeyExclude = filterModel.getMaxKeyExclude();
        
        dimensionOffset = filterModel.getColIncludeDimOffset();
        includeFilters = filterModel.getFilter();
        maxKeyBytes = filterModel.getMaxKey();
        ranges = filterModel.getMaskedByteRanges();
        temps = new byte[dimensionOffset.length][];
        lengths = new int[dimensionOffset.length];
        createTempByteAndRangeLengths(temps,lengths,dimensionOffset,ranges);        
        
        tempsExc = new byte[dimensionOffsetExclude.length][];
        lengthsExc = new int[dimensionOffsetExclude.length];
        createTempByteAndRangeLengths(tempsExc,lengthsExc,dimensionOffsetExclude,excludeRanges);
//        this.optimizer = new IncludeExcludeScanOptimizerImpl(maxKey, filterModel.getIncludePredicateKeys(),
//                filterModel.getExcludePredicateKeys(), keyGenerator,filterModel.getGroups());
    }

    /**
     * @see com.huawei.unibi.molap.engine.filters.KeyFilterImpl#filterKey(com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue)
     */
    @Override
    public boolean filterKey(KeyValue key)
    {
        //
        if(super.filterKey(key))
        {

            byte[][] notAllowedValuesForDim;
            for(int i = 0;i < dimensionOffsetExclude.length;i++)
            {
                //
                notAllowedValuesForDim = excludeFilters[dimensionOffsetExclude[i]];
                int searchresult = binarySearch(notAllowedValuesForDim, key.backKeyArray, key.keyOffset,
                        maxKeyExclude[dimensionOffsetExclude[i]], excludeRanges[dimensionOffsetExclude[i]],tempsExc[i],lengthsExc[i]);
                if(searchresult >= 0)
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    

    /**
     * @see com.huawei.unibi.molap.engine.filters.KeyFilterImpl#filterKey(com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue)
     */
    @Override
    public boolean filterKey(byte[] key)
    {
        //
        if(super.filterKey(key))
        {

            byte[][] notAllowedValuesForDim;
            int[] searchResult= new int[dimensionOffsetExclude.length];
            for(int i = 0;i < dimensionOffsetExclude.length;i++)
            {
                //
                notAllowedValuesForDim = excludeFilters[dimensionOffsetExclude[i]];
                searchResult[i] = binarySearch(notAllowedValuesForDim, key, 0,
                        maxKeyExclude[dimensionOffsetExclude[i]], excludeRanges[dimensionOffsetExclude[i]],tempsExc[i],lengthsExc[i]);
               
            }
            for(int i = 0;i < searchResult.length;i++)
            {
                if(searchResult[i] < 0)
                {
                    return true;
                }
            }
            return false;
        }
        return false;
    }
}
