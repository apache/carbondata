/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d1Su+Q4v5kYCWcWSWgZXE9Q+flFYNXVddo4oXwOBcKcPV8cBwUNDgkwKOE1JiDhncm9f
OzPfDB7ffnNECDKbjfcmNBvAthdY5LnXCw94msWZ6+dSCsTY+ZaFohv78X7Q6w==*/
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
public class IncludeOrKeyFilterImpl extends KeyFilterImpl
{
    
    private int[] dimensionOffsetIncludeOr;
    
    private byte[][][] includeFiltersOr;
    
    private int[][] includeRangesOr;
    
    private byte[][] maxKeyIncludeOr;
    
    private int[] lengthsOr;
    
    private byte[][] tempsOr = null;
    
    public IncludeOrKeyFilterImpl(InMemFilterModel filterModel, KeyGenerator keyGenerator, long[] maxKey)
    {
        this.filterModel = filterModel;
        this.keyGenerator = keyGenerator;
        dimensionOffsetIncludeOr = filterModel.getColIncludeDimOffsetOr();
        includeFiltersOr = filterModel.getIncludeFilterOr();
        includeRangesOr = filterModel.getMaskedByteRangesIncludeOr();
        maxKeyIncludeOr = filterModel.getMaxKeyIncludeOr();
        
        dimensionOffset = filterModel.getColIncludeDimOffset();
        includeFilters = filterModel.getFilter();
        maxKeyBytes = filterModel.getMaxKey();
        ranges = filterModel.getMaskedByteRanges();
        temps = new byte[dimensionOffset.length][];
        lengths = new int[dimensionOffset.length];
        createTempByteAndRangeLengths(temps,lengths,dimensionOffset,ranges); 
        
        tempsOr = new byte[dimensionOffsetIncludeOr.length][];
        lengthsOr = new int[dimensionOffsetIncludeOr.length];
        createTempByteAndRangeLengths(tempsOr,lengthsOr,dimensionOffsetIncludeOr,includeRangesOr);
 
    }

    /**
     * @see com.huawei.unibi.molap.engine.filters.KeyFilterImpl#filterKey(com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue)
     */
    @Override
    public boolean filterKey(KeyValue key)
    {
        //
        if(!super.filterKey(key))
        {

            byte[][] notAllowedValuesForDim;
            for(int i = 0;i < dimensionOffsetIncludeOr.length;i++)
            {
                //
                notAllowedValuesForDim = includeFiltersOr[dimensionOffsetIncludeOr[i]];
                int searchresult = binarySearch(notAllowedValuesForDim, key.backKeyArray, key.keyOffset,
                        maxKeyIncludeOr[dimensionOffsetIncludeOr[i]], includeRangesOr[dimensionOffsetIncludeOr[i]],tempsOr[i],lengthsOr[i]);
                if(searchresult < 0)
                {
                    return false;
                }
            }
            return true;
        }
        return true;
    }
}
