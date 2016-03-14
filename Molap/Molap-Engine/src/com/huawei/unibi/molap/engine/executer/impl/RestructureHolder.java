/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7OOC2Jd5kZgCxR3cwtx0D02ZUANnCG/XV4ab8+C46asBNVuLDhmzzyzecARTAFGu9jkE
46bl6/VrInsxELP5onehfh2AxluAFaSNq5snyVYm5Eqk9l1tRmHDRO79ln+fCw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.impl;

import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.metadata.SliceMetaData;

/**
 * Restructure Holder
 * @author R00900208
 *
 */
public class RestructureHolder
{
    /**
     * 
     */
//    private RestructureStore rsStore;
    
    /**
     * 
     */
    public boolean updateRequired;
    
    /**
     * 
     */
    public SliceMetaData metaData;
    
    /**
     * maskedByteRanges
     */
    public int[] maskedByteRanges;
    
    /**
     * maskedByteRanges
     */
    private int queryDimsCount;
    
    public int getQueryDimsCount()
    {
        return queryDimsCount;
    }

    public void setQueryDimsCount(int queryDimsCount)
    {
        this.queryDimsCount = queryDimsCount;
    }

    /**
     * holder keyGenerator;
     */
    private KeyGenerator keyGenerator;

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        int hashCode =0;
//        if(null!=rsStore)
//        {
//            hashCode = rsStore.hashCode();
//        }
        result = prime * result + hashCode;
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {

        if(obj instanceof RestructureHolder)
        {
            if(this == obj)
            {
                return true;
            }
            RestructureHolder other = (RestructureHolder)obj;
            if(metaData == null)
            {
                if(other.metaData != null)
                {
                    return false;
                }
            }
            else if(!metaData.equals(other.metaData))
            {
                return false;
            }
            return true;
        }
        else
        {
            return false;
        }

    }

    public KeyGenerator getKeyGenerator()
    {
        return keyGenerator;
    }

    public void setKeyGenerator(KeyGenerator keyGenerator)
    {
        this.keyGenerator = keyGenerator;
    }
}
