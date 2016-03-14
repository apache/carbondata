/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7HyuasgFkAs610bSK/ld3q3O8fOXtc4/yW8M36tmlp+88jHdLoPgbKbrUDUHa2N9qI5B
VoQzihhWWDlItUJZTGdnSfi/qFITyUFJXvqxJtduS/g3zYou9BQF04u28OIcbg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.impl;

import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.filter.MolapFilterInfo;

/**
 * FilterHolder
 * @author R00900208
 *
 */
public class FilterHolder
{
    /**
     * dimension
     */
    private Dimension dimension;
    /**
     * filterInfo
     */
    private MolapFilterInfo filterInfo;
    /**
     * @return the dimension
     */
    public Dimension getDimension()
    {
        return dimension;
    }
    /**
     * @param dimension the dimension to set
     */
    public void setDimension(Dimension dimension)
    {
        this.dimension = dimension;
    }
    /**
     * @return the filterInfo
     */
    public MolapFilterInfo getFilterInfo()
    {
        return filterInfo;
    }
    /**
     * @param filterInfo the filterInfo to set
     */
    public void setFilterInfo(MolapFilterInfo filterInfo)
    {
        this.filterInfo = filterInfo;
    }
    
    
}

