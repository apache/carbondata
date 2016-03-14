/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcAIRTtLWBkMMN+iqJ62JNQb/MYFaBoemC1VlrU
n+vkOQx2yC9QT5LXkNhd0TYy6XdPSED0g2v98ucoObMG1Sx9y+z6kgEwWJYCI3V/5LtfszxC
3E07i3xnhgjCJ7kSSR4D4izqe/vyDMukk1j82yuOTBQNBiDdsh5XhnkcSW7z2A==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
*/

package com.huawei.unibi.molap.engine.executer;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Engine
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :Tuple.java
 * Class Description : Tuple class to hold one tuple 
 * Version 1.0
 */
public class Tuple
{
    /**
     * key 
     */
    private byte[] key;
    
    /**
     * measures 
     */
    private MeasureAggregator[] measures;

    /**
     * @return the key
     */
    public byte[] getKey()
    {
        return key;
    }

    /**
     * @param key the key to set
     */
    public void setKey(byte[] key)
    {
        this.key = key;
    }

    /**
     * @return the measures
     */
    public MeasureAggregator[] getMeasures()
    {
        return measures;
    }

    /**
     * @param measures the measures to set
     */
    public void setMeasures(MeasureAggregator[] measures)
    {
        this.measures = measures;
    }

}
