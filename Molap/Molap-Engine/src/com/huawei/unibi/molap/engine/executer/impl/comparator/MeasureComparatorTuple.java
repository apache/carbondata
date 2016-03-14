/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7Jh+2MfojxAIilpnWbvcXdFbVPIHjfJsoVbLbz0MVl1Z7iWWDYnBjejk53KECv/40QGH
QPIDsgIfXoTpQC1ec/n9SIx+/dGjE0K45fsiFgjrCTgKFOaApf5n+TMcvasEJw==*/
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
package com.huawei.unibi.molap.engine.executer.impl.comparator;

import java.util.Comparator;

import com.huawei.unibi.molap.engine.executer.Tuple;

/**
 * Project Name NSE V3R7C00 
 * Module Name : Molap Engine
 * Author K00900841
 * Created Date :21-May-2013 6:42:29 PM 
 * FileName :MaksedByteComparator.java
 * Class Description : Comparator responsible for based on measures
 * Version 1.0
 */
public class MeasureComparatorTuple implements Comparator<Tuple>
{
    /**
     * msrIndex
     */
    private int msrIndex;
    
    /**
     * sortOrder
     */
    private int sortOrder;
    
    /**
     * MeasureComparator Constructor
     * @param msrIndex
     * @param sortOrder
     */
    public MeasureComparatorTuple(int msrIndex, int sortOrder)
    {
        this.msrIndex=msrIndex;
        this.sortOrder=sortOrder;
    }
    
    /**
     * This method will be used to compare two byte array
     * @param o1
     * @param o2
     */
    @Override
    public int compare(Tuple t1, Tuple t2)
    {
        int cmp=0;
        if(this.msrIndex < 0 || this.msrIndex >= t1.getMeasures().length)
        {
            return cmp;
        }
        double msrValue1 = t1.getMeasures()[this.msrIndex].getValue();
        double msrValue2 = t2.getMeasures()[this.msrIndex].getValue();
        
        if(msrValue1<msrValue2)
        {
            cmp=-1;
        }
        else if(msrValue1>msrValue2)
        {
            cmp=1;
        }
        if(this.sortOrder==1 || this.sortOrder==3)
        {
            cmp*=-1;
        }
        return cmp;
    }

}
