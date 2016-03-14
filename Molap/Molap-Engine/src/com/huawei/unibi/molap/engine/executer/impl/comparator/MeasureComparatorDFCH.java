/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7JinIZ0bTRaefaxFZwx0MX2f4iQdTj4U0wVK7BFgIZjmFhFjze2bvfS12wF/d9KGr0UC
Jurlz4VtUD7sWctPbFkwnsbzswGZmN/xcLJSb3oUb2HiuiCUJvrzaAzAD8LuEg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
package com.huawei.unibi.molap.engine.executer.impl.comparator;

import java.util.Comparator;

import com.huawei.unibi.molap.engine.executer.pagination.impl.DataFileChunkHolder;

public class MeasureComparatorDFCH implements Comparator<DataFileChunkHolder>
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
    public MeasureComparatorDFCH(int msrIndex, int sortOrder)
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
    public int compare(DataFileChunkHolder t1, DataFileChunkHolder t2)
    {
        int cmp=0;
        
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
