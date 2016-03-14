/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbweRARwUrjYxPx0CUk3mVB7mxOcZSaagKrMQNlhB
QO/t7O73dv5hSrX7LgqfLEZwD9IZpOd7Te3+G9QyCy9XA4ODzaJiD95RvlQgv58AtscwYMAW
lC18uQVMy07tJ8Z5HNoxRXnb0RlNVoHglAzyMtwANdu7xrZplPnzvAo/iw+uVA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 *
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2012
 * =====================================
 *
 */
package com.huawei.unibi.molap.engine.schema.metadata;

/**
 * 
 * @author K00900841
 *
 */
public class SliceUniqueValueInfo
{
    private double[] uniqueValue;
    
    private String[] cols;
    
    
    /**
     * getLength
     * @return int
     */
    public int getLength()
    {
        return uniqueValue.length;
    }

    /**
     * getUniqueValue
     * @return double[]
     */
    public double[] getUniqueValue()
    {
        return uniqueValue;
    }

    /**
     * setUniqueValue
     * @param uniqueValue1 void
     */
    public void setUniqueValue(double[] uniqueValue1)
    {
        this.uniqueValue = uniqueValue1;
    }

    public String[] getCols()
    {
        return cols;
    }

    public void setCols(String[] cols1)
    {
        this.cols = cols1;
    }


}
