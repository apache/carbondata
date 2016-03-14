/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwcz8AOhvEHjQfa55oxvUSJWRQCwLl+VwWEHaV7n
0eFj3UgN7aAHDNDcFgVI4dL22x+NWYWQAwqM0pKfvKcPgO9z0m8eMB2Ld2i2FaR4NH0aATpT
st0cTwmw8SP2NP0XAu3ddTfq4U0p5pI4M9jTD7pBk3sjGP9l/ag5iqlxNDJoFQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.filters.measurefilter;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;

/**
 * This class filters the non empty measures
 * @author R00900208
 *
 */
public class NotEmptyMeasureFilterImpl implements MeasureFilter
{
    private int index;
    
    public NotEmptyMeasureFilterImpl(int index)
    {
        this.index = index;
    }

    @Override
    public boolean filter(MeasureAggregator[] msrValue)
    {
        return !msrValue[index].isFirstTime();
    }

    @Override
    public boolean filter(double[] msrValue,int msrStartIndex)
    {
        return false;
    }

}
