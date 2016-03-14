/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwdEVzw1icjfRowqz2DW4XzUpEhhSzBOwVynEHjc
u0090SUiXdYdRhbzF5h44lrbXvFQtOz585EI920UpF7Bjm0qHZvvAGkoYs+XjRZxblkM9Lta
3ptTp1nBKQIu0Qj9ZTNGgcYFCn6u96syyG3KWfKbX23RWHvTK/Ggt4qFqfIZCg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.aggregator;

/**
 * @author R00900208
 *
 */
public interface CalculatedMeasureAggregator extends MeasureAggregator
{

    /**
     * Calculate calculated measures
     * @param aggregators
     */
    void calculateCalcMeasure(MeasureAggregator[] aggregators);
}
