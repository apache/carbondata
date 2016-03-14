/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d2CkfFqaW8IdBB1DGoyw9k5iKmo0Ce/wEZKv5UXnkMOsELNWvTzYWgUahYXUWCDqaoG5
9ZlePDjdEzXgag/V1rF6a8z/kFlYcENseckrs3IEEUAShT/Ugo7uu1CvMBhLNg==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.groupby.GroupByHolder;
import com.huawei.unibi.molap.engine.executer.pagination.exception.MolapPaginationException;

/**
 * It process the data
 * @author R00900208
 *
 */
public interface DataProcessor
{
    
    /**
     * Initialize the processor
     * @param model
     */
    void initModel(PaginationModel model) throws MolapPaginationException;

    /**
     * Process the data 
     * @param key
     * @param measures
     */
    void processRow(byte[] key,MeasureAggregator[] measures) throws MolapPaginationException;
    
    /**
     * It process the aggregated data which is done in previous step,which is used in measure filters and topN.
     * @param groupByHolder
     */
    void processGroup(GroupByHolder groupByHolder) throws MolapPaginationException;
    
    
    /**
     * finish processing
     */
    void finish() throws MolapPaginationException;
    
}
