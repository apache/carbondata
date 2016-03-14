/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3dz82DI3nLoFvaoTVHjvradNejahwRUt2DUaZO8iEL7dG+rdPs3VGs/glbwbRevI05E4N
4GeEUNMZn7Ol3fV6492kmPPrE1ocotahYTEbLg2ZrVM4cyhTtdxFptP7riyiAw==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination;

import java.io.IOException;
import java.util.Map;

import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.executer.impl.RestructureHolder;
import com.huawei.unibi.molap.engine.executer.pagination.impl.QueryResult;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

/**
 * It aggregates the data received from individual thread. And also it writes to the disk if the limit exceeds
 * @author R00900208
 *
 */
public interface GlobalPaginatedAggregator
{
    /**
     * Get the rows for the range specified
     * @param fromRow
     * @param toRow
     * @return
     * @throws IOException
     */
    Map<ByteArrayWrapper, MeasureAggregator[]> getPage(int fromRow, int toRow) throws IOException;
    
    
    /**
     * Get the rows for the range specified
     * @param fromRow
     * @param toRow
     * @return
     * @throws IOException
     */
    QueryResult getResult() throws IOException;
    
    /**
     * Write data to disk if rquired
     * @param data
     * @param restructureHolder
     * @throws Exception
     */
    void writeToDisk(Map<ByteArrayWrapper, MeasureAggregator[]> data,RestructureHolder restructureHolder) throws Exception;
    
    
    
    /**
     * Write data to disk if rquired
     * @param data
     * @param restructureHolder
     * @throws Exception
     */
    void writeToDisk(Result data,RestructureHolder restructureHolder) throws Exception;

    
    /**
     * Final merge of all intermediate files if any
     */
    void mergeDataFile(boolean cacheRequired);
    
    /**
     * Get the total row count
     * @return
     */
    int getRowCount();
    
}
