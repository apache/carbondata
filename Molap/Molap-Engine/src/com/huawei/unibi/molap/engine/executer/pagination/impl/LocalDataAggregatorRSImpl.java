/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d3fu0BD55uxd8Gn+v9J/FaFhUsZdWgsI3NLe/ZR9UAMAjFuKqotIm+MGzlDomnkCe77G
/L2SRch9AObtOhM3BjAitrcipNZZMukWjaMdrL4zGAjHVbrak2bFG8WQ3ZYcLA==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination.impl;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
//import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.engine.executer.pagination.GlobalPaginatedAggregator;
import com.huawei.unibi.molap.engine.scanner.Scanner;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;

/**
 * @author R00900208
 *
 */
public class LocalDataAggregatorRSImpl extends LocalDataAggregatorImpl
{
    
    /**
     * LOGGER.
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(LocalDataAggregatorRSImpl.class.getName());

    public LocalDataAggregatorRSImpl(SliceExecutionInfo info, GlobalPaginatedAggregator paginatedAggregator,int rowLimit, String id)
    {
        super(info, paginatedAggregator,rowLimit,id);
    }

    /* (non-Javadoc)
     * @see com.huawei.unibi.molap.engine.executer.pagination.DataAggregator#aggregate(byte[], com.huawei.unibi.molap.engine.aggregator.MeasureAggregator[])
     */
    @Override
    public void aggregate(Scanner scanner)
    {
        long startTime = System.currentTimeMillis();
        try
        {
            
            // for aggregating without decomposition using masking

            int count = 0;
            // Search till end of the data
            while(!scanner.isDone() && !interrupted)
            {
                KeyValue available = scanner.getNext();
                // Set the data into the wrapper
                dimensionsRowWrapper.setData(available.getBackKeyArray(), available.getKeyOffset(), maxKeyBasedOnDim,
                        maskedByteRanges, maskedKeyByteSize);

                // 2) Extract required measures
                MeasureAggregator[] currentMsrRowData = data.get(dimensionsRowWrapper);

                if(currentMsrRowData == null)
                {
//                    currentMsrRowData = AggUtil.getAggregators(queryMsrs, aggTable, generator, slice.getCubeUniqueName());
                    // dimensionsRowWrapper.setActualData(available.getBackKeyArray(),
                    // available.getKeyOffset(), available.getKeyLength());
                    data.put(dimensionsRowWrapper, currentMsrRowData);
                    dimensionsRowWrapper = new ByteArrayWrapper();
                    counter++;
                    if(counter > rowLimit)
                    {
                        aggregateMsrsRS(available, currentMsrRowData);
                        rowLimitExceeded();
                        counter = 0;
                        count++;
                        continue;
                    }
                }
                aggregateMsrsRS(available, currentMsrRowData);
                count++;
            }

            finish();
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Time taken for scan for range " + id + " : "
                    + (System.currentTimeMillis() - startTime) + " && Scanned Count : " + count + "  && Map Size : "
                    + rowCount);
        }
        catch(Exception e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e.getMessage());
        }
    }

    /**
     * @param available
     * @param currentMsrRowData
     */
    private void aggregateMsrsRS(KeyValue available, MeasureAggregator[] currentMsrRowData)
    {
        if(aggTable)
        {
            aggregateMsrsForAggTable(available, currentMsrRowData);
            return;
        }
        for(int i = 0;i < queryMsrs.length;i++)
        {
            double value = msrExists[i] ? available.getValue(measureOrdinal[i]) : msrDft[i];
            if(msrExists[i] && uniqueValues[measureOrdinal[i]] != value)
            {
                currentMsrRowData[i].agg(value, available.getBackKeyArray(), available.getKeyOffset(),
                        available.getKeyLength());
            }
            else if(!msrExists[i])
            {
                currentMsrRowData[i].agg(value, available.getBackKeyArray(), available.getKeyOffset(),
                        available.getKeyLength());
            }
        }
    }
    
    /**
     * aggregateMsrs
     * @param available
     * @param currentMsrRowData
     */
    protected void aggregateMsrsForAggTable(KeyValue available, MeasureAggregator[] currentMsrRowData)
    {
        double countValue = available.getValue(measureOrdinal[countMsrIndex]);
        
        for(int i = 0;i < avgMsrIndexes.length;i++)
        {
            int index = avgMsrIndexes[i];
            double value = msrExists[i] ? available.getValue(measureOrdinal[index]) : msrDft[index];
            if(msrExists[index] && uniqueValues[measureOrdinal[index]] != value)
            {
                currentMsrRowData[index].agg(value, countValue);
            }
            else if(!msrExists[index])
            {
                currentMsrRowData[index].agg(value, countValue);
            }
        }
        
        for(int i = 0;i < otherMsrIndexes.length;i++)
        {
            int index = otherMsrIndexes[i];
            double value = msrExists[i] ? available.getValue(measureOrdinal[index]) : msrDft[index];
            if(msrExists[index] && uniqueValues[measureOrdinal[index]] != value)
            {
                currentMsrRowData[index].agg(value, available.getBackKeyArray(), available.getKeyOffset(),
                        available.getKeyLength());
            }
            else if(!msrExists[index])
            {
                currentMsrRowData[index].agg(value, available.getBackKeyArray(), available.getKeyOffset(),
                        available.getKeyLength());
            }
        }
    }
}
