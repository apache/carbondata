/*--------------------------------------------------------------------------------------------------------------------------*/
/*!!Warning: This is a key information asset of Huawei Tech Co.,Ltd                                                         */
/*CODEMARK:kOyQZYzjDpyGdBAEC2GaWmnksNUG9RKxzMKuuAYTdbJ5ajFrCnCGALet/FDi0nQqbEkSZoTs
2wdXgejaKCr1dP3uE3wfvLHF9gW8+IdXbwedLwWEET5JCCp2J65j3EiB2PJ4ohyqaGEDuXyJ
TTt3d0EsxvHoMkGWmRRmyEz+U3jFefbodoqI8Ek8SEFXdMjJ1wKoRMPneP4jQWJGIdJ6Kwd4
HdTwkaWbZA0QdSLZNpjr74Dk+jSBjEpkxFlhdx2cB5bf5TIifIHq4HxeI85ajQ==*/
/*--------------------------------------------------------------------------------------------------------------------------*/
/**
 * 
 */
package com.huawei.unibi.molap.engine.executer.pagination.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.cache.QueryExecutorUtil;
import com.huawei.unibi.molap.engine.datastorage.storeInterfaces.KeyValue;
import com.huawei.unibi.molap.engine.executer.pagination.GlobalPaginatedAggregator;
import com.huawei.unibi.molap.engine.schema.metadata.SliceExecutionInfo;

/**
 * Project Name NSE V3R8C10 
 * Module Name : MOLAP Data Processor
 * Author :k00900841 
 * Created Date:10-Aug-2014
 * FileName : LocalDataAggregatorForAutoAggregateImpl.java
 * Class Description : scan the data from store and aggregate 
 * Class Version 1.0
 */
public class LocalDataAggregatorForAutoAggregateImpl extends LocalDataAggregatorImpl
{

    /**
     * customMeasureIndex
     */
    private int [] customMeasureIndex;
    
    public LocalDataAggregatorForAutoAggregateImpl(SliceExecutionInfo info,GlobalPaginatedAggregator paginatedAggregator,int rowLimit, String id)
    {
        super(info,paginatedAggregator,rowLimit,id);
        generator=info.getFactKeyGenerator();
        customMeasureIndex= getCustomMeasureIndex();
        if(aggTable)
        {
            otherMsrIndexes = getOtherMsrIndexesWithOutCustomMeasureAndAverageMeasure();
        }
        else
        {
            otherMsrIndexes = getOtherMsrIndexesWithOutCustomMeasure();
        }
    }
    
    private int[] getCustomMeasureIndex()
    {
        List<Integer> list = new ArrayList<Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        for(int i = 0;i < queryMsrs.length;i++)
        {
            if(queryMsrs[i].getAggName().equals(MolapCommonConstants.CUSTOM)
                    || queryMsrs[i].getAggName().equals(MolapCommonConstants.DISTINCT_COUNT))
            {
                list.add(i);
            }
        }
        return QueryExecutorUtil.convertIntegerListToIntArray(list);
    }

    /**
     * aggregateMsrs
     * @param available
     * @param currentMsrRowData
     */
    protected void aggregateMsrs(KeyValue available, MeasureAggregator[] currentMsrRowData)
    {
        if(aggTable)
        {
            aggregateMsrsForAggTable(available, currentMsrRowData);
            return;
        }
        double doubleValue/* = 0.0*/;
        for(int i = 0;i < otherMsrIndexes.length;i++)
        {
            doubleValue = available.getValue(measureOrdinal[otherMsrIndexes[i]]);
            if(uniqueValues[measureOrdinal[otherMsrIndexes[i]]] != doubleValue)
            {
                currentMsrRowData[otherMsrIndexes[i]].agg(doubleValue, available.backKeyArray,
                    available.keyOffset, available.keyLength);
            }
        }
        byte[] byteValue= null;
        for(int i = 0;i < customMeasureIndex.length;i++)
        {
            byteValue = available.getByteArrayValue(measureOrdinal[customMeasureIndex[i]]);
            currentMsrRowData[customMeasureIndex[i]].agg(byteValue, available.backKeyArray, available.keyOffset,
                    available.keyLength);
        }
    }
    
    private int[] getOtherMsrIndexesWithOutCustomMeasureAndAverageMeasure()
    {
        int[] indexes = new int[queryMsrs.length-(avgMsrIndexes.length+customMeasureIndex.length)];
        int k = 0;
        for(int i = 0;i < queryMsrs.length;i++)
        {
            if(Arrays.binarySearch(avgMsrIndexes, i) < 0
                    && Arrays.binarySearch(customMeasureIndex, i)<0)
            {
                indexes[k++] = i;
            }
        }
        return indexes;
    }
    
    private int[] getOtherMsrIndexesWithOutCustomMeasure()
    {
        int[] indexes = new int[queryMsrs.length-(customMeasureIndex.length)];
        int k = 0;
        for(int i = 0;i < queryMsrs.length;i++)
        {
            if(Arrays.binarySearch(customMeasureIndex, i)<0)
            {
                indexes[k++] = i;
            }
        }
        return indexes;
    }
    
    /**
     * aggregateMsrs
     * @param available
     * @param currentMsrRowData
     */
    protected void aggregateMsrsForAggTable(KeyValue available, MeasureAggregator[] currentMsrRowData)
    {
        double countValue = available.getValue(measureOrdinal[countMsrIndex]);
        double avgValue/*= 0.0*/;
        for(int i = 0;i < avgMsrIndexes.length;i++)
        {
            avgValue = available.getValue(measureOrdinal[avgMsrIndexes[i]]);
            if(uniqueValues[measureOrdinal[avgMsrIndexes[i]]] != avgValue)
            {
                currentMsrRowData[avgMsrIndexes[i]].agg(avgValue,countValue);
            }
        }
        double otherValue/*= 0.0*/;
        for(int i = 0;i < otherMsrIndexes.length;i++)
        {
            otherValue = available.getValue(measureOrdinal[otherMsrIndexes[i]]);
            if(uniqueValues[measureOrdinal[otherMsrIndexes[i]]] != otherValue)
            {
                currentMsrRowData[otherMsrIndexes[i]].agg(otherValue, available.backKeyArray,
                    available.keyOffset, available.keyLength);
            }
        }
        byte[] byteValue = null;
        for(int i = 0;i < customMeasureIndex.length;i++)
        {
            byteValue = available.getByteArrayValue(measureOrdinal[customMeasureIndex[i]]);
            currentMsrRowData[customMeasureIndex[i]].agg(byteValue, available.backKeyArray, available.keyOffset,
                    available.keyLength);
        }
    }
}
