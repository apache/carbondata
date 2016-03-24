/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.huawei.unibi.molap.engine.columnar.aggregator.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarAggregatorInfo;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarScannedResultAggregator;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.executer.impl.RestructureHolder;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.engine.result.impl.ListBasedResult;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.util.QueryExecutorUtility;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.keygenerator.KeyGenException;

public class ListBasedResultAggregatorImpl implements ColumnarScannedResultAggregator
{
    /**
     * LOGGER.
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(ListBasedResultAggregatorImpl.class.getName());
    
    private List<ByteArrayWrapper> keys;

    private List<MeasureAggregator[]> values;

    private ColumnarAggregatorInfo columnaraggreagtorInfo;

    private DataAggregator dataAggregator;

    private boolean isAggTable;

    private int rowCounter;

    private int limit;

    public ListBasedResultAggregatorImpl(ColumnarAggregatorInfo columnaraggreagtorInfo, DataAggregator dataAggregator)
    {
        this.columnaraggreagtorInfo = columnaraggreagtorInfo;
        this.dataAggregator = dataAggregator;
        isAggTable = columnaraggreagtorInfo.getCountMsrIndex() > -1;
        limit = columnaraggreagtorInfo.getLimit();
    }

    @Override
    public int aggregateData(AbstractColumnarScanResult keyValue)
    {
        this.keys = new ArrayList<ByteArrayWrapper>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        this.values = new ArrayList<MeasureAggregator[]>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        ByteArrayWrapper key = null;
        MeasureAggregator[] value = null;
        while(keyValue.hasNext() && (limit == -1 || rowCounter < limit))
        {
            key = new ByteArrayWrapper();
            //Primitives types selected
            if(columnaraggreagtorInfo.getQueryDimensionsLength() == keyValue.getKeyBlockLength())
            {
                key.setMaskedKey(keyValue.getKeyArray(key));
            }
            else
            {
                //Complex columns selected.
                List<byte[]> complexKeyArray = keyValue.getKeyArrayWithComplexTypes(this.columnaraggreagtorInfo.getComplexQueryDims(),key);
                key.setMaskedKey(complexKeyArray.remove(complexKeyArray.size() - 1));
                for(byte[] complexKey : complexKeyArray)
                {
                    key.addComplexTypeData(complexKey);
                }
            }
            value = AggUtil.getAggregators(columnaraggreagtorInfo.getAggType(), isAggTable, null,
                        columnaraggreagtorInfo.getCubeUniqueName(),columnaraggreagtorInfo.getMsrMinValue(),columnaraggreagtorInfo.getHighCardinalityTypes());
            dataAggregator.aggregateData(keyValue, value,key);
            keys.add(key);
            values.add(value);
            rowCounter++;
        }
        return rowCounter;
    }

    @Override
    public Result getResult(RestructureHolder restructureHolder)
    {
        List<ByteArrayWrapper> finalKeys = new ArrayList<ByteArrayWrapper>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        List<MeasureAggregator[]> finalValues = new ArrayList<MeasureAggregator[]>(
                MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
        Result<List<ByteArrayWrapper>, List<MeasureAggregator[]>> result = new ListBasedResult();

        if(!restructureHolder.updateRequired)
        {
            result.addScannedResult(keys, values);
        }
        else
        {
            updateScannedResult(restructureHolder, finalKeys, finalValues);
            result.addScannedResult(finalKeys, finalValues);
        }
        return result;
    }

    /**
     * 
     * @param scannedResult
     * @param restructureHolder
     */
    private void updateScannedResult(RestructureHolder restructureHolder, List<ByteArrayWrapper> finalKeys,
            List<MeasureAggregator[]> finalValues)
    {
        if(!restructureHolder.updateRequired)
        {
            return;
        }

        try
        {
            long[] data = null;
            long[] updatedData = null;
            ByteArrayWrapper key = null;
            for(int i = 0;i < keys.size();i++)
            {
                key = keys.get(i);
                data = restructureHolder.getKeyGenerator().getKeyArray(key.getMaskedKey(),
                        restructureHolder.maskedByteRanges);
                updatedData = new long[columnaraggreagtorInfo.getLatestKeyGenerator().getDimCount()];
                Arrays.fill(updatedData, 1);
                System.arraycopy(data, 0, updatedData, 0, data.length);
                if(restructureHolder.metaData.getNewDimsDefVals()!=null && restructureHolder.metaData.getNewDimsDefVals().length>0)
                {
                    for(int k = 0;k < restructureHolder.metaData.getNewDimsDefVals().length;k++)
                    {
                        updatedData[data.length+k]=restructureHolder.metaData.getNewDimsSurrogateKeys()[k];
                    }
                }
                if(restructureHolder.getQueryDimsCount() == columnaraggreagtorInfo.getLatestKeyGenerator().getDimCount())
                {
                    key.setMaskedKey(QueryExecutorUtility.getMaskedKey(columnaraggreagtorInfo.getLatestKeyGenerator()
                        .generateKey(updatedData), columnaraggreagtorInfo.getActualMaxKeyBasedOnDimensions(),
                        columnaraggreagtorInfo.getActalMaskedByteRanges(), columnaraggreagtorInfo
                                .getActualMaskedKeyByteSize()));
                }
                finalKeys.add(key);
                finalValues.add(values.get(i));
            }
        }
        catch(KeyGenException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
        }
    }
}
