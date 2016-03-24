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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.util.AggUtil;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarAggregatorInfo;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarScannedResultAggregator;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.executer.impl.RestructureHolder;
import com.huawei.unibi.molap.engine.result.Result;
import com.huawei.unibi.molap.engine.result.impl.MapBasedResult;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.util.QueryExecutorUtility;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.util.MolapProperties;

public class MapBasedResultAggregatorImpl implements ColumnarScannedResultAggregator
{
    /**
     * LOGGER.
     */
    private static final LogService LOGGER = LogServiceFactory.getLogService(MapBasedResultAggregatorImpl.class.getName());
    
    private XXHash32 xxHash32;
    
    private Map<ByteArrayWrapper, MeasureAggregator[]> aggData;
    
    private ColumnarAggregatorInfo columnaraggreagtorInfo;
    
    private DataAggregator dataAggregator;
    
    private boolean isAggTable;
    
    public MapBasedResultAggregatorImpl(ColumnarAggregatorInfo columnaraggreagtorInfo, DataAggregator dataAggregator)
    {
        this.columnaraggreagtorInfo=columnaraggreagtorInfo;
        
        //CHECKSTYLE:OFF Approval No:Approval-V1R2C10_005
        boolean useXXHASH = Boolean.valueOf(MolapProperties.getInstance().getProperty("molap.enableXXHash", "false"));
        //CHECKSTYLE:ON
        
        if(useXXHASH)
        {
            xxHash32 = XXHashFactory.fastestInstance().hash32();
        }
        aggData = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(100000, 1.0f);
        this.dataAggregator=dataAggregator;
        isAggTable = columnaraggreagtorInfo.getCountMsrIndex() > -1;
    }

    @Override
    public int aggregateData(AbstractColumnarScanResult keyValue)
    {
        ByteArrayWrapper dimensionsRowWrapper = null;
        while(keyValue.hasNext())
        {
            dimensionsRowWrapper = new ByteArrayWrapper();
            //Primitives types selected
            if(columnaraggreagtorInfo.getDimensionAggInfos().size() == keyValue.getKeyBlockLength())
            {
                dimensionsRowWrapper.setMaskedKey(keyValue.getKeyArray(dimensionsRowWrapper));
            }
            else
            {
                //Complex columns selected.
                List<byte[]> complexKeyArray = keyValue.getKeyArrayWithComplexTypes(this.columnaraggreagtorInfo.getComplexQueryDims(),dimensionsRowWrapper);
                dimensionsRowWrapper.setMaskedKey(complexKeyArray.remove(complexKeyArray.size() - 1));
                for(byte[] complexKey : complexKeyArray)
                {
                    dimensionsRowWrapper.addComplexTypeData(complexKey);
                }
            }
            MeasureAggregator[] currentMsrRowData = aggData.get(dimensionsRowWrapper);
            
            if(null == currentMsrRowData)
            {
                currentMsrRowData = AggUtil.getAggregators(columnaraggreagtorInfo.getAggType(),
                        columnaraggreagtorInfo.getCustomExpressions(), isAggTable, null,
                        columnaraggreagtorInfo.getCubeUniqueName(), columnaraggreagtorInfo.getMsrMinValue(),columnaraggreagtorInfo.getHighCardinalityTypes());
                aggData.put(dimensionsRowWrapper, currentMsrRowData);
            }
            dataAggregator.aggregateData(keyValue, currentMsrRowData,dimensionsRowWrapper);
        }
        return 0;
    }
    

    @Override
    public Result getResult(RestructureHolder restructureHolder)
    {
        Result<Map<ByteArrayWrapper, MeasureAggregator[]>,Void> result = new MapBasedResult();
        result.addScannedResult(updateScannedResult(restructureHolder),null);
        return result;
    }
    
    /**
     * 
     * @param scannedResult
     * @param restructureHolder
     */
    private Map<ByteArrayWrapper, MeasureAggregator[]> updateScannedResult(RestructureHolder restructureHolder) 
    {
        if(!restructureHolder.updateRequired)
        {
            return aggData;
        }
        Map<ByteArrayWrapper, MeasureAggregator[]> finalData = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(aggData.size(), 1.0f);
        try
        {
            long[] data = null;
            long[] updatedData = null;
            ByteArrayWrapper key = null;
            for(Entry<ByteArrayWrapper, MeasureAggregator[]> e:aggData.entrySet())
            {
                key = e.getKey();
                data = restructureHolder.getKeyGenerator().getKeyArray(key.getMaskedKey(),
                        restructureHolder.maskedByteRanges);
                updatedData = new long[columnaraggreagtorInfo.getLatestKeyGenerator().getDimCount()];
                Arrays.fill(updatedData, 1);
                System.arraycopy(data, 0, updatedData, 0, data.length);
                if(restructureHolder.metaData.getNewDimsDefVals()!=null && restructureHolder.metaData.getNewDimsDefVals().length>0)
                {
                    for(int i = 0;i < restructureHolder.metaData.getNewDimsDefVals().length;i++)
                    {
                        updatedData[data.length+i]=restructureHolder.metaData.getNewDimsSurrogateKeys()[i];
                    }
                }
                key.setMaskedKey(QueryExecutorUtility.getMaskedKey(columnaraggreagtorInfo.getLatestKeyGenerator()
                        .generateKey(updatedData), columnaraggreagtorInfo.getActualMaxKeyBasedOnDimensions(), columnaraggreagtorInfo
                        .getActalMaskedByteRanges(), columnaraggreagtorInfo.getActualMaskedKeyByteSize()));
                finalData.put(key, e.getValue());
            }
        }
        catch(KeyGenException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
        }
        
        return finalData;
    }
}
