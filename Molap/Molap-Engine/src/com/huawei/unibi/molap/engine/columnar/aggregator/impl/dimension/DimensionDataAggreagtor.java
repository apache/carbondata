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

package com.huawei.unibi.molap.engine.columnar.aggregator.impl.dimension;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.MeasureAggregator;
import com.huawei.unibi.molap.engine.aggregator.dimension.DimensionAggregatorInfo;
import com.huawei.unibi.molap.engine.columnar.aggregator.ColumnarAggregatorInfo;
import com.huawei.unibi.molap.engine.columnar.keyvalue.AbstractColumnarScanResult;
import com.huawei.unibi.molap.engine.datastorage.Member;
import com.huawei.unibi.molap.engine.complex.querytypes.GenericQueryType;
import com.huawei.unibi.molap.engine.util.DataTypeConverter;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.util.QueryExecutorUtility;
import com.huawei.unibi.molap.engine.wrappers.ByteArrayWrapper;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.olap.SqlStatement;
import com.huawei.unibi.molap.olap.SqlStatement.Type;

public class DimensionDataAggreagtor
{
    private static final LogService LOGGER = LogServiceFactory.getLogService(DimensionDataAggreagtor.class.getName());

    private int[][] dimCountAndDistinctCountAGGIndex;

    private int[][] dimAggNormalIndex;
    
    private int[][] dimAggMaxMinIndex;

    private List<DimensionAggregatorInfo> dimensionAggInfos;

    private ColumnarAggregatorInfo columnaraggreagtorInfo;

    public DimensionDataAggreagtor(ColumnarAggregatorInfo columnaraggreagtorInfo)
    {
        this.columnaraggreagtorInfo = columnaraggreagtorInfo;
        this.dimensionAggInfos = columnaraggreagtorInfo.getDimensionAggInfos();

        List<List<Integer>> countIndexListForDims = new ArrayList<List<Integer>>(10);
        List<List<Integer>> nomralIndexListForDims = new ArrayList<List<Integer>>(10);
        List<List<Integer>> maxMinIndexListForDims = new ArrayList<List<Integer>>(10);
        List<Integer> countIndexList = null;
        List<Integer> normalIndexList = null;
        List<Integer> maxMinIndexList = null;
        List<String> aggList = null;
        Iterator<DimensionAggregatorInfo> iterator = dimensionAggInfos.iterator();
        int index = 0;
        while(iterator.hasNext())
        {
            countIndexList = new ArrayList<Integer>(10);
            normalIndexList = new ArrayList<Integer>(10);
            maxMinIndexList = new ArrayList<Integer>(10);
            aggList = iterator.next().getAggList();
            for(int j = 0;j < aggList.size();j++)
            {
                if(aggList.get(j).equals(MolapCommonConstants.COUNT)
                        || aggList.get(j).equals(MolapCommonConstants.DISTINCT_COUNT))
                {
                    countIndexList.add(index);
                }
                else if(aggList.get(j).equals(MolapCommonConstants.SUM)
                        || aggList.get(j).equals(MolapCommonConstants.AVERAGE)
                        || aggList.get(j).equals(MolapCommonConstants.SUM_DISTINCT))
                {
                    normalIndexList.add(index);
                }
                else
                {
                    maxMinIndexList.add(index);
                }
                index++;
            }
            countIndexListForDims.add(countIndexList);
            nomralIndexListForDims.add(normalIndexList);
            maxMinIndexListForDims.add(maxMinIndexList);
        }
        dimCountAndDistinctCountAGGIndex = new int[countIndexListForDims.size()][];
        for(int i = 0;i < dimCountAndDistinctCountAGGIndex.length;i++)
        {
            dimCountAndDistinctCountAGGIndex[i] = new int[countIndexListForDims.get(i).size()];
            for(int j = 0;j < dimCountAndDistinctCountAGGIndex[i].length;j++)
            {
                dimCountAndDistinctCountAGGIndex[i][j] = countIndexListForDims.get(i).get(j);
            }
        }
        dimAggNormalIndex = new int[nomralIndexListForDims.size()][];
        for(int i = 0;i < dimAggNormalIndex.length;i++)
        {
            dimAggNormalIndex[i] = new int[nomralIndexListForDims.get(i).size()];
            for(int j = 0;j < dimAggNormalIndex[i].length;j++)
            {
                dimAggNormalIndex[i][j] = nomralIndexListForDims.get(i).get(j);
            }
        }
        
        dimAggMaxMinIndex = new int[maxMinIndexListForDims.size()][];
        for(int i = 0;i < dimAggMaxMinIndex.length;i++)
        {
            dimAggMaxMinIndex[i] = new int[maxMinIndexListForDims.get(i).size()];
            for(int j = 0;j < dimAggMaxMinIndex[i].length;j++)
            {
                dimAggMaxMinIndex[i][j] = maxMinIndexListForDims.get(i).get(j);
            }
        }
    }

    public void aggregateDimension(AbstractColumnarScanResult keyValue, MeasureAggregator[] currentMsrRowData, ByteArrayWrapper dimensionsRowWrapper)
    {
        DimensionAggregatorInfo dimensionAggregatorInfo= null;
        int dimSurrogate = 0;
        for(int i = 0;i < dimensionAggInfos.size();i++)
        {
            dimensionAggregatorInfo = dimensionAggInfos.get(i);
            
            if(!dimensionAggregatorInfo.isDimensionPresentInCurrentSlice())
            {
                continue;
            }
            else if(!dimensionAggregatorInfo.getDim().isHighCardinalityDim())
            {
                Object dataBasedOnDataType = null;
                byte[] complexSurrogates = null;
                GenericQueryType complexType = null;
                if(dimensionAggregatorInfo.getDim().getDataType() != Type.ARRAY &&  dimensionAggregatorInfo.getDim().getDataType() != Type.STRUCT)
                {
                    dimSurrogate = keyValue.getDimDataForAgg(dimensionAggregatorInfo.getDim().getOrdinal());
                    if(dimSurrogate==1)
                    {
                        break;
                    }
                }
                else
                {
                    try
                    {
                        complexType = this.columnaraggreagtorInfo.getComplexQueryDims().get(dimensionAggregatorInfo.getDim().getOrdinal());
                        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                        DataOutputStream dataOutputStream = new DataOutputStream(byteStream);
                        keyValue.getComplexDimDataForAgg(complexType, dataOutputStream);
                        complexSurrogates = byteStream.toByteArray();
                        byteStream.close();
                    }
                    catch(IOException e)
                    {
                        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
                    }
                }
                for(int j = 0;j < dimCountAndDistinctCountAGGIndex[i].length;j++)
                {
                    if(dimensionAggregatorInfo.getDim().getDataType() != Type.ARRAY &&  dimensionAggregatorInfo.getDim().getDataType() != Type.STRUCT)
                    {
                        if(dimensionAggregatorInfo.getDim().getDataType()!=SqlStatement.Type.STRING)
                        {
                            if(null==dataBasedOnDataType)
                            {
                                dataBasedOnDataType = QueryExecutorUtility
                                        .getMemberBySurrogateKey(dimensionAggregatorInfo.getDim(), dimSurrogate,
                                                columnaraggreagtorInfo.getSlices(), columnaraggreagtorInfo.getCurrentSliceIndex()).toString();
                                dataBasedOnDataType = DataTypeConverter.getDataBasedOnDataType((String)dataBasedOnDataType,
                                        dimensionAggregatorInfo.getDim().getDataType());
                            }
                            if(null!=dataBasedOnDataType)
                            {
                                currentMsrRowData[dimCountAndDistinctCountAGGIndex[i][j]].agg(dimSurrogate, null, 0, 0);
                            }
                        }
                        else
                        {
                            currentMsrRowData[dimCountAndDistinctCountAGGIndex[i][j]].agg(dimSurrogate, null, 0, 0);
                        }
                    }
                    else
                    {
                        currentMsrRowData[dimCountAndDistinctCountAGGIndex[i][j]].agg(complexSurrogates, null, 0, 0);
                        dataBasedOnDataType = complexType.getDataBasedOnDataTypeFromSurrogates(this.columnaraggreagtorInfo.getSlices(), ByteBuffer.wrap(complexSurrogates), this.columnaraggreagtorInfo.getDimensions());
                    }
                }
                
                for(int j = 0;j < dimAggNormalIndex[i].length;j++)
                {
                    if(dataBasedOnDataType== null)
                    {
                        dataBasedOnDataType = QueryExecutorUtility
                                .getMemberBySurrogateKey(dimensionAggregatorInfo.getDim(), dimSurrogate,
                                        columnaraggreagtorInfo.getSlices(), columnaraggreagtorInfo.getCurrentSliceIndex()).toString();
                    }
                    // Do not convert to double as the aggregator may work on
                    // different data types. Min & Max can work for String,
                    // TimeStamp,etc
    
                    if(dataBasedOnDataType instanceof Double)
                    {
                        currentMsrRowData[dimAggNormalIndex[i][j]].agg(dataBasedOnDataType, null, 0, 0);
                    }
                    else if(!((String)dataBasedOnDataType).equals(MolapCommonConstants.MEMBER_DEFAULT_VAL))
                    {
                        dataBasedOnDataType = DataTypeConverter.getDataBasedOnDataType((String)dataBasedOnDataType,
                                SqlStatement.Type.DOUBLE);
                        if(null != dataBasedOnDataType)
                        {
                            currentMsrRowData[dimAggNormalIndex[i][j]].agg(dataBasedOnDataType, null, 0, 0);
                        }
                    }
                }
                
                for(int j = 0;j < dimAggMaxMinIndex[i].length;j++)
                {
                    if(dataBasedOnDataType== null)
                    {
                        dataBasedOnDataType = QueryExecutorUtility
                                .getMemberBySurrogateKey(dimensionAggregatorInfo.getDim(), dimSurrogate,
                                        columnaraggreagtorInfo.getSlices(), columnaraggreagtorInfo.getCurrentSliceIndex()).toString();
                    }
                    if(dataBasedOnDataType instanceof Double)
                    {
                        currentMsrRowData[dimAggMaxMinIndex[i][j]].agg(dataBasedOnDataType, null, 0, 0);
                    }
                    else if(!((String)dataBasedOnDataType).equals(MolapCommonConstants.MEMBER_DEFAULT_VAL))
                    {
                        dataBasedOnDataType = DataTypeConverter.getDataBasedOnDataType((String)dataBasedOnDataType,
                                dimensionAggregatorInfo.getDim().getDataType());
                        if(null != dataBasedOnDataType)
                        {
                            currentMsrRowData[dimAggMaxMinIndex[i][j]].agg(dataBasedOnDataType, null, 0, 0);
                        }
                    }
                }
            }
            else
            {
                aggregateDirectDurrogateDims(keyValue,currentMsrRowData,dimensionAggregatorInfo.getDim(),i,dimensionsRowWrapper);
                
            }
        }
        
    }

    /**
     * For handling aggregation in case of direct surrogate keys columns.
     * @param keyValue
     * @param currentMsrRowData
     * @param dim
     * @param index
     * @param dimensionsRowWrapper
     */
    private void aggregateDirectDurrogateDims(AbstractColumnarScanResult keyValue,
            MeasureAggregator[] currentMsrRowData, Dimension dim,int index, ByteArrayWrapper dimensionsRowWrapper)
    {
        //since new byte array wrapper is been created and send for aggregation again system has to set the data to the byte array wrapper
        if(null==dimensionsRowWrapper.getDirectSurrogateKeyList() || dimensionsRowWrapper.getDirectSurrogateKeyList().isEmpty() )
        {
            dimensionsRowWrapper.addToDirectSurrogateKeyList(keyValue.getHighCardinalityDimDataForAgg(dim));
        }
        String data = new String(keyValue.getHighCardinalityDimDataForAgg(dim));
        if(MolapCommonConstants.MEMBER_DEFAULT_VAL.equals(data))
        {
            return;
        }
        
        //Logic is for distinct count of dimensions, passing the members as string itself
        for(int j = 0;j < dimCountAndDistinctCountAGGIndex[index].length;j++)
        {

                currentMsrRowData[dimCountAndDistinctCountAGGIndex[index][j]].agg(data,
                  null, 0, 0);
        }

        Object dataBasedOnDataType = DataTypeConverter.getDataBasedOnDataType(data, dim.getDataType());
        if(null==dataBasedOnDataType)
        {
            return;
        }
        //for handling other aggregations like sum and avg for normal dimensions
        for(int j = 0;j < dimAggNormalIndex[index].length;j++)
        {
            if(dataBasedOnDataType == null)
            {
                dataBasedOnDataType = new Member(keyValue.getHighCardinalityDimDataForAgg(dim));
            }
            // Do not convert to double as the aggregator may work on
            // different data types. Min & Max can work for String,
            // TimeStamp,etc

            if(dataBasedOnDataType instanceof Double)
            {
                currentMsrRowData[dimAggNormalIndex[index][j]].agg(dataBasedOnDataType, null, 0, 0);
            }
            else if(!(dataBasedOnDataType.toString()).equals(MolapCommonConstants.MEMBER_DEFAULT_VAL))
            {
                dataBasedOnDataType = DataTypeConverter.getDataBasedOnDataType(dataBasedOnDataType.toString(),
                        SqlStatement.Type.DOUBLE);
                if(null != dataBasedOnDataType)
                {
                    currentMsrRowData[dimAggNormalIndex[index][j]].agg(dataBasedOnDataType, null, 0, 0);
                }
            }
        }
        //for handling max and min aggegator, passing the members as per the selected data type
        for(int j = 0;j < dimAggMaxMinIndex[index].length;j++)
        {
             currentMsrRowData[dimAggMaxMinIndex[index][j]].agg(dataBasedOnDataType, null, 0, 0);
        }

    }
}
