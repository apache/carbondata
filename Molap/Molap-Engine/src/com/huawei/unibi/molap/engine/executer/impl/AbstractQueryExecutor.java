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

package com.huawei.unibi.molap.engine.executer.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.constants.MolapCommonConstants;
import com.huawei.unibi.molap.engine.aggregator.CustomMolapAggregateExpression;
import com.huawei.unibi.molap.engine.aggregator.dimension.DimensionAggregatorInfo;
import com.huawei.unibi.molap.engine.cache.QueryExecutorUtil;
import com.huawei.unibi.molap.engine.datastorage.CubeDataStore;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCube;
import com.huawei.unibi.molap.engine.datastorage.InMemoryCubeStore;
import com.huawei.unibi.molap.engine.datastorage.MemberStore;
import com.huawei.unibi.molap.engine.datastorage.QueryMapper;
import com.huawei.unibi.molap.engine.executer.MolapQueryExecutorModel;
import com.huawei.unibi.molap.engine.executer.QueryExecutor;
import com.huawei.unibi.molap.engine.executer.exception.QueryExecutionException;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.engine.util.QueryExecutorUtility;
import com.huawei.unibi.molap.keygenerator.KeyGenException;
import com.huawei.unibi.molap.keygenerator.KeyGenerator;
import com.huawei.unibi.molap.keygenerator.factory.KeyGeneratorFactory;
import com.huawei.unibi.molap.metadata.MolapMetadata.Dimension;
import com.huawei.unibi.molap.metadata.MolapMetadata.Measure;

/**
 * Abstract class used for query execution and to use to initialize all the properties used for query execution
 * @author K00900841
 *
 */
public abstract class AbstractQueryExecutor implements QueryExecutor
{
    private static final LogService LOGGER = LogServiceFactory.getLogService(AbstractQueryExecutor.class.getName());

    protected QueryExecuterProperties executerProperties;

    public AbstractQueryExecutor(List<Dimension> dimList, String schemaName, String cubeName)
    {
        executerProperties = new QueryExecuterProperties();
        executerProperties.schemaName = schemaName;
        executerProperties.cubeName = cubeName;
        if(null == cubeName)
        {
            if(null != dimList && dimList.size() != 0)
            {
                cubeName = dimList.get(0).getCube().getCubeName();
            }
        }
        executerProperties.cubeUniqueName = schemaName + '_' + cubeName;
        if(dimList != null)
        {
            executerProperties.dimTables = dimList.toArray(new Dimension[dimList.size()]);
            executerProperties.complexDimensionsMap = QueryExecutorUtility.getComplexDimensionsMap(executerProperties.dimTables);
        }
        Long threadID = Thread.currentThread().getId();
        List<Long> sliceIds = QueryMapper.getSlicesForThread(threadID);
        if(sliceIds == null || sliceIds.size() == 0)
        {
            executerProperties.slices = InMemoryCubeStore.getInstance().getActiveSlices(
                    executerProperties.cubeUniqueName);
        }
        else
        {
            executerProperties.slices = InMemoryCubeStore.getInstance().getSllicesbyIds(
                    executerProperties.cubeUniqueName, sliceIds);
        }
       
    }

    private final class SliceComparator implements Comparator<InMemoryCube>
    {
        private String tableName;
        
        private SliceComparator(String tableName)
        {
            this.tableName=tableName;
        }
        @Override
        public int compare(InMemoryCube o1, InMemoryCube o2)
        {
            int loadId1 = !o1.getTableName().equals(tableName)?Integer.MIN_VALUE:o1.getLoadId();
            int loadId2 = !o2.getTableName().equals(tableName)?Integer.MIN_VALUE:o2.getLoadId();
            if(loadId1<0)
            {
                return -1;
            }
            else if(loadId2<0)
            {
                return 1;
            }
            else
            {
                return loadId1-loadId2;
            }
        }
    }
    protected void initQuery(MolapQueryExecutorModel queryModel) throws QueryExecutionException
    {
        Collections.sort(executerProperties.slices, new SliceComparator(queryModel.getFactTable()));
        for(int i = executerProperties.slices.size()-1;i >=0;i--)
        {
            if(executerProperties.slices.get(i).getTableName().equals(queryModel.getFactTable()))
            {
                executerProperties.globalKeyGenerator= executerProperties.slices.get(i)
                        .getKeyGenerator(queryModel.getFactTable());
                executerProperties.hybridStoreModel=executerProperties.slices.get(i).getHybridStoreModel();
                break;
            }
        }
        
        executerProperties.aggExpDimensions = new ArrayList<Dimension>();
        executerProperties.aggExpMeasures = new ArrayList<Measure>();
        fillDimensionsFromExpression(queryModel.getExpressions(), executerProperties.aggExpDimensions, executerProperties.aggExpMeasures);
        
        executerProperties.uniqueValue = QueryExecutorUtility.updateUniqueForSlices(queryModel.getFactTable(),
                queryModel.isAggTable(), executerProperties.slices);
        
        double[] msrMinValue = QueryExecutorUtility.getMinValueOfSlices(queryModel.getFactTable(),
                queryModel.isAggTable(), executerProperties.slices);
        
        executerProperties.dimSortOrder = new byte[0];
        
        int aggTypeCount = queryModel.getMsrs().size() + queryModel.getExpressions().size();
        
        Iterator<DimensionAggregatorInfo> iterator = queryModel.getDimensionAggInfo().iterator();
        while(iterator.hasNext())
        {
            aggTypeCount+=iterator.next().getAggList().size();
        }
        executerProperties.aggTypes= new String[aggTypeCount];
        executerProperties.isHighCardinality=new boolean[aggTypeCount];
        int index=0;
        iterator = queryModel.getDimensionAggInfo().iterator();
        for(int i = 0;i < queryModel.getDimensionAggInfo().size();i++)
        {
            DimensionAggregatorInfo dimAggInfo=queryModel.getDimensionAggInfo().get(i);
            if(null!=dimAggInfo)
            {
               
                if(dimAggInfo.getDim().isHighCardinalityDim())
                {
                    executerProperties.isHighCardinality[index]=true;
                }
               // executerProperties.a
            }
            List<String> aggList = iterator.next().getAggList();
            for(int j = 0;j < aggList.size();j++)
            {

                executerProperties.aggTypes[index] = aggList.get(j);
                index++;
            }
        }
        
        for(int i = 0;i < queryModel.getExpressions().size();i++)
        {
            executerProperties.aggTypes[index++] = MolapCommonConstants.CUSTOM;
        }
        
        
        for(int i = 0;i < queryModel.getMsrs().size();i++)
        {
            executerProperties.aggTypes[index++] = queryModel.getMsrs().get(i).getAggName();
        }
        
        executerProperties.aggExpressionStartIndex = executerProperties.aggTypes.length
                - queryModel.getExpressions().size() - queryModel.getMsrs().size();
        
        executerProperties.measureStartIndex = executerProperties.aggTypes.length
                - queryModel.getMsrs().size();
        

        executerProperties.msrMinValue = new double[executerProperties.aggTypes.length];

//        System.arraycopy(msrMinValue, 0, executerProperties.msrMinValue, executerProperties.measureStartIndex,
//                queryModel.getMsrs().size());
        
        for(int i = 0;i < queryModel.getMsrs().size();i++)
        {
            if(queryModel.getMsrs().get(i).getOrdinal()<msrMinValue.length)
            {
                executerProperties.msrMinValue[executerProperties.measureStartIndex+i]=msrMinValue[queryModel.getMsrs().get(i).getOrdinal()];
            }
        }
        if(queryModel.isDetailQuery())
        {
            Arrays.fill(executerProperties.aggTypes, MolapCommonConstants.DUMMY);
        }
        else if(queryModel.isAggTable())
        {
            for(int i = 0;i < executerProperties.aggTypes.length;i++)
            {
                if(executerProperties.aggTypes[i].equals(MolapCommonConstants.COUNT))
                {
                    executerProperties.aggTypes[i]=MolapCommonConstants.SUM;
                }
            }
        }
        // get the mask byte range based on dimension present in the query
        executerProperties.maskByteRanges = QueryExecutorUtil.getMaskedByte(queryModel.getDims(),
                    executerProperties.globalKeyGenerator,executerProperties.hybridStoreModel);    
       
        

        // creating a masked key
        executerProperties.maskedBytes = new int[executerProperties.globalKeyGenerator.getKeySizeInBytes()];

        // update the masked byte
        QueryExecutorUtil.updateMaskedKeyRanges(executerProperties.maskedBytes, executerProperties.maskByteRanges);

        // get the total number of bytes
        executerProperties.byteCount = executerProperties.maskByteRanges.length;
        try
        {
            // get the max key for execution
            executerProperties.maxKey = QueryExecutorUtil.getMaxKeyBasedOnDimensions(queryModel.getDims(),
                    executerProperties.globalKeyGenerator, executerProperties.dimTables);
        }
        catch(KeyGenException e)
        {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem while generating the max key for query: " + queryModel.getQueryId());
            throw new QueryExecutionException(e);
        }

//        updateMsrProperties(queryModel);
    }

//    /**
//     * Below method will be used to get update the measure properties. In case
//     * of aggregate table to calculate the average of any msr we need to get the
//     * sum and count msr
//     * 
//     * @param queryModel
//     */
//    private void updateMsrProperties(MolapQueryExecutorModel queryModel)
//    {
//        /*
//         * To make the avg work, we have to do this dirty work.
//         */
//        executerProperties.avgIndexes = new ArrayList<Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
//        if(queryModel.isAggTable())
//        {
//            QueryExecutorUtility.getMeasureIndexes(queryModel.getMsrs(), MolapCommonConstants.AVERAGE, executerProperties.avgIndexes);
//            if(executerProperties.avgIndexes.size() > 0)
//            {
//                List<Measure> orgMsrs = QueryExecutorUtility.getOriginalMeasures(queryModel.getMsrs(),
//                        queryModel.getCube().getMeasures(queryModel.getFactTable()));
//                List<Integer> countIndexes = new ArrayList<Integer>(20);
//                QueryExecutorUtility.getMeasureIndexes(orgMsrs, MolapCommonConstants.COUNT, countIndexes);
//                executerProperties.countMsrIndex = countIndexes.size() > 0 ? countIndexes.get(0) : -1;
//                if(executerProperties.countMsrIndex == -1)
//                {
//                    Measure measure = queryModel.getCube().getMeasures(queryModel.getFactTable())
//                            .get(queryModel.getCube().getMeasures(queryModel.getFactTable()).size() - 1).getCopy();
//                    measure.setAggName(MolapCommonConstants.SUM);
//                    measure.setName(MolapCommonConstants.GEN_COUNT_MEASURE);
//                    measure.setOrdinal(measure.getOrdinal() + 1);
//                    queryModel.getMsrs().add(measure);
//                    executerProperties.countMsrIndex = queryModel.getMsrs().size() - 1;
//                }
//                else
//                {
//                    executerProperties.isCountMsrExistInCurrTable = true;
//                }
//            }
//        }
//    }
    
    protected long[] getEndKey(Dimension[] currentDim, int currentSliceIndex)
    {
        long[] endKey = new long[currentDim.length];
        
        for(int i = 0;i < endKey.length;i++)
        {
            endKey[i]=getMaxValue(currentDim[i],currentSliceIndex);
        }
        return endKey;
    }
    
    private long getMaxValue(Dimension dim, int currentSliceIndex)
    {
        long max = 0;
        int index=-1;
        // Get data from all the available slices of the cube
        for(InMemoryCube slice : executerProperties.slices)
        {
            index++;
            if(index>currentSliceIndex)
            {
                return max;
            }
            MemberStore memberCache = slice.getMemberCache(dim.getTableName() + '_' + dim.getColName() + '_' + dim.getDimName() + '_' + dim.getHierName());
            if (null == memberCache)
            {
                continue;
            }
            long sliceMax = memberCache.getMaxValue();
            if(max < sliceMax)
            {
                max = sliceMax;
            }
        }
        return max;
    }
    
    protected void updateDimensionAggregatorInfo(Set<DimensionAggregatorInfo> dimensionAggregatorInfoList, CubeDataStore slice)
    {
        Iterator<DimensionAggregatorInfo> iterator = dimensionAggregatorInfoList.iterator();
        KeyGenerator dimKeyGenerator=null;
        DimensionAggregatorInfo dimensionAggregatorInfo = null;
        while(iterator.hasNext())
        {
            dimensionAggregatorInfo = iterator.next();
            dimKeyGenerator= KeyGeneratorFactory.getKeyGenerator(new int[]{slice.getDimCardinality()[dimensionAggregatorInfo.getDim().getOrdinal()]});
            try
            {
                dimensionAggregatorInfo.setNullValueMdkey(dimKeyGenerator.generateKey(new int[]{1}));
            }
            catch(KeyGenException e)
            {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e, "Problem while generating the key");
            }
        }
        
    }
        protected void fillDimensionsFromExpression(List<CustomMolapAggregateExpression> expressions, List<Dimension> dims,
            List<Measure> msrs)
    {
        for(CustomMolapAggregateExpression expression : expressions)
        {
            List<Dimension> dimsFromExpr = expression.getReferredColumns();
            for(Dimension dimFromExpr : dimsFromExpr)
            {
                if(dimFromExpr instanceof Measure)
                {
                    msrs.add((Measure)dimFromExpr);
                }
                else
                {
                    dims.add(dimFromExpr);
                }
            }
        }

    }
}
