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

package org.carbondata.query.executer.impl;

import java.util.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.carbondata.core.metadata.MolapMetadata.Dimension;
import org.carbondata.core.metadata.MolapMetadata.Measure;
import org.carbondata.core.olap.SqlStatement;
import org.carbondata.query.aggregator.CustomMolapAggregateExpression;
import org.carbondata.query.aggregator.dimension.DimensionAggregatorInfo;
import org.carbondata.query.cache.QueryExecutorUtil;
import org.carbondata.query.datastorage.*;
import org.carbondata.query.executer.MolapQueryExecutorModel;
import org.carbondata.query.executer.QueryExecutor;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.util.MolapEngineLogEvent;
import org.carbondata.query.util.QueryExecutorUtility;

/**
 * Abstract class used for query execution and to use to initialize all the properties used for query execution
 *
 * @author K00900841
 */
public abstract class AbstractQueryExecutor implements QueryExecutor {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(AbstractQueryExecutor.class.getName());

    protected QueryExecuterProperties executerProperties;

    public AbstractQueryExecutor(List<Dimension> dimList, String schemaName, String cubeName) {
        executerProperties = new QueryExecuterProperties();
        executerProperties.schemaName = schemaName;
        executerProperties.cubeName = cubeName;
        if (null == cubeName) {
            if (null != dimList && dimList.size() != 0) {
                cubeName = dimList.get(0).getCube().getCubeName();
            }
        }
        executerProperties.cubeUniqueName = schemaName + '_' + cubeName;
        if (dimList != null) {
            executerProperties.dimTables = dimList.toArray(new Dimension[dimList.size()]);
            executerProperties.complexDimensionsMap =
                    QueryExecutorUtility.getComplexDimensionsMap(executerProperties.dimTables);
        }
        Long threadID = Thread.currentThread().getId();
        List<Long> sliceIds = QueryMapper.getSlicesForThread(threadID);
        if (sliceIds == null || sliceIds.size() == 0) {
            executerProperties.slices = InMemoryCubeStore.getInstance()
                    .getActiveSlices(executerProperties.cubeUniqueName);
        } else {
            executerProperties.slices = InMemoryCubeStore.getInstance()
                    .getSllicesbyIds(executerProperties.cubeUniqueName, sliceIds);
        }

    }

    protected void initQuery(MolapQueryExecutorModel queryModel) throws QueryExecutionException {
        Collections.sort(executerProperties.slices, new SliceComparator(queryModel.getFactTable()));
        for (int i = executerProperties.slices.size() - 1; i >= 0; i--) {
            if (executerProperties.slices.get(i).getTableName().equals(queryModel.getFactTable())) {
                executerProperties.globalKeyGenerator =
                        executerProperties.slices.get(i).getKeyGenerator(queryModel.getFactTable());
                executerProperties.hybridStoreModel =
                        executerProperties.slices.get(i).getHybridStoreModel();
                break;
            }
        }

        executerProperties.aggExpDimensions = new ArrayList<Dimension>();
        executerProperties.aggExpMeasures = new ArrayList<Measure>();
        fillDimensionsFromExpression(queryModel.getExpressions(),
                executerProperties.aggExpDimensions, executerProperties.aggExpMeasures);

        executerProperties.dimSortOrder = new byte[0];

        int aggTypeCount = queryModel.getMsrs().size() + queryModel.getExpressions().size();

        Iterator<DimensionAggregatorInfo> iterator = queryModel.getDimensionAggInfo().iterator();
        while (iterator.hasNext()) {
            aggTypeCount += iterator.next().getAggList().size();
        }
        executerProperties.aggTypes = new String[aggTypeCount];
        executerProperties.dataTypes = new SqlStatement.Type[aggTypeCount];

        //initialize all measuretypes
        List<Measure> measures =
                queryModel.getCube().getMeasures(queryModel.getCube().getFactTableName());
        SqlStatement.Type[] allMeasureTypes = new SqlStatement.Type[measures.size()];
        for (int j = 0; j < measures.size(); j++) {
            allMeasureTypes[j] = measures.get(j).getDataType();
        }

        //        Cube cube = queryModel.getCube();
        executerProperties.isHighCardinality = new boolean[aggTypeCount];
        int index = 0;
        iterator = queryModel.getDimensionAggInfo().iterator();
        for (int i = 0; i < queryModel.getDimensionAggInfo().size(); i++) {
            DimensionAggregatorInfo dimAggInfo = queryModel.getDimensionAggInfo().get(i);
            if (null != dimAggInfo) {

                if (dimAggInfo.getDim().isHighCardinalityDim()) {
                    executerProperties.isHighCardinality[index] = true;
                }
                // executerProperties.a
            }
            List<String> aggList = iterator.next().getAggList();
            for (int j = 0; j < aggList.size(); j++) {
                executerProperties.aggTypes[index] = aggList.get(j);
                executerProperties.dataTypes[index] = SqlStatement.Type.STRING;
                index++;
            }
        }

        for (int i = 0; i < queryModel.getExpressions().size(); i++) {
            executerProperties.aggTypes[index] = MolapCommonConstants.CUSTOM;
            executerProperties.dataTypes[index] = SqlStatement.Type.STRING;
            index++;
        }

        for (int i = 0; i < queryModel.getMsrs().size(); i++) {
            Measure measure = queryModel.getMsrs().get(i);
            executerProperties.aggTypes[index] = measure.getAggName();
            executerProperties.dataTypes[index] = measure.getDataType();
            executerProperties.measureOrdinalMap.put(measure.getOrdinal(), index);
            index++;
        }

        // need to initialize data type first
        executerProperties.uniqueValue = QueryExecutorUtility
                .updateUniqueForSlices(queryModel.getFactTable(), queryModel.isAggTable(),
                        executerProperties.slices, allMeasureTypes);

        Object[] msrMinValue = QueryExecutorUtility
                .getMinValueOfSlices(queryModel.getFactTable(), queryModel.isAggTable(),
                        executerProperties.slices, allMeasureTypes);

        executerProperties.aggExpressionStartIndex =
                executerProperties.aggTypes.length - queryModel.getExpressions().size() - queryModel
                        .getMsrs().size();

        executerProperties.measureStartIndex =
                executerProperties.aggTypes.length - queryModel.getMsrs().size();

        executerProperties.msrMinValue = new Object[executerProperties.aggTypes.length];

        // force initialize object[]
        for (int j = 0; j < executerProperties.aggTypes.length; j++) {
            executerProperties.msrMinValue[j] = 0.0;
        }

        //        System.arraycopy(msrMinValue, 0, executerProperties.msrMinValue, executerProperties.measureStartIndex,
        //                queryModel.getMsrs().size());

        for (int i = 0; i < queryModel.getMsrs().size(); i++) {
            if (queryModel.getMsrs().get(i).getOrdinal() < msrMinValue.length) {
                executerProperties.msrMinValue[executerProperties.measureStartIndex + i] =
                        msrMinValue[queryModel.getMsrs().get(i).getOrdinal()];
            }
        }
        if (queryModel.isDetailQuery()) {
            Arrays.fill(executerProperties.aggTypes, MolapCommonConstants.DUMMY);
        } else if (queryModel.isAggTable()) {
            for (int i = 0; i < executerProperties.aggTypes.length; i++) {
                if (executerProperties.aggTypes[i].equals(MolapCommonConstants.COUNT)) {
                    executerProperties.aggTypes[i] = MolapCommonConstants.SUM;
                }
            }
        }
        // get the mask byte range based on dimension present in the query
        executerProperties.maskByteRanges = QueryExecutorUtil
                .getMaskedByte(queryModel.getDims(), executerProperties.globalKeyGenerator,
                        executerProperties.hybridStoreModel);

        // creating a masked key
        executerProperties.maskedBytes =
                new int[executerProperties.globalKeyGenerator.getKeySizeInBytes()];

        // update the masked byte
        QueryExecutorUtil.updateMaskedKeyRanges(executerProperties.maskedBytes,
                executerProperties.maskByteRanges);

        // get the total number of bytes
        executerProperties.byteCount = executerProperties.maskByteRanges.length;
        try {
            // get the max key for execution
            executerProperties.maxKey = QueryExecutorUtil
                    .getMaxKeyBasedOnDimensions(queryModel.getDims(),
                            executerProperties.globalKeyGenerator, executerProperties.dimTables);
        } catch (KeyGenException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem while generating the max key for query: " + queryModel.getQueryId());
            throw new QueryExecutionException(e);
        }

        //        updateMsrProperties(queryModel);
    }

    protected long[] getEndKey(Dimension[] currentDim, int currentSliceIndex) {
        long[] endKey = new long[currentDim.length];

        for (int i = 0; i < endKey.length; i++) {
            endKey[i] = getMaxValue(currentDim[i], currentSliceIndex);
        }
        return endKey;
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

    private long getMaxValue(Dimension dim, int currentSliceIndex) {
        long max = 0;
        int index = -1;
        // Get data from all the available slices of the cube
        for (InMemoryCube slice : executerProperties.slices) {
            index++;
            if (index > currentSliceIndex) {
                return max;
            }
            MemberStore memberCache = slice.getMemberCache(
                    dim.getTableName() + '_' + dim.getColName() + '_' + dim.getDimName() + '_' + dim
                            .getHierName());
            if (null == memberCache) {
                continue;
            }
            long sliceMax = memberCache.getMaxValue();
            if (max < sliceMax) {
                max = sliceMax;
            }
        }
        return max;
    }

    protected void updateDimensionAggregatorInfo(
            Set<DimensionAggregatorInfo> dimensionAggregatorInfoList, CubeDataStore slice) {
        Iterator<DimensionAggregatorInfo> iterator = dimensionAggregatorInfoList.iterator();
        KeyGenerator dimKeyGenerator = null;
        DimensionAggregatorInfo dimensionAggregatorInfo = null;
        while (iterator.hasNext()) {
            dimensionAggregatorInfo = iterator.next();
            dimKeyGenerator = KeyGeneratorFactory.getKeyGenerator(new int[] {
                    slice.getDimCardinality()[dimensionAggregatorInfo.getDim().getOrdinal()] });
            try {
                dimensionAggregatorInfo
                        .setNullValueMdkey(dimKeyGenerator.generateKey(new int[] { 1 }));
            } catch (KeyGenException e) {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                        "Problem while generating the key");
            }
        }

    }

    protected void fillDimensionsFromExpression(List<CustomMolapAggregateExpression> expressions,
            List<Dimension> dims, List<Measure> msrs) {
        for (CustomMolapAggregateExpression expression : expressions) {
            List<Dimension> dimsFromExpr = expression.getReferredColumns();
            for (Dimension dimFromExpr : dimsFromExpr) {
                if (dimFromExpr instanceof Measure) {
                    msrs.add((Measure) dimFromExpr);
                } else {
                    dims.add(dimFromExpr);
                }
            }
        }

    }

    private final class SliceComparator implements Comparator<InMemoryCube> {
        private String tableName;

        private SliceComparator(String tableName) {
            this.tableName = tableName;
        }

        @Override public int compare(InMemoryCube o1, InMemoryCube o2) {
            int loadId1 = !o1.getTableName().equals(tableName) ? Integer.MIN_VALUE : o1.getLoadId();
            int loadId2 = !o2.getTableName().equals(tableName) ? Integer.MIN_VALUE : o2.getLoadId();
            if (loadId1 < 0) {
                return -1;
            } else if (loadId2 < 0) {
                return 1;
            } else {
                return loadId1 - loadId2;
            }
        }
    }
}
