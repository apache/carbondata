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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.core.metadata.MolapMetadata.Dimension;
import org.carbondata.query.executer.AbstractMolapExecutor;
import org.carbondata.query.executer.MolapQueryExecutorModel;
import org.carbondata.query.holders.MolapResultHolder;
import org.carbondata.query.queryinterface.filter.MolapFilterInfo;
import org.carbondata.query.util.MolapEngineLogEvent;

//import mondrian.rolap.SqlStatement;
//import mondrian.rolap.SqlStatement.Type;
//import mondrian.olap.ResourceLimitExceededException;

/**
 * This class is the concrete implementation for MOLAP Query Execution.
 * It is responsible for handling the query execution.
 *
 * @author K00900841
 */
public class InMemoryQueryExecutor extends AbstractMolapExecutor implements RowCounterListner {

    //    /**
    //     * 
    //     */
    //    private static final int DIMENSION_DEFAULT = 1;
    //
    //    /**
    //     * 
    //     */
    //    private Dimension[] dimTables;
    //
    //    /**
    //     * 
    //     */
    //    private String cubeName;
    //    
    //    /**
    //     * 
    //     */
    //    private String schemaName;
    //    
    //    
    //    /**
    //     * 
    //     */
    //    private String cubeUniqueName;
    //
    //    /**
    //     * 
    //     */
    //    private KeyGenerator keyGenerator;
    //
    //    /**
    //     * 
    //     */
    //    private List<InMemoryCube> slices;
    //
    //    /**
    //     * 
    //     */
    //    private String queryId;
    //    
    //    
    //    /**
    //     * queryDimAndSortOrder
    //     */
    //    private byte[] queryDimAndSortOrder;
    //
    //    
    //    /**
    //     * uniqueValue
    //     */
    //    private double[] uniqueValue;
    //    
    //    
    //    /**
    //     * msrConstraints
    //     */
    //    private List<GroupMeasureFilterModel> msrConstraints;
    //    
    //    /**
    //     * topNModel
    //     */
    //    private TopNModel topNModel;
    //    
    //    /**
    //     * msrSortModel
    //     */
    //    private MeasureSortModel msrSortModel;
    //    
    //    /**
    //     * sliceExecutors
    //     */
    //    private List<ParallelSliceExecutor> sliceExecutors = new ArrayList<ParallelSliceExecutor>();
    //    
    //    /**
    //     * interrupted
    //     */
    //    private boolean interrupted;
    //    
    //    
    //    /**
    //     * pagination Required or not
    //     */
    //    private boolean paginationRequired;
    //    
    //    /**
    //     * Row limit
    //     */
    //    private int rowLimit = Integer.parseInt(MolapProperties.getInstance().getProperty(MolapCommonConstants.MOLAP_RESULT_SIZE_KEY,MolapCommonConstants.MOLAP_RESULT_SIZE_DEFAULT));
    //    
    //    /**
    //     * It is used to interrupt the query when row limit exceeds configurable limit.
    //     */
    //    private QueryRowCounter rowCounter;
    //
    //    private boolean isColumnar;

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(InMemoryQueryExecutor.class.getName());

    @Override public void execute(MolapQueryExecutorModel queryModel)
            throws IOException, KeyGenException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");
    }

    @Override
    public void executeHierarichies(String hName, final int[] dims, List<Dimension> dimNames,
            Map<Dimension, MolapFilterInfo> constraints, MolapResultHolder hIterator)
            throws IOException, KeyGenException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");

    }

    @Override public void executeDimensionCount(Dimension dimension, MolapResultHolder hIterator)
            throws IOException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");

    }

    @Override public void executeAggTableCount(String table, MolapResultHolder hIterator)
            throws IOException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");

    }

    @Override public long executeTableCount(String table) throws IOException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");
        return 0;
    }

    @Override public void executeDimension(String hName, Dimension dim, final int[] dims,
            Map<Dimension, MolapFilterInfo> constraints, MolapResultHolder hIterator)
            throws IOException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");

    }

    @Override public void interruptExecutor() {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");

    }

    @Override public void rowLimitExceeded() throws Exception {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");

    }

    @Override protected Long getMaxValue(Dimension dim) throws IOException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");
        return 0L;
    }

    @Override public long[] getSurrogates(List<String> dimMem, Dimension dimName)
            throws IOException {
        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "UNSUPPORT Operation");
        return new long[0];
    }

    ////    public InMemoryQueryExecutor(List<Dimension> dimList, String cubeName, String schemaName)
    ////    {
    //////        cubeName = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.CUBE_NAME);
    //////        schemaName = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.SCHEMA_NAME);
    ////        
    ////        this.cubeName = cubeName;//(String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.CUBE_NAME);
    ////        this.schemaName = schemaName;//(String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.SCHEMA_NAME);
    ////        
    ////        
    ////        if(null==cubeName)
    ////        { if(null!=dimList && dimList.size()!=0)
    ////            {
    ////                cubeName=dimList.get(0).getCube().getCubeName();
    ////            }
    ////        }
    ////        cubeUniqueName=schemaName+'_'+cubeName;
    //////        queryId = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.QUERY_ID);
    ////        if(dimList != null)
    ////        {
    ////            dimTables = dimList.toArray(new Dimension[dimList.size()]);
    ////            keyGenerator = getKeyGenerator(dimTables);
    ////        }
    ////
    ////        Long threadID = Thread.currentThread().getId();
    ////
    ////        List<Long> sliceIds = QueryMapper.getSlicesForThread(threadID);
    ////
    ////        if(sliceIds == null || sliceIds.size() == 0 )
    ////        {
    ////            // When the query is validated then slices applicable is not set
    ////            // because
    ////            // as per the current flow while creating the query from the string
    ////            // Object itself they are
    ////            // validating the filter members. so to get t`he filter members we
    ////            // need to know all the slices
    ////            // So in this case we are starting the query for the first time
    ////            // here.
    ////            slices = InMemoryCubeStore.getInstance().getActiveSlices(cubeUniqueName);
    ////
    ////        }
    ////        else
    ////        {
    ////            slices = InMemoryCubeStore.getInstance().getSllicesbyIds(cubeUniqueName, sliceIds);
    ////        }
    ////        
    ////        rowCounter = new QueryRowCounter(rowLimit);
    ////        rowCounter.registerRowCountListner(this);
    ////        isColumnar=Boolean.parseBoolean(MolapProperties.getInstance().getProperty(
    ////                MolapCommonConstants.IS_COLUMNAR_STORAGE,
    ////                MolapCommonConstants.IS_COLUMNAR_STORAGE_DEFAULTVALUE));
    ////    }
    //    
    //    public InMemoryQueryExecutor(List<Dimension> dimList, String schemaName,String cubeName)
    //    {
    //        this.schemaName = schemaName;
    //        this.cubeName = cubeName;
    //        if(null==cubeName)
    //        { if(null!=dimList && dimList.size()!=0)
    //            {
    //                cubeName=dimList.get(0).getCube().getCubeName();
    //            }
    //        }
    //        cubeUniqueName=schemaName+'_'+cubeName;
    ////        queryId = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.QUERY_ID);
    //        if(dimList != null)
    //        {
    //            dimTables = dimList.toArray(new Dimension[dimList.size()]);
    //            keyGenerator = getKeyGenerator(dimTables);
    //        }
    //
    //        Long threadID = Thread.currentThread().getId();
    //
    //        List<Long> sliceIds = QueryMapper.getSlicesForThread(threadID);
    //
    //        if(sliceIds == null || sliceIds.size() == 0 )
    //        {
    //            // When the query is validated then slices applicable is not set
    //            // because
    //            // as per the current flow while creating the query from the string
    //            // Object itself they are
    //            // validating the filter members. so to get t`he filter members we
    //            // need to know all the slices
    //            // So in this case we are starting the query for the first time
    //            // here.
    //            slices = InMemoryCubeStore.getInstance().getActiveSlices(cubeUniqueName);
    //
    //        }
    //        else
    //        {
    //            slices = InMemoryCubeStore.getInstance().getSllicesbyIds(cubeUniqueName, sliceIds);
    //        }
    //        isColumnar=Boolean.parseBoolean(MolapProperties.getInstance().getProperty(
    //                MolapCommonConstants.IS_COLUMNAR_STORAGE,
    //                MolapCommonConstants.IS_COLUMNAR_STORAGE_DEFAULTVALUE));
    //        rowCounter = new QueryRowCounter(rowLimit);
    //        rowCounter.registerRowCountListner(this);
    //    }
    //    
    //    /**
    //     * 
    //     * @param dimList
    //     * @param useSlices
    //     */
    //    public InMemoryQueryExecutor(List<Dimension> dimList,boolean useSlices, String schemaName,String cubeName)
    //    {
    //        this.cubeName = cubeName;
    //        this.schemaName = schemaName;
    //        
    //        
    //        if(null==cubeName)
    //        { if(null!=dimList && dimList.size()!=0)
    //            {
    //                cubeName=dimList.get(0).getCube().getCubeName();
    //            }
    //        }
    //        cubeUniqueName=schemaName+'_'+cubeName;
    ////        queryId = (String)RolapConnection.THREAD_LOCAL.get().get(RolapConnection.QUERY_ID);
    //        if(dimList != null)
    //        {
    //            dimTables = dimList.toArray(new Dimension[dimList.size()]);
    //            keyGenerator = getKeyGenerator(dimTables);
    //        }
    //
    //        slices = InMemoryCubeStore.getInstance().getActiveSlices(cubeUniqueName);
    //        
    //        rowCounter = new QueryRowCounter(rowLimit);
    //        rowCounter.registerRowCountListner(this);
    //        isColumnar=Boolean.parseBoolean(MolapProperties.getInstance().getProperty(
    //                MolapCommonConstants.IS_COLUMNAR_STORAGE,
    //                MolapCommonConstants.IS_COLUMNAR_STORAGE_DEFAULTVALUE));
    //    }
    //    
    //
    //    
    //    /**
    //     * This method is responsible for creating model and delegate request to executor.
    //     * 
    //     * @see com.huawei.unibi.molap.engine.executer.MolapExecutor#execute(MolapQueryExecutorModel queryModel)
    //     *
    //     */
    //    public void execute(MolapQueryExecutorModel queryModel) throws IOException, KeyGenException
    //    {
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "******************** Columnar="+ isColumnar);
    //        if(isColumnar)
    //        {
    //            queryModel.setFilterInHierGroups(false);
    //        }
    //        Thread.currentThread().setName("Query Thread" + queryId);
    //        if(null==slices ||slices.size()==0)
    //        {
    //            queryModel.gethIterator().setObject(new Object[0][0]);
    //        }
    //        byte[] maxKey= null;
    //        int byteCount=0;
    //        int[] maskByteRanges = null;
    //        int[] maskedBytes = null;
    //        byte[] maxKeyNormalized= null;
    //        int byteCountNormalized=0;
    //        int[] maskByteRangesNormalized = null;
    //        int[] maskedBytesNormalized = null;
    //        Cube cube = queryModel.getCube();
    //        MolapResultHolder hIterator = queryModel.gethIterator();
    //        String factTable = queryModel.getFactTable();
    //        List<Measure> msrs = queryModel.getMsrs();
    //        Map<Dimension, MolapFilterInfo> constraints = queryModel.getConstraints();
    //        Map<Dimension, MolapFilterInfo> constraintsAfterTopN = queryModel.getConstraintsAfterTopN();
    //        
    //        constraintsAfterTopN = updateConstrainstsAfterTopN(queryModel, cube, constraintsAfterTopN);
    //        boolean properties = queryModel.isProperties();
    //        this.msrConstraints = queryModel.getMsrFilterModels();
    ////        this.topNModel = queryModel.getTopNModel();
    //        this.msrSortModel = queryModel.getSortModel();
    //        paginationRequired = queryModel.isPaginationRequired();
    //        if(queryModel.getActualQueryDims() == null)
    //        {
    //            queryModel.setActualQueryDims(queryModel.getDims());
    //        }
    //        Dimension[] queryDims = paginationRequired?getCopy(queryModel.getActualQueryDims()):queryModel.getDims();
    //        queryDims=updateDimsForPaginationInCaseOfSliceFilterPresent(queryDims,queryModel.getDims(),queryModel.isSliceFilterPresent());
    //        queryDims = getDimsFromConstraintsInCaseOfPagination(constraints, queryDims);
    //        
    //        logQueryString(queryDims, msrs, constraints);
    //        List<Measure> calcMeasures = queryModel.getCalcMeasures();
    //        if(queryModel.getRowLimit() > 0)
    //        {
    //            rowCounter.setRowLimit(queryModel.getRowLimit());
    //        }
    //        QueryExecutorUtil.updateContentMatchMolapFilters(constraints, slices, this, queryModel.isFilterInHierGroups(), cube, queryModel.isUseNonVisualTotal());
    //        QueryExecutorUtil.updateContentMatchMolapFilters(constraintsAfterTopN, slices, this, queryModel.isFilterInHierGroups(), cube, queryModel.isUseNonVisualTotal());
    //        
    ////        Map<RestructureHolder, List<Map<ByteArrayWrapper, MeasureAggregator[]>>> sliceMetaAndDataMap = new HashMap<RestructureHolder, List<Map<ByteArrayWrapper, MeasureAggregator[]>>>();
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "CUBE : " + cube.getCubeName() + "Execution for the query with id " + queryId
    //                + " started on Table: " + factTable);
    //
    //        // Go through queryDims, if it is normalized, 
    //        // then replace it with column present in fact table
    //        // Store this info so that later we can switch it
    //        boolean isNormalizedCase = false;
    //        List<Integer> normalizedStartingIndexArray = new ArrayList<Integer>(20);
    //        Dimension[] replacedDims = new Dimension[queryDims.length];
    //        isNormalizedCase = checkAndReplaceNormalizedDims(queryDims, replacedDims, normalizedStartingIndexArray);
    //        List<Int2ObjectMap<List<int[]>>> listOfMapsOfDimValues = null;
    //        if(isNormalizedCase)
    //        {
    //            // Get the possible values of fact table dim for normalized case
    //            listOfMapsOfDimValues = findAndGetFactDimValues(cube, constraints, replacedDims, normalizedStartingIndexArray);
    //            constraints = updateConstraintsForNormalized(constraints, slices, cube);
    //            replacedDims = updateDimensionsNormalized(replacedDims,isNormalizedCase);
    //        }
    //       
    ////        Query query= (Query)RolapConnection.THREAD_LOCAL.get().get("QUERY_OBJ");
    //        this.queryDimAndSortOrder = queryModel.getSortOrder();
    //        if(queryDimAndSortOrder == null)
    //        {
    //            queryDimAndSortOrder = new byte[queryDims.length];
    //        }
    //        
    //        if(constraints.get(null) != null)
    //        {
    //            hIterator.setObject(new Object[0][0]);
    //            return;
    //        }
    //
    //        Map<Dimension, MolapPredicates> molapPredicates = QueryExecutorUtil.getMolapPredicates(constraints, dimTables,
    //                slices, queryDims.length == 0 ? false : queryDims[0].isActualCol(), queryModel.isFilterInHierGroups(),cube,factTable);
    //        MolapSegmentHeader molapSegmentHeader = null;//MolapCacheManager.getInstance().createMolapSegmentHeader(
    //                //cube.getCubeName(), factTable, isNormalizedCase?replacedDims:queryDims, molapPredicates, dimTables, paginationRequired);
    //
    //        long t1 = System.currentTimeMillis();
    //        int []measureIndexToReadFromPaginationFile = null;
    //        if(paginationRequired)
    //        {
    //            List<Measure> actualCalMeasure = queryModel.getActualCalMeasure();
    //            List<Measure> actualMsrs = queryModel.getActualMsrs();
    //            actualMsrs = updateMeasureInCaseOfPagination(queryModel, cube, factTable, actualMsrs);
    ////            if(actualMsrs.size() < msrs.size())
    ////            {
    ////                actualMsrs = msrs;
    ////            }
    //            actualMsrs = getUnifiedMeasuresForPagination(actualMsrs, msrs);
    //            msrs=actualMsrs;
    //            measureIndexToReadFromPaginationFile = new int[msrs.size()+calcMeasures.size()];
    //            getMeasureIndexToRead(measureIndexToReadFromPaginationFile, msrs,actualMsrs, calcMeasures,actualCalMeasure);
    //            calcMeasures=actualCalMeasure;
    //        }
    //        /*
    //         * To make the avg work, we have to do this dirty work.
    //         */
    //        boolean isCtMsrEXstInCurr = false;
    //        int countMsrsIndex = -1;
    //        List<Integer> avgIndexes = new ArrayList<Integer>(20);
    //        if(queryModel.isAggTable())
    //        {
    //            QueryExecutorUtility.getMeasureIndexes(msrs, MolapCommonConstants.AVERAGE,avgIndexes);
    //            if(avgIndexes.size() > 0)
    //            {
    //                List<Measure> orgMsrs = QueryExecutorUtility.getOriginalMeasures(msrs, cube.getMeasures(factTable));
    //                List<Integer> countIndexes = new ArrayList<Integer>(20);
    //                QueryExecutorUtility.getMeasureIndexes(orgMsrs, MolapCommonConstants.COUNT,countIndexes);
    //                countMsrsIndex = countIndexes.size()>0?countIndexes.get(0):-1;
    //                if(countMsrsIndex == -1)
    //                {
    //                    Measure measure = cube.getMeasures(factTable).get(cube.getMeasures(factTable).size()-1).getCopy();
    //                    measure.setAggName(MolapCommonConstants.SUM);
    //                    measure.setName(MolapCommonConstants.GEN_COUNT_MEASURE);
    //                    measure.setOrdinal(measure.getOrdinal()+1);
    //                    msrs.add(measure);
    //                    measureIndexToReadFromPaginationFile = getMeasureIndexToReadFromPaginationFile(measureIndexToReadFromPaginationFile);
    //                    countMsrsIndex = msrs.size()-1;
    //                }
    //                else
    //                {
    //                    isCtMsrEXstInCurr = true;
    //                }
    //            }
    //        }
    //
    //        updateUniqueAndMinValueForSlice(factTable,msrs,queryModel.isAggTable());
    //        maskByteRanges=QueryExecutorUtil.getMaskedByte(queryDims, keyGenerator);
    //        maskedBytes = new int[keyGenerator.getKeySizeInBytes()];
    //        QueryExecutorUtil.updateMaskedKeyRanges(maskedBytes, maskByteRanges);
    //        byteCount=maskByteRanges.length;
    //        maxKey=QueryExecutorUtil.getMaxKeyBasedOnDimensions(queryDims, keyGenerator,dimTables);
    //        KeyGenerator keyGenNormalized = null;
    //        if(isNormalizedCase)
    //        {
    //            keyGenNormalized = getKeyGenerator(dimTables, false); 
    //            maskByteRangesNormalized=QueryExecutorUtil.getMaskedByte(replacedDims, keyGenNormalized);
    //            maskedBytesNormalized = new int[keyGenNormalized.getKeySizeInBytes()];
    //            QueryExecutorUtil.updateMaskedKeyRanges(maskedBytesNormalized, maskByteRangesNormalized);
    //            byteCountNormalized=maskByteRangesNormalized.length;
    //            maxKeyNormalized=QueryExecutorUtil.getMaxKeyBasedOnDimensions(replacedDims, keyGenNormalized,dimTables);
    //        }
    //        
    //        //Check if it can fill data from paginated cache. 
    //        //It can fill  the data if and only if pagination is enabled and requested query data is in harddisk. 
    //        if(fillDataFromPaginatedCache(queryModel, hIterator, factTable, msrs, properties, t1, isCtMsrEXstInCurr,
    //                countMsrsIndex, avgIndexes,paginationRequired,measureIndexToReadFromPaginationFile,topNModel,isNormalizedCase?keyGenNormalized:keyGenerator,isNormalizedCase?replacedDims:queryModel.getDims()))
    //        {
    //            //No need to go beyond if the data is alrday filled.
    //            return;
    //        }
    //        
    //        Dimension topNDim = null;
    //        //Fill the topN model if user requested for topN query.
    //        if(topNModel != null)
    //        {
    //            topNDim = fillTopNModelForQuery(queryModel, maskByteRanges, maskByteRangesNormalized, queryDims,
    //                    isNormalizedCase, replacedDims, countMsrsIndex, avgIndexes, keyGenNormalized);
    //        } 
    ////        if(queryModel.getMeasureFilterProcessorModel() != null)
    ////        {
    ////            MeasureFilterProcessorModel filterProcessorModel = queryModel.getMeasureFilterProcessorModel();
    ////            List<Integer> bytePos = new ArrayList<Integer>(20);
    ////            // create masked key till what level we are applying measure filter
    ////            // group
    ////            topNDim = filterProcessorModel.getDimension();
    ////            filterProcessorModel.setMaskedBytes(QueryExecutorUtil.getMaskedBytesForTopN(isNormalizedCase ? replacedDims
    ////                    : queryDims, isNormalizedCase ? keyGenNormalized : keyGenerator,
    ////                    filterProcessorModel.getDimIndex(), queryModel.getActualQueryDims(),
    ////                    queryModel.getActualDimsRows(), queryModel.getActualDimsCols(),
    ////                    topNDim, true, isNormalizedCase ? maskByteRangesNormalized : maskByteRanges, bytePos));
    ////            filterProcessorModel.setMaskedBytesPos(QueryExecutorUtil.convertIntegerListToIntArray(bytePos));
    ////        }
    //        List<SliceExecutionInfo> infos = new ArrayList<SliceExecutionInfo>(20);
    //        SliceMetaData sliceMataData = null;
    //        //CHECKSTYLE:OFF    Approval No:Approval-297
    //        for(InMemoryCube slice : slices)//CHECKSTYLE:ON
    //        {
    //            if(null != slice.getDataCache(factTable) && !interrupted)
    //            {
    //                sliceMataData = slice.getRsStore().getSliceMetaCache(factTable);
    //            }
    //            else
    //            {
    //                 continue;
    ////                return null;
    //            }
    //            
    //            //CHECKSTYLE:OFF    Approval No:Approval-297
    //            SliceExecutionInfo info= getSliceExecutionInfo(slice, factTable, queryDims, constraints, queryModel, isNormalizedCase, msrs,
    //                    measureIndexToReadFromPaginationFile, maskByteRanges, maskedBytes, byteCount, maxKey, replacedDims,
    //                    keyGenNormalized, maskByteRangesNormalized, topNDim, calcMeasures, molapSegmentHeader, avgIndexes,
    //                    countMsrsIndex, maxKeyNormalized, byteCountNormalized, maskedBytesNormalized,
    //                    listOfMapsOfDimValues, normalizedStartingIndexArray,sliceMataData,constraintsAfterTopN,cube);
    //            info.setLimit(queryModel.getLimit());
    //            info.setDetailQuery(queryModel.isDetailQuery());
    //            //CHECKSTYLE:ON
    //            if(null!=info)
    //            {
    //                infos.add(info);
    //            }
    //        }
    //        
    //        SliceMetaData newSliceMetaDataForCache = new SliceMetaData();
    //        InMemoryCube sliceForCache=null;
    //      //CHECKSTYLE:OFF    Approval No:Approval-297
    //        for(InMemoryCube slice: slices)//CHECKSTYLE:ON
    //        {
    //            newSliceMetaDataForCache = slice.getRsStore().getSliceMetaCache(factTable);
    //            if(newSliceMetaDataForCache == null)
    //            {
    //            	 continue;
    //            }
    //            if(null == newSliceMetaDataForCache.getNewActualDimensions()
    //                    && null == newSliceMetaDataForCache.getNewKeyGenerator()
    //                    && null == newSliceMetaDataForCache.getNewMeasures()
    //                    && null == newSliceMetaDataForCache.getNewActualDimLens())
    //            {
    //                sliceForCache = slice;
    //                break;
    //            }
    //        }
    //
    //        SliceExecutionInfo infoForCache = null;
    //        if(sliceForCache != null)
    //        {
    //            infoForCache = getSliceExecutionInfo(sliceForCache, factTable, queryDims, constraints, queryModel, isNormalizedCase, msrs,
    //                    measureIndexToReadFromPaginationFile, maskByteRanges, maskedBytes, byteCount, maxKey, replacedDims,
    //                    keyGenNormalized, maskByteRangesNormalized, topNDim, calcMeasures, molapSegmentHeader, avgIndexes,
    //                    countMsrsIndex, maxKeyNormalized, byteCountNormalized, maskedBytesNormalized,
    //                    listOfMapsOfDimValues, normalizedStartingIndexArray,newSliceMetaDataForCache,constraintsAfterTopN,cube);
    //            infoForCache.setLimit(queryModel.getLimit());
    //            infoForCache.setDetailQuery(queryModel.isDetailQuery());
    //        }
    //
    //        Map<ByteArrayWrapper, MeasureAggregator[]> data=null;
    //        QueryResult result=null;
    //        if(infos.size()>0)
    //        {
    //            
    //            //Check if it can fill data from Molap cache. 
    //            //It can fill  the data if and only if pagination is disabled and Molap cache is enabled and requested query data is in cache. 
    ////            if(fillDataFromMolapCache(queryModel, hIterator, factTable, msrs, properties, queryDims, isNormalizedCase,
    ////                    molapSegmentHeader, t1, isCtMsrEXstInCurr, countMsrsIndex, avgIndexes,paginationRequired,infoForCache))
    ////            {
    ////                //No need to go beyond if the data is alrday filled.
    ////                return;
    ////            }
    //            
    ////           data = submitExecutor(infos,hIterator,infoForCache);
    //           result = submitExecutorDetailQuery(infos,hIterator,infoForCache);
    //        }
    ////        if(null==data || data.isEmpty())
    ////        {
    //////            isInterrupted();
    ////            hIterator.setObject(new Object[0][0]);
    ////            hIterator.setKeys(new ArrayList<MolapKey>());
    ////            hIterator.setValues(new ArrayList<MolapValue>());
    ////
    ////            return;
    ////        }
    //        
    //        if(null==result || result.size()<1)
    //        {
    //            isInterrupted();
    //            hIterator.setObject(new Object[0][0]);
    //            hIterator.setKeys(new ArrayList<MolapKey>());
    //            hIterator.setValues(new ArrayList<MolapValue>());
    //
    //            return;
    //        }
    ////        isInterrupted();
    //        if(paginationRequired)
    //        {
    //            
    //            List<Measure> actualMsrs = queryModel.getActualMsrs();
    //            actualMsrs = getUnifiedMeasuresForPagination(actualMsrs, msrs);
    //            msrs=getUpdatedMsrs(msrs, actualMsrs.toArray(new Measure[actualMsrs.size()]));
    //        }
    ////        updatedWithCurrentUnique(data, msrs);
    //        if(null != result)
    //        {
    //            updatedWithCurrentUnique(result, msrs);
    //        }
    //        
    //       /* checkAndRaiseLimitException(queryModel, maskedBytes, maskedBytesNormalized, queryDims, isNormalizedCase,
    //                replacedDims, keyGenNormalized, data);*/
    //        
    //        
    //       if(!queryModel.isSparkExecution())
    //        {
    //
    //        createDataFromAggregates(slices, hIterator, data, paginationRequired ? (isNormalizedCase ? replacedDims
    //                : queryModel.getDims()) : (isNormalizedCase ? replacedDims : queryDims), properties, msrs.size(),
    //                false, factTable, avgIndexes, countMsrsIndex, isCtMsrEXstInCurr,
    //                msrs.toArray(new Measure[msrs.size()]), isNormalizedCase ? keyGenNormalized : keyGenerator,
    //                isNormalizedCase ? maskedBytesNormalized : maskedBytes, queryModel.isOnlyNameColumnReq(), queryModel);
    //        }
    //       
    //       else
    //       {
    //        createDataFromAggregatesSpark(slices, hIterator, result, paginationRequired ? (isNormalizedCase ? replacedDims
    //                : queryModel.getDims()) : (isNormalizedCase ? replacedDims : queryDims), properties, msrs.size(),
    //                false, factTable, avgIndexes, countMsrsIndex, isCtMsrEXstInCurr,
    //                msrs.toArray(new Measure[msrs.size()]), isNormalizedCase ? keyGenNormalized : keyGenerator,
    //                isNormalizedCase ? maskedBytesNormalized : maskedBytes, queryModel.isOnlyNameColumnReq(), cube, factTable, queryModel.isDetailQuery());
    //        
    //       }
    //       
    ////        }
    ////        else
    ////        {
    ////            createDataFromAggregatesNormalized(slices, hIterator, data, paginationRequired?queryModel.getDims():queryDims, properties, msrs.size(), false,
    ////                    factTable, avgIndexes, countMsrsIndex, isCtMsrEXstInCurr, listOfMapsOfDimValues, replacedDims, 
    ////                    normalizedStartingIndexArray, msrs.toArray(new Measure[msrs.size()]),queryModel.getRowLimit(),maskedBytes,queryModel.isOnlyNameColumnReq());
    ////        }
    ////        
    //
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Total time taken Execute the query with id "
    //                + queryId + " is : " + (System.currentTimeMillis() - t1));
    //    }
    //    
    //    private void logQueryString(Dimension[] dims, List<Measure> msrs,
    //          Map<Dimension, MolapFilterInfo> constraints)
    //  {
    //      
    //      StringBuffer buffer = new StringBuffer();
    //      buffer.append("&&&&&&&&&&&&&&&&&&&&&&&&");
    //      buffer.append("Dimensions : ");
    //      for(Dimension dimension : dims)
    //      {
    //          buffer.append(dimension.getName());
    //          buffer.append(",");
    //      }
    //      buffer.append(" Measures : ");
    //      for(Measure msr : msrs)
    //      {
    //          buffer.append(msr.getName());
    //          buffer.append(",");
    //      }
    //      
    //      buffer.append(" DimensionConstraints : ");
    //      for(Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet())
    //      {
    //          buffer.append(entry.getKey()!=null?entry.getKey().getName():entry.getKey());
    //          buffer.append(" (");
    //          buffer.append(" Include : ");
    //          for(String filt : entry.getValue().getIncludedMembers())
    //          {
    //              buffer.append(filt);
    //              buffer.append(",");
    //          }
    //          
    //          buffer.append(" Exclude : ");
    //          for(String filt : entry.getValue().getExcludedMembers())
    //          {
    //              buffer.append(filt);
    //              buffer.append(",");
    //          }
    //          buffer.append(" )");
    //      }
    //      
    //      LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG ,buffer.toString());
    //      
    //  }
    //
    //    private int[] getMeasureIndexToReadFromPaginationFile(int[] measureIndexToReadFromPaginationFile)
    //    {
    //        if(paginationRequired)
    //        {
    //            int[] newMeasureIndex = new int[measureIndexToReadFromPaginationFile.length + 1];
    //            System.arraycopy(measureIndexToReadFromPaginationFile, 0, newMeasureIndex, 0,
    //                    measureIndexToReadFromPaginationFile.length);
    //            newMeasureIndex[newMeasureIndex.length - 1] = measureIndexToReadFromPaginationFile.length;
    //            measureIndexToReadFromPaginationFile = newMeasureIndex;
    //        }
    //        return measureIndexToReadFromPaginationFile;
    //    }
    //
    //    private void checkAndRaiseLimitException(MolapQueryExecutorModel queryModel, int[] maskedBytes,
    //            int[] maskedBytesNormalized, Dimension[] queryDims, boolean isNormalizedCase, Dimension[] replacedDims,
    //            KeyGenerator keyGenNormalized, Map<ByteArrayWrapper, MeasureAggregator[]> data)
    //    {
    ////        if(!queryModel.isSliceFilterPresent())
    ////        {
    ////            if(!queryModel.isRangeFilter())
    ////            {
    ////                if(data.size() > rowLimit && !paginationRequired && queryModel.getRowLimit()<=0) 
    ////                {
    ////                    raiseLimitExceedException(data, isNormalizedCase ? replacedDims : queryDims,
    ////                            isNormalizedCase ? maskedBytesNormalized : maskedBytes, isNormalizedCase ? keyGenNormalized
    ////                                    : keyGenerator);
    ////                }
    ////           }
    ////        }
    //    }
    //
    //    private List<Measure> updateMeasureInCaseOfPagination(MolapQueryExecutorModel queryModel, Cube cube,
    //            String factTable, List<Measure> actualMsrs)
    //    {
    //        if(queryModel.isAggTable())
    //        {
    //            actualMsrs=updateActualMeasureBaseOnRequestTable(actualMsrs,cube, factTable);
    //            actualMsrs=getUpdatedMeasureForAggTable(actualMsrs, true,
    //                    cube.getAutoAggregateType().equals(MolapCommonConstants.MOLAP_AUTO_TYPE_VALUE)
    //                            || cube.getAutoAggregateType().equals(MolapCommonConstants.MOLAP_MANUAL_TYPE_VALUE));
    //        }
    //        return actualMsrs;
    //    }
    //
    //    private Dimension[] getDimsFromConstraintsInCaseOfPagination(Map<Dimension, MolapFilterInfo> constraints,
    //            Dimension[] queryDims)
    //    {
    //        Dimension[] sliceFilterDimension= null;
    //        if(paginationRequired)
    //        {
    //            sliceFilterDimension=getSliceFilterDimsFromConstraints(constraints,queryDims);
    //            Dimension[] temp = new Dimension[sliceFilterDimension.length+queryDims.length];
    //            int index=0;
    //            for(int i = 0;i < queryDims.length;i++)
    //            {
    //                temp[index++]=queryDims[i];
    //            }
    //            for(int i = 0;i < sliceFilterDimension.length;i++)
    //            {
    //                temp[index++]=sliceFilterDimension[i];
    //            }
    //            queryDims=temp;
    //        }
    //        return queryDims;
    //    }
    //
    //    private List<Measure>getUpdatedMeasureForAggTable(List<Measure> actualMsr, boolean isAgg, boolean isAutoAgg)
    //    {
    //        List<Measure> newMsr = new ArrayList<MolapMetadata.Measure>(actualMsr.size());
    //        for(int i = 0;i < actualMsr.size();i++)
    //        {
    //            Measure copy = actualMsr.get(i);
    //            if(copy.getAggName().equals(MolapCommonConstants.COUNT))
    //            {
    //                copy = copy.getCopy();
    //                copy.setAggName(MolapCommonConstants.SUM);
    //            }
    //            else if(copy.getAggName().equals(MolapCommonConstants.DISTINCT_COUNT) && isAgg && !isAutoAgg)
    //            {
    //                copy = copy.getCopy();
    //                copy.setAggName(MolapCommonConstants.SUM);
    //            }
    //            newMsr.add(copy);
    //        }
    //        return newMsr;
    //    }
    //    private List<Measure> updateActualMeasureBaseOnRequestTable(List<Measure> actualMsrs, Cube cube, String factTableName)
    //    {
    //        List<Measure> newMeasures = new ArrayList<Measure>();
    //        List<Measure> measures = cube.getMeasures(factTableName);
    //        int actualMeasureSize=actualMsrs.size();
    //        int tableMeasureSize=measures.size();
    //        Measure a = null;
    //        for(int i = 0;i < actualMeasureSize;i++)
    //        {
    //            a= actualMsrs.get(i);
    //            for(int j = 0;j < tableMeasureSize;j++)
    //            {
    //                if(a.getName().equals(measures.get(j).getName()))
    //                {
    //                    newMeasures.add(measures.get(j));
    //                    break;
    //                }
    //            }
    //        }
    //        
    //        return newMeasures;
    //    }
    //
    //    private Dimension[] getSliceFilterDimsFromConstraints(Map<Dimension, MolapFilterInfo> constraints,
    //            Dimension[] queryDims)
    //    {
    //        List<Dimension> sliceFilterExtraDims = new ArrayList<Dimension>();
    //        boolean found= false;
    //        for(Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet())
    //        {
    //            Dimension key = entry.getKey();
    //            found= false;
    //            for(int i = 0;i < queryDims.length;i++)
    //            {
    //                if(key.equals(queryDims[i]))
    //                {
    //                    found=true;
    //                    break;
    //                }
    //            }
    //            if(!found)
    //            {
    //                sliceFilterExtraDims.add(key);
    //            }
    //        }
    //        return sliceFilterExtraDims.toArray(new Dimension[sliceFilterExtraDims.size()]);
    //    }
    //
    //    private Dimension[] updateDimsForPaginationInCaseOfSliceFilterPresent(Dimension[] queryDims, Dimension[] dims, boolean isSliceFilterPresent)
    //    {
    //        List<Dimension> tempDims= new ArrayList<Dimension>();
    //        boolean isFound=false;
    //        if(paginationRequired)
    //        {
    //            if(dims.length > queryDims.length)
    //            {
    //                for(int i = 0;i < dims.length;i++)
    //                {
    //                    isFound=false;
    //                    for(int j = 0;j < queryDims.length;j++)
    //                    {
    //                        if(queryDims[j].equals(dims[i]))
    //                        {
    //                            isFound=true;
    //                            tempDims.add(queryDims[j]);
    //                            break;
    //                        }
    //                        
    //                    }
    //                    if(!isFound)
    //                    {
    //                        tempDims.add(dims[i]);
    //                    }
    //                }
    //            }
    //            
    //            else 
    //            {
    //                return queryDims;
    //            }
    //        }
    //        else
    //        {
    //            for(int i = 0;i < queryDims.length;i++)
    //            {
    //                tempDims.add(queryDims[i]);
    //            }
    //        }
    //        return tempDims.toArray(new Dimension[tempDims.size()]);
    //    }
    //
    //    private Dimension fillTopNModelForQuery(MolapQueryExecutorModel queryModel, int[] maskByteRanges,
    //            int[] maskByteRangesNormalized, Dimension[] queryDims, boolean isNormalizedCase, Dimension[] replacedDims,
    //            int countMsrsIndex, List<Integer> avgIndexes, KeyGenerator keyGenNormalized) throws KeyGenException
    //    {
    //        Dimension topNDim= null;
    //        List<Integer> bytePos = new ArrayList<Integer>();
    //        topNDim = topNModel.getDimension();
    //        //create masked key till what level we are applying topN
    //        topNModel.setTopNMaskedBytes(QueryExecutorUtil.getMaskedBytesForTopN(isNormalizedCase ? replacedDims
    //                : queryDims, isNormalizedCase ? keyGenNormalized : keyGenerator, topNModel.getDimIndex(),
    //                queryModel.getActualQueryDims(), queryModel.getActualDimsRows(), queryModel.getActualDimsCols(),
    //                topNDim, true, isNormalizedCase ? maskByteRangesNormalized
    //                        : maskByteRanges, bytePos));
    //        topNModel.setTopNMaskedBytesPos(QueryExecutorUtil.convertIntegerListToIntArray(bytePos));
    //        bytePos.clear();
    //        if(!topNModel.isBreakHierarchy())
    //        {
    //            // create masked key till just before level of topN level
    //            topNModel.setTopNGroupMaskedBytes(QueryExecutorUtil.getMaskedBytesForTopN(isNormalizedCase ? replacedDims
    //                    : queryDims, isNormalizedCase ? keyGenNormalized : keyGenerator, topNModel.getDimIndex(),
    //                    queryModel.getActualQueryDims(), queryModel.getActualDimsRows(), queryModel.getActualDimsCols(),
    //                    topNDim, false, isNormalizedCase ? maskByteRangesNormalized : maskByteRanges,
    //                    bytePos));
    //        }
    //        else
    //        {
    //            topNModel.setTopNGroupMaskedBytes(new byte[0]);
    //        }
    //        topNModel.setTopNGroupMaskedBytesPos(QueryExecutorUtil.convertIntegerListToIntArray(bytePos));
    //        topNModel.setAvgMsrIndex(avgIndexes.indexOf(topNModel.getMsrIndex()));
    //        topNModel.setCountMsrIndex(countMsrsIndex);
    //        return topNDim;
    //    }
    //
    //    private void updateUniqueAndMinValueForSlice(String factTable, List<Measure> msrs, boolean isAgg)
    //    {
    //        List<SliceUniqueValueInfo> sliceUniqueValueInfos = new ArrayList<SliceUniqueValueInfo>(null != slices ? slices.size(): 0);
    //        QueryExecutorUtility.processUniqueAndMinValueInfo(factTable, sliceUniqueValueInfos,true,isAgg,slices);
    //        if(sliceUniqueValueInfos.size()>0)
    //        {
    //            this.uniqueValue= mergerSliceUniqueValueInfo(sliceUniqueValueInfos);
    //        }
    //        List<SliceUniqueValueInfo> sliceMinValueInfos = new ArrayList<SliceUniqueValueInfo>(null != slices ? slices.size(): 0);
    //        QueryExecutorUtility.processUniqueAndMinValueInfo(factTable, sliceMinValueInfos,false,isAgg,slices);
    //        if(sliceUniqueValueInfos.size()>0)
    //        {
    //            double[] minValues= mergerSliceUniqueValueInfo(sliceMinValueInfos);
    //            for(Measure measure : msrs)
    //            {
    //                measure.setMinValue(minValues[measure.getOrdinal()]);
    //            }
    //        }
    //    }
    //
    ////    private void processUniqueAndMinValueInfo(String factTable, List<SliceUniqueValueInfo> sliceUniqueValueInfos, boolean uniqueValue, boolean isAgg)
    ////    {
    ////        SliceUniqueValueInfo sliceUniqueValueInfo = null;
    ////        SliceMetaData sliceMataData = null;
    ////        for(int i = 0;i < slices.size();i++)
    ////        {
    ////            CubeDataStore dataCache = slices.get(i).getDataCache(factTable);
    ////            if(null!=dataCache)
    ////            {
    ////                sliceMataData = slices.get(i).getRsStore().getSliceMetaCache(factTable);
    ////                double[] currentUniqueValue = null;
    ////                if(uniqueValue) {
    ////                    currentUniqueValue = slices.get(i).getDataCache(factTable).getUniqueValue();
    ////                }
    ////                else {
    ////                    if(isAgg)
    ////                    {
    ////                        currentUniqueValue = slices.get(i).getDataCache(factTable).getMinValueFactForAgg();
    ////                    }
    ////                    else
    ////                    {
    ////                        currentUniqueValue = slices.get(i).getDataCache(factTable).getMinValue();
    ////                    }
    ////                }
    ////                sliceUniqueValueInfo = new SliceUniqueValueInfo();
    ////                sliceUniqueValueInfo.setCols(sliceMataData.getMeasures());
    ////                sliceUniqueValueInfo.setUniqueValue(currentUniqueValue);
    ////                sliceUniqueValueInfos.add(sliceUniqueValueInfo);
    ////            }
    ////        }
    ////    }
    //
    //    private Map<Dimension, MolapFilterInfo> updateConstrainstsAfterTopN(MolapQueryExecutorModel queryModel, Cube cube,
    //            Map<Dimension, MolapFilterInfo> constraintsAfterTopN)
    //    {
    //        List<Dimension> factTableDimensions = cube.getDimensions(queryModel.getFactTable());
    //        if(queryModel.isAggTable() && null!=factTableDimensions && null!=constraintsAfterTopN)
    //        {
    //        for(Entry<Dimension, MolapFilterInfo> entry :constraintsAfterTopN.entrySet())
    //        {
    //            if(!factTableDimensions.contains(entry.getKey()))
    //            {
    //                constraintsAfterTopN = new HashMap<Dimension, MolapFilterInfo>();
    //                break;
    //            }
    //        }
    //        }
    //        return constraintsAfterTopN;
    //    }
    //    
    //    //CHECKSTYLE:OFF    Approval No:Approval-297
    //    private SliceExecutionInfo getSliceExecutionInfo(InMemoryCube slice, String factTable, Dimension[] queryDims,
    //            Map<Dimension, MolapFilterInfo> constraints, MolapQueryExecutorModel queryModel, boolean isNormalizedCase,
    //            List<Measure> msrs, int[] measureIndexToReadFromPaginationFile, int[] maskByteRanges, int[] maskedBytes,
    //            int byteCount, byte[] maxKey, Dimension[] replacedDims, KeyGenerator keyGenNormalized,
    //            int[] maskByteRangesNormalized, Dimension topNDim, List<Measure> calcMeasures,
    //            MolapSegmentHeader molapSegmentHeader, List<Integer> avgIndexes, int countMsrsIndex,
    //            byte[] maxKeyNormalized, int byteCountNormalized, int[] maskedBytesNormalized,
    //            List<Int2ObjectMap<List<int[]>>> listOfMapsOfDimValues, List<Integer> normalizedStartingIndexArray,SliceMetaData sliceMataData, 
    //            Map<Dimension, MolapFilterInfo> constraintsAfterTopN, Cube cube)
    //            throws KeyGenException, IOException
    //    {
    //        KeyGenerator sliceKeyGen = sliceMataData.getKeyGenerator();
    //        double[] currentUniqueValue = null;
    //        boolean isCustomMeasure= false;
    //        KeyGenerator factKeyGenerator= null;
    //        if(null!=slice.getDataCache(factTable))
    //        {
    //            currentUniqueValue = slice.getDataCache(factTable).getUniqueValue();
    //            char []type=slice.getDataCache(factTable).getType();
    //            for(int i = 0;i < type.length;i++)
    //            {
    //                if(type[i]=='c')
    //                {
    //                    isCustomMeasure=true;
    //                }
    //            }
    //        }
    //        if(isCustomMeasure)
    //        {
    ////            factKeyGenerator = cube.getKeyGenerator(cube.getFactTableName());
    //        }
    //        String[] sMetaDims = sliceMataData.getDimensions();
    //        List<Dimension> currentDimList = new ArrayList<MolapMetadata.Dimension>();
    //        Dimension[] currentDimTables = new Dimension[sliceKeyGen.getDimCount()];
    //        RestructureHolder holder = new RestructureHolder();
    //
    //        // added method for source monitor fix
    //        updateRestructureHolder(queryDims, sliceMataData, sMetaDims, currentDimList, currentDimTables, holder);
    //
    //        Dimension[] currentQueryDims = currentDimList.toArray(new Dimension[currentDimList.size()]);
    //        Map<Dimension, MolapFilterInfo> newConstraints = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>(
    //                constraints);
    //        if(holder.updateRequired)
    //        {
    //            if(!QueryExecutorUtil.updateFilterForOlderSlice(newConstraints, currentDimTables, slices))
    //            {
    //                // continue;
    //                return null;
    //            }
    //        }
    //
    //        InMemFilterModel filterModel = getFilterModel(queryModel, queryModel.getCube(), isNormalizedCase, sliceKeyGen,
    //                currentDimTables, newConstraints,factTable);
    //        /*InMemFilterModel filterModelAfterTopN = getFilterModel(queryModel, queryModel.getCube(), isNormalizedCase, keyGenerator,
    //                dimTables, constraintsAfterTopN,factTable);*/
    //        
    //        long[] startKey = new long[currentDimTables.length];
    //        long[] endKey = new long[currentDimTables.length];
    //
    //        // Set the start and end key based in the Filter models exclude and
    //        // include predicate keys.
    //        setStartAndEndKeys(startKey, endKey, filterModel.getIncludePredicateKeys(),
    //                filterModel.getIncludePredicateKeysOr(), filterModel.getExcludePredicateKeys(), currentDimTables);
    //
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "End key : " + Arrays.toString(endKey));
    //
    //        // Search till end of the data
    //        Measure[] measures = msrs.toArray(new Measure[msrs.size()]);
    //
    //
    //        int[] measureOrdinal = new int[measures.length];
    //        boolean[] msrExists = new boolean[measures.length];
    //        String[] sMetaMsrs = sliceMataData.getMeasures();
    //        double[] newMsrsDftVal = new double[measures.length];
    ////        updateMeasureInfo(sliceMataData, measures, measureOrdinal, msrExists, sMetaMsrs, newMsrsDftVal);
    //        RestructureUtil.updateMsr(sliceMataData, measures, measureOrdinal, msrExists, newMsrsDftVal, sMetaMsrs);
    //        // Create ScliceExecutionInfo object, polulate with the proper
    //        // information and
    //        // submit for the Execution.
    //        SliceExecutionInfo info = new SliceExecutionInfo();
    //        info.setFactKeyGenerator(factKeyGenerator);
    //        info.setCustomMeasure(isCustomMeasure);
    ////        info.setConstraintsAfterTopN(constraintsAfterTopN);
    ////        info.setMeasureIndexToRead(measureIndexToReadFromPaginationFile);
    //        info.setTableName(factTable);
    ////        info.setConstraints(newConstraints);
    //        info.setEndKey(endKey);
    ////        info.setFilterModel(filterModel);
    ////        info.setFilterModelAfterTopN(filterModelAfterTopN);
    //        info.setKeyGenerator(sliceKeyGen);
    //        info.setQueryDimensions(currentQueryDims);
    //        info.setQueryMsrs(measures);
    //        info.setMeasureOrdinal(measureOrdinal);
    //        info.setStartKey(startKey);
    ////        info.setMaxKeyBasedOnDimensions(QueryExecutorUtil.getMaxKeyBasedOnDimensions(currentQueryDims, sliceKeyGen,
    ////                currentDimTables));
    //        info.setSchemaName(schemaName);
    //        info.setCubeName(cubeName);
    //        info.setQueryId(queryModel.getQueryId());
    ////        info.setPaginationRequired(paginationRequired);
    ////        info.setRowRange(queryModel.getRowRange());
    ////        info.setMaskedByteRanges(QueryExecutorUtil.getMaskedByte(currentQueryDims, sliceKeyGen));
    //        info.setMaskedKeyByteSize(QueryExecutorUtil.getMaskedByte(currentQueryDims, sliceKeyGen).length);
    //        info.setNewMeasureDftValue(newMsrsDftVal);
    //        info.setMsrsExists(msrExists);
    //        info.setUniqueValues(currentUniqueValue);
    //        int[] maskedBytesLocal = new int[sliceKeyGen.getKeySizeInBytes()];
    ////        QueryExecutorUtil.updateMaskedKeyRanges(maskedBytesLocal, info.getMaskedByteRanges());
    //        holder.maskedByteRanges = maskedBytesLocal;
    //        info.setActalMaskedByteRanges(maskByteRanges);
    //        info.setMaskedBytePositions(maskedBytes);
    //        info.setActualMaskedKeyByteSize(byteCount);
    //        info.setActualMaxKeyBasedOnDimensions(maxKey);
    //        info.setActualKeyGenerator(keyGenerator);
    //        info.setRestructureHolder(holder);
    ////        if(null!=slice.getDataCache(factTable) && !isColumnar)
    ////        {
    ////            info.setRanges(QueryExecutorUtil.getValidRanges(slice, slice.getDataCache(factTable).getDataStoreRange(),
    ////                    startKey, endKey));
    ////        }
    //        info.setSlice(slice);
    //        info.setTopNModel(topNModel);
    //        info.setMsrConstraints(msrConstraints);
    //        info.setMsrConstraintsAfterTopN(queryModel.getMsrFilterModelsTopN());
    //        info.setMsrSortModel(msrSortModel);
    //        info.setDimensionSortOrder(queryDimAndSortOrder);
    //        Dimension[] orderedDims = getOrderAsperActualDims(isNormalizedCase ? replacedDims : queryDims,
    //                updateDimensionsNormalized(queryModel.getActualQueryDims(), isNormalizedCase),
    //                updateDimensionsNormalized(queryModel.getActualDimsRows(), isNormalizedCase),
    //                updateDimensionsNormalized(queryModel.getActualDimsCols(), isNormalizedCase), topNDim);
    //        int[][] maskedByteRangeForSorting = QueryExecutorUtility.getMaskedByteRangeForSorting(orderedDims,
    //                isNormalizedCase ? keyGenNormalized : keyGenerator, isNormalizedCase ? maskByteRangesNormalized
    //                        : maskByteRanges);
    //        info.setMaskedByteRangeForSorting(maskedByteRangeForSorting);
    //        info.setDimensionMaskKeys(getMaksedKeyForSorting(orderedDims, isNormalizedCase ? keyGenNormalized
    //                : keyGenerator, maskedByteRangeForSorting, isNormalizedCase ? maskByteRangesNormalized : maskByteRanges));
    //        info.setSlices(slices);
    ////        info.setNormalizedCase(isNormalizedCase);
    ////        info.setCalculatedMeasures(calcMeasures.toArray(new CalculatedMeasure[calcMeasures.size()]));
    ////        info.setSegmentHeader(molapSegmentHeader);
    ////        info.setRowLimitForDrillDown(queryModel.getRowLimit());
    //        info.setOriginalDims(isNormalizedCase ? replacedDims : queryModel.getDims());
    ////        info.setMsrFilterProcessorModel(queryModel.getMeasureFilterProcessorModel());
    //        info.setAvgIndexes(avgIndexes);
    //        info.setCountMsrsIndex(countMsrsIndex);
    ////        info.setKeyGenNormalized(keyGenNormalized);
    ////        info.setMaskByteRangesNormalized(maskByteRangesNormalized);
    ////        info.setMaxKeyNormalized(maxKeyNormalized);
    ////        info.setByteCountNormalized(byteCountNormalized);
    ////        info.setMaskedBytesNormalized(maskedBytesNormalized);
    ////        info.setListOfMapsOfDimValues(listOfMapsOfDimValues);
    ////        info.setNormalizedStartingIndexArray(normalizedStartingIndexArray);
    //        info.setReplacedDims(replacedDims);
    ////        if(isColumnar && null!=slice.getDataCache(factTable))
    ////        {
    ////            List<Boolean> needCompressedDataList= new ArrayList<Boolean>();
    ////            int[] dimIndex=getDimensionOffsets(queryDims,factTable,constraints, constraintsAfterTopN,needCompressedDataList,slice.getDataCache(factTable).getAggKeyBlock());
    //    //        int[]selectedDimIndex=getSelectedDimnesionIndex(dimIndex,queryDims);
    //     //       int[] filterIndexes=getFilterIndexs(constraints,dimIndex);
    ////            info.setFilterIndexes(filterIndexes);
    ////            info.setDimensionIndex(dimIndex);
    ////            info.setColumnarSplitter(getColumnarSplitter(dimTables, true));
    ////            info.setNeedCompressedData(MolapUtil.convertToBooleanArray(needCompressedDataList));
    ////            info.setIsUniqueValueBlock(null!=slice.getDataCache(factTable)?slice.getDataCache(factTable).getAggKeyBlock():null);
    ////            info.setSelectdDimension(selectedDimIndex);
    ////            int numberOfTuplesInLeaf=Integer.parseInt(MolapProperties.getInstance().getProperty(
    ////                    MolapCommonConstants.LEAFNODE_SIZE,
    ////                    MolapCommonConstants.LEAFNODE_SIZE_DEFAULT_VAL));
    ////            info.setNumberOfBits(numberOfTuplesInLeaf);
    ////        }
    //        return info;
    //    }
    //
    ////    private int[] getSelectedDimnesionIndex(int[] dimIndex, Dimension[] queryDims)
    ////    {
    ////        int[] selectedDimsIndex= new int[queryDims.length];
    ////        for(int i = 0;i < queryDims.length;i++)
    ////        {
    ////            selectedDimsIndex[i]=queryDims[i].getOrdinal();
    ////        }
    ////        for(int i = 0;i < selectedDimsIndex.length;i++)
    ////        {
    ////            for(int j = 0;j < dimIndex.length;j++)
    ////            {
    ////                if(selectedDimsIndex[i]==dimIndex[j])
    ////                {
    ////                    selectedDimsIndex[i]=j;
    ////                    break;
    ////                }
    ////            }
    ////        } 
    ////        Arrays.sort(selectedDimsIndex);
    ////        return selectedDimsIndex;
    ////    }
    //
    ////    private int[] getFilterIndexs(Map<Dimension, MolapFilterInfo> constraints, int[] dimIndex)
    ////    {
    ////        int[] filterIndex= new int[constraints.size()];
    ////        int index=0;
    ////        for(Entry<Dimension, MolapFilterInfo> entry:constraints.entrySet())
    ////        {
    ////            filterIndex[index++]=Arrays.binarySearch(dimIndex, entry.getKey().getOrdinal());
    ////        }
    ////        Arrays.sort(filterIndex);
    ////        return filterIndex;
    ////    }
    //
    ////    private int[] getDimensionOffsets(Dimension[] queryDims, String factTable,
    ////            Map<Dimension, MolapFilterInfo> constraints, Map<Dimension, MolapFilterInfo> constraintAfterTopN,
    ////            List<Boolean> needCompressedDataList, boolean[] isAggBlocks)
    ////    {
    ////        Set<Dimension> dimSet= new HashSet<Dimension>(); 
    ////        for(int i = 0;i < queryDims.length;i++)
    ////        {
    ////            dimSet.add(queryDims[i]);
    ////        }
    ////        for(Entry<Dimension, MolapFilterInfo> entry:constraints.entrySet())
    ////        {
    ////            dimSet.add(entry.getKey());
    ////        }
    ////        for(Entry<Dimension, MolapFilterInfo> entry:constraintAfterTopN.entrySet())
    ////        {
    ////            dimSet.add(entry.getKey());
    ////        }
    ////        int[] dimIndex=new int[dimSet.size()];
    ////        Iterator<Dimension> iterator = dimSet.iterator();
    ////        QueryDimInfo[] queryDimInfoArray= new QueryDimInfo[dimSet.size()];
    ////        
    ////        QueryDimInfo dimInfo = null; 
    ////        int index=0;
    ////        while(iterator.hasNext())
    ////        {
    ////            dimInfo = new QueryDimInfo();
    ////            Dimension next = iterator.next();
    ////            if(constraints.containsKey(next) && isAggBlocks[next.getOrdinal()])
    ////            {
    ////                dimInfo.needCompressedData=true;
    ////            }
    ////            else
    ////            {
    ////                dimInfo.needCompressedData=false;
    ////            }
    ////            dimInfo.index=next.getOrdinal();
    ////            queryDimInfoArray[index++]=dimInfo;
    ////        }
    ////        Arrays.sort(queryDimInfoArray);
    ////        for(int i = 0;i < queryDimInfoArray.length;i++)
    ////        {
    ////            dimIndex[i]=queryDimInfoArray[i].index;
    ////            needCompressedDataList.add(queryDimInfoArray[i].needCompressedData);
    ////        }
    ////        return dimIndex;
    ////    }
    //    
    ////    private class QueryDimInfo implements Comparable<QueryDimInfo>
    ////    {
    ////        private int index;
    ////        
    ////        private boolean needCompressedData;
    ////
    ////        @Override
    ////        public int compareTo(QueryDimInfo o)
    ////        {
    ////            return index-o.index;
    ////        }
    ////    }
    //
    ////    private void updateMeasureInfo(SliceMetaData sliceMataData, Measure[] measures, int[] measureOrdinal,
    ////            boolean[] msrExists, String[] sMetaMsrs, double[] newMsrsDftVal)
    ////    {
    ////        for(int i = 0;i < measures.length;i++)
    ////        {
    ////            for(int j = 0;j < sMetaMsrs.length;j++)
    ////            {
    ////                if(measures[i].getColName() != null && measures[i].getColName().equals(sMetaMsrs[j]))
    ////                {
    ////                    measureOrdinal[i] = measures[i].getOrdinal();
    ////                    msrExists[i] = true;
    ////                    break;
    ////                }
    ////            }
    ////            if(!msrExists[i])
    ////            {
    ////                String[] newMeasures = sliceMataData.getNewMeasures();
    ////                double[] newMsrDfts = sliceMataData.getNewMsrDfts();
    ////                if(newMeasures != null)
    ////                {
    ////                    for(int j = 0;j < newMeasures.length;j++)
    ////                    {
    ////                        if(measures[i].getColName() != null && measures[i].getColName().equals(newMeasures[j]))
    ////                        {// CHECKSTYLE:OFF Approval No:Approval-283
    ////                            newMsrsDftVal[i] = newMsrDfts[j];// CHECKSTYLE:ON
    ////                            break;
    ////                        }
    ////                    }
    ////                }
    ////            }
    ////        }
    ////    }
    //    
    //    private List<Measure> getUnifiedMeasuresForPagination(List<Measure> actualMsrs,List<Measure> msrs)
    //    {
    //        List<Measure> unifiedMsrs = new ArrayList<Measure>();
    //        unifiedMsrs.addAll(msrs);
    //        for(Measure msr : actualMsrs)
    //        {
    //            if(!unifiedMsrs.contains(msr))
    //            {
    //                unifiedMsrs.add(msr);
    //            }
    //        }
    //        return unifiedMsrs;
    //    }
    //    
    //    /**
    //     * @param queryModel
    //     * @param cube
    //     * @param isNormalizedCase
    //     * @param sliceKeyGen
    //     * @param currentDimTables
    //     * @param newConstraints
    //     * @return
    //     * @throws IOException
    //     * @throws KeyGenException
    //     */
    //    private InMemFilterModel getFilterModel(MolapQueryExecutorModel queryModel, Cube cube, boolean isNormalizedCase,
    //            KeyGenerator sliceKeyGen, Dimension[] currentDimTables, Map<Dimension, MolapFilterInfo> newConstraints,String factTableName)
    //            throws IOException, KeyGenException
    //    {
    //        // Create the Memory Filter
    //        InMemFilterModel filterModel = new InMemFilterModel();
    //        // Set the max key in the memory Filter. (max key for each dimension
    //        // on initialization)
    //        filterModel.setMaxKey(new byte[sliceKeyGen.getDimCount()][]);
    //
    //        if(queryModel.isFilterInHierGroups() && !isNormalizedCase)
    //        {
    //            // Compute column include nd exclude predicate key and update the
    //            // filter model.
    //            
    //            int[][] dimensionGroups = null;
    //            
    //            if(queryModel.isComputePredicatePresent())
    //            {
    //                dimensionGroups = QueryExecutorUtil.getDimensionGroupsForDifferentDimAsOneGroup(cube, newConstraints, factTableName);
    //            }
    //            else
    //            {
    //                dimensionGroups =QueryExecutorUtil.getDimensionGroups(cube, newConstraints); 
    //            }
    //            
    //            
    //            // Set the max key in the memory Filter. (max key for each dimension
    //            // on initialization)
    //            filterModel.setMaxKey(new byte[sliceKeyGen.getDimCount()][]);
    //            filterModel.setMaxKeyExclude(new byte[sliceKeyGen.getDimCount()][]);
    //            
    //            if(queryModel.isComputePredicatePresent())
    //            {
    //                QueryExecutorUtil.computeColIncludePredicateKeysForSubtotal(newConstraints, filterModel,
    //                        currentDimTables, sliceKeyGen, dimensionGroups, slices, cube, factTableName);
    //            }
    //            else
    //            {
    //                QueryExecutorUtil.computeColIncludePredicateKeys(newConstraints, filterModel, currentDimTables,
    //                        sliceKeyGen, dimensionGroups, slices, cube, factTableName);
    //            }
    //            
    //            QueryExecutorUtil.computeColExcludePredicateKeys(newConstraints, filterModel, currentDimTables, sliceKeyGen,dimensionGroups,slices,cube,factTableName);
    //        }
    //        else
    //        {
    //            // Set the max key in the memory Filter. (max key for each dimension
    //            // on initialization)
    //            filterModel.setMaxKey(new byte[sliceKeyGen.getDimCount()][]);
    //            filterModel.setMaxKeyExclude(new byte[sliceKeyGen.getDimCount()][]);
    //            filterModel.setMaxKeyIncludeOr(new byte[sliceKeyGen.getDimCount()][]);
    //            // Compute column include nd exclude predicate key and update the
    //            // filter model.
    //            QueryExecutorUtil.computeColIncludePredicateKeys(newConstraints, filterModel, currentDimTables, sliceKeyGen,slices);
    //            QueryExecutorUtil.computeColExcludePredicateKeys(newConstraints, filterModel, currentDimTables, sliceKeyGen,slices);
    //            QueryExecutorUtil.computeColIncludeOrPredicateKeys(newConstraints, filterModel, currentDimTables, sliceKeyGen,slices);
    //        }
    //
    //        // Set the max Size. (max bytes to store the key)
    //        filterModel.setMaxSize(sliceKeyGen.getKeySizeInBytes());
    //        return filterModel;
    //    }
    //    
    //    //CHECKSTYLE:ON
    //    
    ////    private void getFilterModel(MolapQueryExecutorModel queryModel,Cube cube,boolean isNormalizedCase) throws IOException, KeyGenException
    ////    {
    ////        // Create the Memory Filter
    ////        InMemFilterModel filterModel = new InMemFilterModel();
    ////        // Set the max key in the memory Filter. (max key for each dimension
    ////        // on initialization)
    ////        filterModel.setMaxKey(new byte[keyGenerator.getDimCount()][]);
    ////
    ////        List<Dimension> currentDimTablesList = new ArrayList<Dimension>();
    ////        for(int i = 0;i < dimTables.length;i++)
    ////        {
    ////            if(!dimTables[i].isNormalized())
    ////            {
    ////                currentDimTablesList.add(dimTables[i]);
    ////            }
    ////        }
    ////        Dimension[] currentDimTables = new Dimension[currentDimTablesList.size()];
    ////        currentDimTables = currentDimTablesList.toArray(currentDimTables);
    ////        if(queryModel.isFilterInHierGroups() && !isNormalizedCase)
    ////        {
    ////            // Compute column include nd exclude predicate key and update the
    ////            // filter model.
    ////            int[][] dimensionGroups = QueryExecutorUtil.getDimensionGroups(cube, queryModel.getConstraints());
    ////
    ////            // Set the max key in the memory Filter. (max key for each dimension
    ////            // on initialization)
    ////            filterModel.setMaxKey(new byte[keyGenerator.getDimCount()][]);
    ////            filterModel.setMaxKeyExclude(new byte[keyGenerator.getDimCount()][]);
    ////
    ////            QueryExecutorUtil.computeColIncludePredicateKeys(queryModel.getConstraints(), filterModel,
    ////                    currentDimTables, keyGenerator, dimensionGroups, slices);
    ////            QueryExecutorUtil.computeColExcludePredicateKeys(queryModel.getConstraints(), filterModel,
    ////                    currentDimTables, keyGenerator, dimensionGroups, slices);
    ////        }
    ////        else
    ////        {
    ////            // Set the max key in the memory Filter. (max key for each dimension
    ////            // on initialization)
    ////            filterModel.setMaxKey(new byte[keyGenerator.getDimCount()][]);
    ////            filterModel.setMaxKeyExclude(new byte[keyGenerator.getDimCount()][]);
    ////            filterModel.setMaxKeyIncludeOr(new byte[keyGenerator.getDimCount()][]);
    ////            // Compute column include nd exclude predicate key and update the
    ////            // filter model.
    ////            QueryExecutorUtil.computeColIncludePredicateKeys(queryModel.getConstraints(), filterModel,
    ////                    currentDimTables, keyGenerator, slices);
    ////            QueryExecutorUtil.computeColExcludePredicateKeys(queryModel.getConstraints(), filterModel,
    ////                    currentDimTables, keyGenerator, slices);
    ////            QueryExecutorUtil.computeColIncludeOrPredicateKeys(queryModel.getConstraints(), filterModel,
    ////                    currentDimTables, keyGenerator, slices);
    ////        }
    ////
    ////        // Set the max Size. (max bytes to store the key)
    ////        filterModel.setMaxSize(keyGenerator.getKeySizeInBytes());
    ////
    ////    }
    //    
    //    private List<Measure> getUpdatedMsrs(List<Measure> measures,Measure[] storedMeasures)
    //    {
    //        List<Measure> updatedMsrs = new ArrayList<MolapMetadata.Measure>();
    //        for(int i = 0;i < measures.size();i++)
    //        {
    //            for(Measure msr : storedMeasures)
    //            {
    //                if(measures.get(i).getName().equals(msr.getName()))
    //                {
    //                    updatedMsrs.add(msr);
    //                }
    //            }
    //        }
    //        return updatedMsrs;
    //    }
    //
    ////    /**
    ////     * @param queryDims
    ////     * @param query
    ////     */
    ////    private void getSortTypesFromQuery(Dimension[] queryDims, Query query)
    ////    {
    ////        queryDimAndSortOrder = new byte[queryDims.length];
    ////        try
    ////        {
    ////            if(query != null)
    ////            {
    ////                QueryAxis[] axes = query.axes;
    ////                Map<String, String> memberExpAndItsLitrl = getMemberExpAndItsLitrl(axes);
    ////                
    ////                
    ////                for(int i = 0;i < queryDims.length;i++)
    ////                {
    ////                    String string = memberExpAndItsLitrl.get(queryDims[i].getName());
    ////                    if(string == null || "BASC".equals(string))
    ////                    {
    ////                        queryDimAndSortOrder[i]= 0;
    ////                    }
    ////                    else
    ////                    {
    ////                        queryDimAndSortOrder[i]= 1;
    ////                    }
    ////                }
    ////            }
    ////        }
    ////        catch (Exception e) 
    ////        {
    ////            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, "Query parsing is failed... ");
    ////        }
    ////    }
    //    
    //    private Dimension[] getCopy(Dimension[] dimensions)
    //    {
    //        Dimension[] dimensionsCopy = new Dimension[dimensions.length];
    //        System.arraycopy(dimensions, 0, dimensionsCopy, 0, dimensions.length);
    //        return dimensionsCopy;
    //    }
    //
    //    private void getMeasureIndexToRead(int[] measureIndexToRead, List<Measure> queryMeasure, List<Measure> allMeasure, List<Measure> calMeasure, List<Measure> allCalMeasure)
    //    {
    //        int index=0;
    //        int i=0;
    //        for(Measure queryMsr:queryMeasure)
    //        {
    //            index = 0;
    //            for(Measure msr:allMeasure)
    //            {
    //                if(queryMsr.getName().equals(msr.getName()))
    //                {
    //                    measureIndexToRead[i++]=index;
    //                    break;
    //                }
    //                index++;
    //            }
    //        }
    //        for(Measure cMsr:calMeasure)
    //        {
    //            // setting the actual start index for calculated measure. In data , calc msrs starts after actual msrs
    //            index=allMeasure.size();
    //            for(Measure msr:allCalMeasure)
    //            {
    //                if(cMsr.getName().equals(msr.getName()))
    //                {
    //                    measureIndexToRead[i++]=index;
    //                    break;
    //                }
    //                index++;
    //            }
    //        }
    //        
    //    }
    //    
    ////    private void getMeasureIndexToRead(int[] measureIndexToRead, List<Measure> queryMeasure, List<Measure> allMeasure, List<Measure> calMeasure, List<Measure> allCalMeasure)
    ////    {
    ////        int index=0;
    ////        int i=0;
    ////        for(Measure msr:allMeasure)
    ////        {
    ////            for(Measure queryMsr:queryMeasure)
    ////            {
    ////                if(queryMsr.getName().equals(msr.getName()))
    ////                {
    ////                    measureIndexToRead[index++]=i;
    ////                    break;
    ////                }
    ////            }
    ////            i++;
    ////        }
    ////        
    ////        for(Measure msr:allCalMeasure)
    ////        {
    ////            for(Measure cMsr:calMeasure)
    ////            {
    ////                if(cMsr.getName().equals(msr.getName()))
    ////                {
    ////                    measureIndexToRead[index++]=i;
    ////                }
    ////            }
    ////            i++;
    ////        }
    ////        
    ////    }
    ////    /**
    ////     * It gets the data from MOLAP cache and fills it to iterator.
    ////     * @param queryModel
    ////     * @param hIterator
    ////     * @param factTable
    ////     * @param msrs
    ////     * @param properties
    ////     * @param queryDims
    ////     * @param isNormalizedCase
    ////     * @param molapSegmentHeader
    ////     * @param t1
    ////     * @param isCtMsrEXstInCurr
    ////     * @param countMsrsIndex
    ////     * @param avgIndexes
    ////     * @throws KeyGenException 
    ////     */
    ////    private boolean fillDataFromMolapCache(MolapQueryExecutorModel queryModel, MolapResultHolder hIterator,
    ////            String factTable, List<Measure> msrs, boolean properties, Dimension[] queryDims, boolean isNormalizedCase,
    ////            MolapSegmentHeader molapSegmentHeader, long t1, boolean isCtMsrEXstInCurr, int countMsrsIndex,
    ////            List<Integer> avgIndexes,boolean paginationEnabled,SliceExecutionInfo info) throws KeyGenException
    ////    {
    ////        if(paginationEnabled ||  isNormalizedCase || info == null)  
    ////        {
    ////            return false;
    ////        }
    ////        long st = System.currentTimeMillis();
    ////        //Check in cache
    ////        MolapSegmentBody segmentBodyFromCache = MolapCacheManager.getInstance().getSegmentBodyFromCache(molapSegmentHeader);
    ////        Map<ByteArrayWrapper, MeasureAggregator[]> checkMatchAndReturnData = null;
    ////
    ////        if( segmentBodyFromCache != null)
    ////        {
    ////            
    ////            checkMatchAndReturnData = PostQueryAggregatorUtil.checkMatchAndReturnData(segmentBodyFromCache,msrs);
    ////        }
    ////        else
    ////        {
    //////            List<Integer> bytePos = new ArrayList<Integer>();
    ////            Pair<MolapSegmentBody,Map<ByteArrayWrapper, MeasureAggregator[]>> pair = MolapCacheManager.getInstance().aggregate(molapSegmentHeader,keyGenerator,msrs);
    ////            if(pair != null)
    ////            {
    ////                checkMatchAndReturnData = pair.getValue();
    ////                segmentBodyFromCache = pair.getKey();
    ////            }
    ////            else
    ////            {
    ////                Pair<MolapSegmentBody, Map<ByteArrayWrapper, MeasureAggregator[]>> pairAgg = MolapCacheManager
    ////                        .getInstance().aggregate(molapSegmentHeader, keyGenerator, msrs,info.getFilterModel(),dimTables,info.getSlices(), queryModel.isSegmentCallWithFilterPresent());
    ////                if(pairAgg != null)
    ////                {
    ////                    checkMatchAndReturnData = pairAgg.getValue();
    ////                    segmentBodyFromCache = pairAgg.getKey();
    ////                }
    ////            }
    //////            byte[] maskedBytesForRollUp = QueryExecutorUtil.getMaskedBytesForRollUp(molapSegmentHeader.getDims(), keyGenerator,
    //////                    info.getActalMaskedByteRanges(), bytePos);
    //////            int[] bytePosArray = QueryExecutorUtil.convertIntegerListToIntArray(bytePos);
    //////            if(segmentBodyFromCache != null)
    //////            {
    //////                checkMatchAndReturnData = PostQueryAggregatorUtil.aggregateData(segmentBodyFromCache, keyGenerator, msrs, maskedBytesForRollUp, bytePosArray);
    //////            }
    ////        }
    ////        if(checkMatchAndReturnData != null)
    ////        {
    ////            if(checkMatchAndReturnData.size() > rowLimit && queryModel.getRowLimit() <= 0)
    ////            {
    ////                raiseLimitExceedException(checkMatchAndReturnData, queryDims,info.getMaskedBytePositions(),keyGenerator);
    ////            }
    ////           
    ////            byte[] updatedSortOrder = MolapCacheManager.getInstance().getUpdatedSortOrder(queryDims, info.getDimensionSortOrder(), molapSegmentHeader.getDims());
    ////            
    ////            FileBasedGlobalPaginatedAggregatorImpl paginatedAggregator = new FileBasedGlobalPaginatedAggregatorImpl(info,new LRUCacheKey(),rowCounter);
    ////            checkMatchAndReturnData = paginatedAggregator.processDataForNonPaginationWithSort(checkMatchAndReturnData,true);
    ////            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,"Processed data after cache : "+(System.currentTimeMillis()-st) );
    ////            updatedWithCurrentUnique(checkMatchAndReturnData, msrs);
    ////          
    ////            
    ////            if(!queryModel.isSparkExecution())
    ////            {
    ////                createDataFromAggregates(slices, hIterator, checkMatchAndReturnData, queryDims, properties,
    ////                        msrs.size(), false, factTable, avgIndexes, countMsrsIndex, isCtMsrEXstInCurr,
    ////                        msrs.toArray(new Measure[msrs.size()]),keyGenerator,info.getMaskedBytePositions(),queryModel.isOnlyNameColumnReq(),queryModel);
    ////            }
    ////            else
    ////            {
    ////                createDataFromAggregatesSpark(slices, hIterator, checkMatchAndReturnData, queryDims, properties,
    ////                        msrs.size(), false, factTable, avgIndexes, countMsrsIndex, isCtMsrEXstInCurr,
    ////                        msrs.toArray(new Measure[msrs.size()]),keyGenerator,info.getMaskedBytePositions(),queryModel.isOnlyNameColumnReq());
    ////                
    ////            }
    ////            
    ////            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Data Provided from MOLAP Cache : Total time taken Execute the query with id "
    ////                + queryId + " is : " + (System.currentTimeMillis() - t1)); 
    ////            return true;
    ////        }
    ////        return false;
    ////    }
    //
    //    /**
    //     * Get the data from paginated cache and fill to iterator
    //     * @param queryModel
    //     * @param hIterator
    //     * @param factTable
    //     * @param msrs
    //     * @param properties
    //     * @param t1
    //     * @param isCtMsrEXstInCurr
    //     * @param countMsrsIndex
    //     * @param avgIndexes
    //     * @throws IOException
    //     */
    //    private boolean fillDataFromPaginatedCache(MolapQueryExecutorModel queryModel, MolapResultHolder hIterator,
    //            String factTable, List<Measure> msrs, boolean properties, long t1, boolean isCtMsrEXstInCurr,
    //            int countMsrsIndex, List<Integer> avgIndexes,boolean paginationEnabled, int[] measureIndexToRead,TopNModel topNModel,KeyGenerator keyGenerator,Dimension[] dims) throws IOException, KeyGenException
    //    {
    //        if(!paginationEnabled)
    //        {
    //            return false;
    //        }
    //        LRUCacheKey cacheKey = new LRUCacheKey();
    //        cacheKey.setQueryId(queryModel.getQueryId());
    //        //Search the query ID in the LRU cache.
    //        LRUCacheValue rs = FileSizeBasedLRU.getInstance().get(cacheKey);
    //        if(rs != null )
    //        {
    //            cacheKey = rs.getCacheKey();
    //            
    //            List<Measure> actualCalMeasure = queryModel.getActualCalMeasure();
    //            measureIndexToRead = new int[msrs.size()+queryModel.getCalcMeasures().size()];
    //            getMeasureIndexToRead(measureIndexToRead,msrs,Arrays.asList(cacheKey.getMeasures()), queryModel.getCalcMeasures(),actualCalMeasure);
    //            
    //            String outLocation = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION,
    //                    MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)
    //                    + File.separator + MolapCommonConstants.PAGINATED_CACHE_FOLDER;
    //            DataFileReader fileReader = new DataFileReader(cacheKey.getByteCount(), outLocation,
    //                    queryModel.getQueryId(), keyGenerator, cacheKey.getMeasures(), queryModel.getCalcMeasures()
    //                            .toArray(new CalculatedMeasure[queryModel.getCalcMeasures().size()]),measureIndexToRead,slices.get(0));
    //            
    //            Map<ByteArrayWrapper, MeasureAggregator[]> readData = null;
    //            if(topNModel != null && topNModel.isBreakHierarchy())
    //            {
    //                
    //                if(QueryExecutorUtil.isSameDimensionLevels(rs.getCacheKey().getSegmentHeader().getDims(), dims))
    //                {
    //                    
    //                    if(queryModel.isCountFunction())
    //                    {
    //                        readData = fileReader.readDataAndAggregate(0, (int)rs.getRowCount(), rs.getCacheKey().getSegmentHeader(), keyGenerator, queryModel.getAnalyzerDims(),true);
    //                        if(rs.getRowCount()>0)
    //                        {
    //                            hIterator.setTotalRowCount(fileReader.getSizeAfterAggregate(queryModel.getRowRange()[0], queryModel.getRowRange()[1], rs.getCacheKey().getSegmentHeader(), keyGenerator, queryModel.getAnalyzerDims()));
    //                        }
    //                    }
    //                    else
    //                    {
    //                        readData = fileReader.readData(0, (int)rs.getRowCount());
    //                        if(rs.getRowCount()>0)
    //                        {
    //                            hIterator.setTotalRowCount((int)rs.getRowCount());
    //                        }
    //                    }
    //                    
    //                }
    //                else if(QueryExecutorUtil.isSubsetDimensionLevels(rs.getCacheKey().getSegmentHeader().getDims(), dims))
    //                {
    //                    if(queryModel.isCountFunction() && !QueryExecutorUtil.isSameDimensionLevels(queryModel.getAnalyzerDims(), dims))
    //                    {
    //                        readData = fileReader.readDataAndAggregate(0, (int)rs.getRowCount(), rs.getCacheKey().getSegmentHeader(), keyGenerator, queryModel.getAnalyzerDims(),true);
    //                        if(rs.getRowCount()>0)
    //                        {
    //                            hIterator.setTotalRowCount(fileReader.getSizeAfterAggregate(queryModel.getRowRange()[0], queryModel.getRowRange()[1], rs.getCacheKey().getSegmentHeader(), keyGenerator, queryModel.getAnalyzerDims()));
    //                        }
    //                    }
    //                    else
    //                    {
    //                        readData = fileReader.readDataAndAggregate(0, (int)rs.getRowCount(), rs.getCacheKey().getSegmentHeader(), keyGenerator, dims,false);
    //                        if(rs.getRowCount()>0)
    //                        {
    //                            hIterator.setTotalRowCount(fileReader.getSizeAfterAggregate(queryModel.getRowRange()[0], queryModel.getRowRange()[1], rs.getCacheKey().getSegmentHeader(), keyGenerator, dims));
    //                        }
    //                        
    //                    }
    //                }
    //                
    //            }
    //            else
    //            {
    //                
    //                if(QueryExecutorUtil.isSameDimensionLevels(rs.getCacheKey().getSegmentHeader().getDims(), dims))
    //                {
    //                    if(queryModel.isCountFunction())
    //                    {
    //                        readData = fileReader.readDataAndAggregate(queryModel.getRowRange()[0], queryModel.getRowRange()[1], rs.getCacheKey().getSegmentHeader(), keyGenerator, queryModel.getAnalyzerDims(),true);
    //                        if(rs.getRowCount()>0)
    //                        {
    //                            hIterator.setTotalRowCount(fileReader.getSizeAfterAggregate(queryModel.getRowRange()[0], queryModel.getRowRange()[1], rs.getCacheKey().getSegmentHeader(), keyGenerator, queryModel.getAnalyzerDims()));
    //                        }
    //                    }
    //                    else
    //                    {
    //                        readData = fileReader.readData(queryModel.getRowRange()[0], queryModel.getRowRange()[1]);
    //                        
    //                        if(rs.getRowCount()>0)
    //                        {
    //                            hIterator.setTotalRowCount((int)rs.getRowCount());
    //                        }
    //                    }
    //                }
    //                else if(QueryExecutorUtil.isSubsetDimensionLevels(rs.getCacheKey().getSegmentHeader().getDims(), dims))
    //                {
    //                    if(queryModel.isCountFunction() && !QueryExecutorUtil.isSameDimensionLevels(queryModel.getAnalyzerDims(), dims))
    //                    {
    //                        readData = fileReader.readDataAndAggregate(queryModel.getRowRange()[0], queryModel.getRowRange()[1], rs.getCacheKey().getSegmentHeader(), keyGenerator, queryModel.getAnalyzerDims(),true);
    //                        if(rs.getRowCount()>0)
    //                        {
    //                            hIterator.setTotalRowCount(fileReader.getSizeAfterAggregate(queryModel.getRowRange()[0], queryModel.getRowRange()[1], rs.getCacheKey().getSegmentHeader(), keyGenerator, queryModel.getAnalyzerDims()));
    //                        }
    //                    }
    //                    else
    //                    {
    //                        readData = fileReader.readDataAndAggregate(queryModel.getRowRange()[0], queryModel.getRowRange()[1], rs.getCacheKey().getSegmentHeader(), keyGenerator, dims,false);
    //                        if(rs.getRowCount()>0)
    //                        {
    //                            hIterator.setTotalRowCount(fileReader.getSizeAfterAggregate(queryModel.getRowRange()[0], queryModel.getRowRange()[1], rs.getCacheKey().getSegmentHeader(), keyGenerator, dims));
    //                        }
    //                    }
    //                }
    //                
    //            }
    //            if(readData != null)
    //            {
    ////                if(!queryModel.isAggTable())
    ////                {
    //                    msrs=getUpdatedMsrs(msrs, cacheKey.getMeasures());
    ////                }
    //                updatedWithCurrentUnique(readData, msrs);
    //                
    //                createDataFromAggregates(slices, hIterator, readData, dims, properties,
    //                        msrs.size(), false, factTable, avgIndexes, countMsrsIndex, isCtMsrEXstInCurr,
    //                        msrs.toArray(new Measure[msrs.size()]),cacheKey.getGenerator(),cacheKey.getMaskedKeyRanges(),queryModel.isOnlyNameColumnReq(), null);
    ////                if(rs.getRowCount()>0)
    ////                {
    ////                    hIterator.setTotalRowCount((int)rs.getRowCount());
    ////                }
    //                LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Data Provided from Pagination : Total time taken Execute the query with id "
    //                    + queryModel.getQueryId() + " is : " + (System.currentTimeMillis() - t1)); 
    //                return true;
    //            }
    //        }
    //        return false;
    //    }
    //    
    //    
    //    
    //    private Map<Dimension, MolapFilterInfo> updateConstraintsForNormalized(Map<Dimension, MolapFilterInfo> constraints,List<InMemoryCube> slices,Cube cube) throws IOException
    //    {
    //        
    //        Map<Dimension, MolapFilterInfo> updatedConstraints = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>(); 
    //        
    //        Map<String, List<FilterHolder>> dimGroups = new HashMap<String, List<FilterHolder>>();
    //        for(Entry<Dimension, MolapFilterInfo> entry : constraints.entrySet())
    //        {
    //            Dimension dim = entry.getKey();
    //            
    //            String key = dim.getDimName()+'_'+dim.getHierName();
    //            List<FilterHolder> list = dimGroups.get(key);
    //            if(list == null)
    //            {
    //                list = new ArrayList<FilterHolder>();
    //                dimGroups.put(key,list);
    //            }
    //            FilterHolder filterHolder = new FilterHolder();
    //            filterHolder.setDimension(dim);
    //            filterHolder.setFilterInfo(entry.getValue());
    //            list.add(filterHolder);
    //        }
    //        
    //        for(Iterator<Entry<String, List<FilterHolder>>> iterator = dimGroups.entrySet().iterator();iterator.hasNext();)
    //        {
    //            Entry<String, List<FilterHolder>> entry = iterator.next();
    //            List<FilterHolder> value = entry.getValue();
    //            boolean normalized = false;
    //            for(FilterHolder filterHolder : value)
    //            {
    //                if(filterHolder.getDimension().isNormalized())
    //                {
    //                    normalized = true;
    //                    break;
    //                }
    //            }
    //            if(!normalized)
    //            {
    //                for(FilterHolder filterHolder : value)
    //                {
    //                    updatedConstraints.put(filterHolder.getDimension(), filterHolder.getFilterInfo());
    //                }
    //                iterator.remove();
    //            }
    //        }
    //        
    //        
    //        
    //        for(Entry<String, List<FilterHolder>> entry : dimGroups.entrySet())
    //        {
    //            String key = entry.getKey();
    //            List<FilterHolder> filterHolders = entry.getValue();
    //         
    //            List<Dimension> dimList = cube.getHierarchiesMapping(key);
    //            
    //            Map<Dimension, MolapFilterInfo> localConstraints = new HashMap<MolapMetadata.Dimension, MolapFilterInfo>(); 
    //            
    //            for(FilterHolder filterHolder : filterHolders)
    //            {
    //                Dimension dim = filterHolder.getDimension();
    //                for(Dimension dimension : dimList)
    //                {
    //                    if(dim.getDimName().equals(dimension.getDimName()) && dim.getHierName().equals(dimension.getHierName())
    //                            && dim.getName().equals(dimension.getName()))
    //                    {
    //                        dim = dimension;
    //                        break;
    //                    }
    //                }
    //                localConstraints.put(dim, filterHolder.getFilterInfo());
    //            }
    //            int[] ordinals = new int[1]; 
    //            List<Type> dataTypes = new ArrayList<Type>();
    //            Dimension queryDim = dimList.get(dimList.size()-1);
    //            ordinals[0]=queryDim.getOrdinal();
    //            dataTypes.add(queryDim.getDataType());
    //            MolapResultHolder iter = new MolapResultHolder(dataTypes);
    //            executeHierarichies(queryDim.getHierName(),ordinals, dimList, localConstraints, iter,false,this.keyGenerator,false);
    //            MolapFilterInfo filtersFromIterator = new MolapFilterInfo();
    //            QueryExecutorUtil.getFiltersFromIterator(ordinals, iter,filtersFromIterator,true);
    //            for(Dimension dimension : dimTables)
    //            {
    //                if(queryDim.getDimName().equals(dimension.getDimName()) && queryDim.getHierName().equals(dimension.getHierName())
    //                        && queryDim.getName().equals(dimension.getName()))
    //                {
    //                    queryDim = dimension;
    //                    break;
    //                }
    //            }
    //            queryDim = queryDim.getDimCopy();
    //            queryDim.setActualCol(true);
    //            updatedConstraints.put(queryDim, filtersFromIterator);
    //            
    //        }
    //        return updatedConstraints;
    //    }
    //    
    // 
    //    /**
    //     * 
    //     * @param sliceUniqueValueInfos
    //     * 
    //     */
    //    private double[] mergerSliceUniqueValueInfo(List<SliceUniqueValueInfo> sliceUniqueValueInfos)
    //    {
    //        int maxInfoIndex= 0;
    //        int lastMaxValue=0;
    //        for(int k = 0;k < sliceUniqueValueInfos.size();k++)
    //        {
    //            if(sliceUniqueValueInfos.get(k).getLength()>lastMaxValue)
    //            {
    //                lastMaxValue=sliceUniqueValueInfos.get(k).getLength();
    //                maxInfoIndex=k;
    //            }
    //        }
    //        SliceUniqueValueInfo sliceUniqueValueInfo = sliceUniqueValueInfos.get(maxInfoIndex);
    //        double[] maxSliceUniqueValue = sliceUniqueValueInfo.getUniqueValue();
    //        String[] columnss = null;
    //        double[] currentUniqueValue = null;
    //        for(int i = 0;i < sliceUniqueValueInfos.size();i++)
    //        {
    //            if(i==maxInfoIndex)
    //            {
    //                continue;
    //            }
    //            columnss= sliceUniqueValueInfos.get(i).getCols();
    //            currentUniqueValue= sliceUniqueValueInfos.get(i).getUniqueValue();
    //            for(int j = 0;j < columnss.length;j++)
    //            {
    //                maxSliceUniqueValue[j]= maxSliceUniqueValue[j]>currentUniqueValue[j]?currentUniqueValue[j]: maxSliceUniqueValue[j];
    //            }
    //        }
    //        return maxSliceUniqueValue;
    //    }
    //
    //    private void updatedWithCurrentUnique(Map<ByteArrayWrapper, MeasureAggregator[]> data,List<Measure> msrList)
    //    {
    //
    //        int size = msrList.size();
    //        for(Entry<ByteArrayWrapper, MeasureAggregator[]> entry: data.entrySet())
    //        {
    //            MeasureAggregator[] value = entry.getValue();
    //            for(int i = 0;i < size;i++)
    //            {
    //                Measure m = msrList.get(i);
    //
    //                if(value[i].isFirstTime())
    //                {
    //                    value[i].setNewValue(this.uniqueValue[m.getOrdinal()]);
    //                }
    //            }
    //        }
    //    }
    //    
    //    private void updatedWithCurrentUnique(QueryResult result,List<Measure> msrList)
    //    {
    //        int size = msrList.size();
    //        QueryResultIterator iterator=result.iterator();
    //        while(iterator.hasNext())
    //        {
    //            MeasureAggregator[] value = iterator.getValue();
    //            for(int i = 0;i < size;i++)
    //            {
    //                Measure m = msrList.get(i);
    //
    //                if(value[i].isFirstTime())
    //                {
    //                    value[i].setNewValue(this.uniqueValue[m.getOrdinal()]);
    //                }
    //            }
    //        }
    //    }
    //    
    //
    //    /**
    //     * @param cube
    //     * @param constraints
    //     * @param replacedDims
    //     * @param normalizedStartingIndexArray 
    //     * @throws IOException
    //     */
    //    private List<Int2ObjectMap<List<int[]>>> findAndGetFactDimValues(Cube cube, Map<Dimension, MolapFilterInfo> constraints,
    //            Dimension[] replacedDims, List<Integer> normalizedStartingIndexArray) throws IOException
    //    {
    //        List<Int2ObjectMap<List<int[]>>> listOfMaps = new ArrayList<Int2ObjectMap<List<int[]>>>();
    //        for(Integer replacedDimIndex : normalizedStartingIndexArray)
    //        {
    //               if(replacedDims[replacedDimIndex] != null)
    //               {    
    //                   // Execute hierarchy so that we get 
    //                   // corresponding factDim values for 
    //                   // normalized column
    //                   List<Integer> queryDimOrdinalsList = new ArrayList<Integer>();
    //                   
    //                   List<Dimension> listings;
    //                   List<SqlStatement.Type> dataTypes = new ArrayList<SqlStatement.Type>();
    //                   listings = cube.getHierarchiesMapping(replacedDims[replacedDimIndex].getDimName() 
    //                           + '_' + replacedDims[replacedDimIndex].getHierName());
    //                   for(Dimension d : listings)
    //                   {
    //                       for(int i = replacedDimIndex; i < replacedDims.length; i++)
    //                       {
    //                           if(d.equals(replacedDims[i]))
    //                           {
    //                               dataTypes.add(d.getDataType());
    //                               queryDimOrdinalsList.add(listings.indexOf(d));
    //                               break;
    //                           }
    //                       }
    //                       
    //                   }
    //                   // Last level of any normalized hier will not present in replacedDims
    //                   // Hence set it
    //                   if(!queryDimOrdinalsList.contains(listings.size() - 1))
    //                   {
    //                       queryDimOrdinalsList.add(listings.size() - 1) ;
    //                   }
    //                   dataTypes.add(listings.get(listings.size() - 1).getDataType());
    //                   int[] queryDimOrdinals = QueryExecutorUtil.convertIntegerListToIntArray(queryDimOrdinalsList);
    //                   MolapResultHolder tempHolder = new MolapResultHolder(dataTypes);
    //                   // TODO as of now it will return only first 
    //                   // Handle case when there is more than 1 normalized dim present
    //                   listOfMaps.add(executeHierarichiesWithSurrogates(replacedDims[replacedDimIndex].getHierName(), queryDimOrdinals, listings, 
    //                            constraints, tempHolder,getKeyGenerator(listings.toArray(new Dimension[listings.size()]),false),cube));
    //                }
    //            }
    //        return listOfMaps;
    //    }
    //    
    //    
    //    /**
    //     * @param queryDims
    //     * @param replacedDims 
    //     * @param normalizedStartingIndexArray
    //     * @return
    //     */
    //    private boolean checkAndReplaceNormalizedDims(Dimension[] queryDims, Dimension[] replacedDims, List<Integer> normalizedStartingIndexArray)
    //    {
    //        // Go through queryDims, if it is normalized, 
    //        // then replace it with column present in fact table
    //        // Store this info so that later we can switch it
    //        boolean isNormalizedCase = false;
    //        //boolean normHierStart = false;
    //        String currentHierName = "";
    //        for(int i = 0; i < queryDims.length; i++)
    //        {
    //            if(queryDims[i].isNormalized())
    //            {
    //                isNormalizedCase = true;
    //                // See if hierarchy is changing or not
    //                if(!queryDims[i].getHierName().equalsIgnoreCase(currentHierName))
    //                {
    //                    // this is the starting index of normalized hierarchy
    //                    normalizedStartingIndexArray.add(i);
    //                    currentHierName = queryDims[i].getHierName();
    //                }
    //                //CHECKSTYLE:OFF    Approval No:Approval-118
    //                //normHierStart = true;
    //                replacedDims[i] = queryDims[i];
    //                queryDims[i] = queryDims[i].getDimInFact();
    //            }
    //            else
    //            {
    //                replacedDims[i] = queryDims[i];
    //            }
    //        }
    //        return isNormalizedCase;
    //    }
    //    
    //    private Dimension[] updateDimensionsNormalized(Dimension[] replaceDims,boolean isNormalized)
    //    {
    //        if(replaceDims == null || replaceDims.length == 0)
    //        {
    //            return new Dimension[0];
    //        }
    //        Dimension[] updatedDims = new Dimension[replaceDims.length];
    //        if(isNormalized)
    //        {
    //            Dimension[] normalizedDims = getNormalizedDims();
    //            for(int i = 0;i < replaceDims.length;i++)
    //            {
    //                updatedDims[i] = QueryExecutorUtil.getDimension(replaceDims[i],normalizedDims);
    //            }
    //            return updatedDims;
    //        }
    //        return replaceDims;
    //    }
    //    
    //    /**
    //     * Update the ordinals
    //     * @return
    //     */
    //    private Dimension[] getNormalizedDims()
    //    {
    //        Dimension[] updatedDims = new Dimension[dimTables.length];
    //        
    //        for(int i = 0;i < updatedDims.length;i++)
    //        {
    //            Dimension dimCopy = dimTables[i].getDimCopy();
    //            dimCopy.setOrdinal(i);
    //            updatedDims[i] = dimCopy;
    //        }
    //        return updatedDims;
    //    }
    //    
    ////    /**
    ////     * 
    ////     * 
    ////     * @param axes
    ////     *
    ////     */
    ////     private  Map<String, String> getMemberExpAndItsLitrl(QueryAxis[] axes)
    ////     {
    ////         Map<String, String> columnNameAndSortOrderMap = new LinkedHashMap<String, String>();
    ////         for(int i = 0;i < axes.length;i++)
    ////         {
    ////             Exp set = axes[i].getSet();
    ////             if(set instanceof mondrian.mdx.NamedSetExpr)
    ////             {
    ////                 mondrian.mdx.NamedSetExpr setExpr = (NamedSetExpr)set;
    ////                 updateMap(columnNameAndSortOrderMap,setExpr);
    ////             }
    ////             else if(set instanceof mondrian.mdx.ResolvedFunCall)
    ////             {
    ////                 Exp[] args = ((mondrian.mdx.ResolvedFunCall)set).getArgs();
    ////                 for(int j = 0;j < args.length-1;j++)
    ////                 {
    ////                     updateMap(columnNameAndSortOrderMap,(NamedSetExpr)args[i]);
    ////                 }
    ////             }
    ////         }
    ////         return columnNameAndSortOrderMap;
    ////     }
    ////     
    ////     private void updateMap(Map<String, String> columnNameAndSortOrderMap, mondrian.mdx.NamedSetExpr setType)
    ////     {
    ////         Exp[] args = ((mondrian.mdx.ResolvedFunCall)setType.getNamedSet().getExp()).getArgs();
    ////         Set<String> cols = new LinkedHashSet<String>();
    ////         int i = 1;
    ////         while (i < args.length)
    //////         for(int i = 1;i < args.length;)
    ////         {
    ////             List<String> colsList = new ArrayList<String>();
    ////             if(args[i] instanceof mondrian.mdx.ResolvedFunCall)
    ////             {
    ////                 mondrian.mdx.ResolvedFunCall a = (mondrian.mdx.ResolvedFunCall)((mondrian.mdx.ResolvedFunCall)args[i])
    ////                         .getArgs()[0];
    ////                 Exp arg = ((mondrian.mdx.ResolvedFunCall)a).getArg(0);
    ////                 if(arg instanceof mondrian.mdx.DimensionExpr)
    ////                 {
    ////                     mondrian.mdx.DimensionExpr arg1 = (mondrian.mdx.DimensionExpr)arg;
    ////                     Hierarchy[] hierarchies = arg1.getDimension().getHierarchies();
    ////                     Level[] levels = hierarchies[0].getLevels();
    ////                     for(int j = 0;j < levels.length;j++)
    ////                     {
    ////                         if(!levels[j].isAll())
    ////                         {
    ////                             if(!cols.contains((levels[j].getName())))
    ////                             {
    ////                                 cols.add(levels[j].getName());
    ////                                 colsList.add(levels[j].getName());
    ////                             }
    ////                         }
    ////                     }
    ////                 }
    ////                 else
    ////                 {
    ////                     Exp arg1 = ((mondrian.mdx.ResolvedFunCall)a).getArg(0);
    ////
    ////                     if(arg1 instanceof mondrian.mdx.LevelExpr)
    ////                     {
    ////                         mondrian.mdx.LevelExpr arg12 = (mondrian.mdx.LevelExpr)arg1;
    ////                         cols.add(arg12.getLevel().getName());
    ////                         colsList.add(arg12.getLevel().getName());
    ////                     }
    ////                 }
    ////                 for(String columnName : colsList)
    ////                 {
    ////                     columnNameAndSortOrderMap.put(columnName,
    ////                             (String)((mondrian.olap.Literal)args[i + 1]).getValue());
    ////                 }
    ////                 i = i + 2;
    ////             }
    ////             else 
    ////             {
    ////                 i++;
    ////             }
    ////         }
    ////     }
    //
    //
    ////    /**
    ////     * et measure indexes with agg type
    ////     * @param msrs
    ////     * @param aggType
    ////     * @return List<Integer>
    ////     */
    ////    private List<Integer> getMeasureIndexes(List<Measure> msrs, String aggType,List<Integer> integers)
    ////    {
    ////        int i = 0;
    ////        for(Measure msr : msrs)
    ////        {
    ////            if(msr.getAggName().equals(aggType))
    ////            {
    ////                integers.add(i);
    ////            }
    ////            i++;
    ////        }
    ////        return integers;
    ////    }
    //    
    ////    /**
    ////     * 
    ////     * 
    ////     * @param msrs
    ////     * @param allMsrs
    ////     * @return
    ////     *
    ////     */
    ////    private List<Measure> getOriginalMeasures(List<Measure> msrs,List<Measure> allMsrs)
    ////    {
    ////        List<Measure> updated = new ArrayList<MolapMetadata.Measure>();
    ////        for(Measure currMsr : msrs)
    ////        {
    ////            for(Measure orgMsr : allMsrs)
    ////            {
    ////                if(currMsr.getOrdinal() == orgMsr.getOrdinal())
    ////                {
    ////                    updated.add(orgMsr);
    ////                    break;
    ////                }
    ////            }
    ////        }
    ////        return updated;
    ////    }
    //    
    ////    /**
    ////     * Update Avg Measures with count measures 
    ////     * @param msrs
    ////     * @param avgIndexes
    ////     */
    ////    private void updateMeasuresWithSum(List<Measure> msrs,List<Integer> avgIndexes)
    ////    {
    ////        int i=0;
    ////        for(Measure msr : msrs)
    ////        {
    ////            if(avgIndexes.contains(i))
    ////            {
    ////                Measure tmpMsr = msr.getCopy();
    ////                tmpMsr.setAggName(MolapCommonConstants.SUM);
    ////                msrs.set(i, tmpMsr);
    ////            }
    ////            else
    ////            {
    ////                msrs.set(i, msr);
    ////            }
    ////            i++;
    ////        }
    ////        
    ////        
    ////    }
    //
    //
    //    /**
    //     * 
    //     * @param queryDims
    //     * @param sliceMataData
    //     * @param sMetaDims
    //     * @param currentDimList
    //     * @param currentDimTables
    //     * @param holder
    //     * @return
    //     * 
    //     */
    //    private int updateRestructureHolder(Dimension[] queryDims, SliceMetaData sliceMataData, String[] sMetaDims,
    //            List<Dimension> currentDimList, Dimension[] currentDimTables, RestructureHolder holder)
    //    {
    //        boolean found = false;
    //        int len = 0;
    //        for(int i = 0;i < dimTables.length;i++)
    //        {
    //            found = false;
    //            for(int j = 0;j < sMetaDims.length;j++)
    //            {
    //                if(sMetaDims[j].equals(dimTables[i].getActualTableName() + '_' + dimTables[i].getColName()))
    //                {//CHECKSTYLE:OFF    Approval No:Approval-285
    //                    if( !dimTables[i].isNormalized())
    //                    {
    //                        currentDimTables[len] = dimTables[i];
    //                        len++;
    //                    }
    //                    found=true;
    //                    break;
    //                }//CHECKSTYLE:ON
    //            }
    //            
    //            if(!found)
    //            {
    //                holder.updateRequired = true;
    //            }
    //          
    //        }
    //        holder.metaData = sliceMataData;
    //        len = 0;
    //         
    //        for(int i = 0;i < queryDims.length;i++)
    //        {
    //            
    //            for(int j = 0;j < sMetaDims.length;j++)
    //            {
    //                if(sMetaDims[j].equals(queryDims[i].getActualTableName() + '_' + queryDims[i].getColName()))
    //                {
    //                    currentDimList.add(queryDims[i]);
    //                    found = true;
    //                   // holder.dimsMissed[i] = -DIMENSION_DEFAULT;
    //                    break;
    //                    
    //                }
    //            }
    //
    //            len++;
    //        }
    //        if(!this.keyGenerator.equals(holder.metaData.getKeyGenerator()))
    //        {
    //            holder.updateRequired = true;
    //        }
    //        return len;
    //    }
    //    
    //    
    //
    //    
    //    
    //    /**
    //     * Submit query for Execution to slice Executor to get the result map and Populate the list.
    //     * 
    //     * @param factTable
    //     * @param dataArray
    //     * @param startKey
    //     * @param endKey
    //     * @param info2
    //     * @param slice
    //     *
    //     */
    //    private Map<ByteArrayWrapper, MeasureAggregator[]> submitExecutor(List<SliceExecutionInfo> infos,
    //            MolapResultHolder resultHolder, SliceExecutionInfo latestInfo)
    //    {
    //        ParallelSliceExecutor parallelSliceExec = null;
    //        if(isColumnar)
    //        {
    ////        parallelSliceExec = new ColumnarParallelSliceExecutor(infos, latestInfo, rowCounter,
    ////                rowLimit, resultHolder);
    //        }
    //        else
    //        {
    ////            parallelSliceExec = new ParallelSliceExecutorImpl(infos, latestInfo, rowCounter,
    ////                    rowLimit, resultHolder);
    //        }
    //        sliceExecutors.add(parallelSliceExec);
    //        // TODO this call can be made to execute on separate thread
    //        // for each
    //        // slice
    //        try
    //        {
    //            return parallelSliceExec.executeSliceInParallel();
    //        }
    //        catch(Exception e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, "Error happend on executing slices parallely");
    //        }
    //        return null;
    //    }
    //    
    //    /**
    //     * Submit query for Execution to slice Executor to get the result map and Populate the list.
    //     * 
    //     * @param factTable
    //     * @param dataArray
    //     * @param startKey
    //     * @param endKey
    //     * @param info2
    //     * @param slice
    //     *
    //     */
    //    private QueryResult submitExecutorDetailQuery(List<SliceExecutionInfo> infos,
    //            MolapResultHolder resultHolder, SliceExecutionInfo latestInfo)
    //    {
    //        ParallelSliceExecutor parallelSliceExec = null;
    //        if(isColumnar)
    //        {
    ////        parallelSliceExec = new ColumnarParallelSliceExecutor(infos, latestInfo, rowCounter,
    ////                rowLimit, resultHolder);
    //        }
    //        else
    //        {
    ////            parallelSliceExec = new ParallelSliceExecutorImpl(infos, latestInfo, rowCounter,
    ////                    rowLimit, resultHolder);
    //        }
    //        sliceExecutors.add(parallelSliceExec);
    //        // TODO this call can be made to execute on separate thread
    //        // for each
    //        // slice
    //        try
    //        {
    //            return parallelSliceExec.executeSlices();
    //        }
    //        catch(Exception e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, "Error happend on executing slices parallely");
    //        }
    //        return null;
    //    }
    //
    //    /**
    //     * Return the scanner based on the constraints.
    //     * 
    //     * @param keyGen
    //     * @param startKey
    //     * @param endKey
    //     * @param constraints
    //     * @param filterModel
    //     * @return
    //     * @throws Exception
    //     *
    //     */
    //    private Scanner getScanner(KeyGenerator keyGen, long[] startKey, long[] endKey,
    //            Map<Dimension, MolapFilterInfo> constraints, InMemFilterModel filterModel, FileHolder fileHolder) throws Exception
    //    {
    //        Scanner scanner;
    //        InMemoryFilter filter = null;
    //        byte[] startKeyBytes = keyGen.generateKey(startKey);
    //        KeyValue keyValue = new KeyValue();
    //        if(CacheUtil.checkAnyExcludeExists(constraints))
    //        {
    //            filter = new IncludeExcludeKeyFilterImpl(filterModel, keyGen, endKey);
    //
    //            scanner = new FilterTreeScanner(startKeyBytes, keyGen.generateKey(endKey), keyGen, keyValue, new int[0],
    //                    fileHolder,true);
    //
    //            ((FilterTreeScanner)scanner).setFilter(filter);
    //        }
    //        else if(CacheUtil.checkAnyIncludeExists(constraints))
    //        {
    //            filter = new KeyFilterImpl(filterModel, keyGen, endKey);
    //
    //            scanner = new FilterTreeScanner(startKeyBytes, keyGen.generateKey(endKey), keyGen, keyValue, new int[0],
    //                    fileHolder,true);
    //
    //            ((FilterTreeScanner)scanner).setFilter(filter);
    //        }
    //        else
    //        {
    //            scanner = new NonFilterTreeScanner(startKeyBytes, null, keyGen, keyValue, new int[0], fileHolder);
    //        }
    //        // dataCache.initializeScanner(startKeyBytes, scanner);
    //        return scanner;
    //    }
    //
    //    /**
    //     *This method handles the point query based on filter model. 
    //     * 
    //     * @param msrOrdinal
    //     * @param filterModel
    //     * @param msrs
    //     * @param slice
    //     * @param dataStore
    //     * @param dimTables
    //     * @param keyGenerator
    //     * @return  the result map.
    //     * @throws KeyGenException
    //     *
    //     */
    ////    private Map<ByteArrayWrapper, MeasureAggregator[]> handlePointQueries(int[] msrOrdinal, InMemFilterModel filterModel,
    ////            Measure[] msrs, InMemoryCube slice, DataStore dataStore, Dimension[] dimTables, KeyGenerator keyGenerator)
    ////            throws KeyGenException
    ////    {
    ////        long starttime = System.currentTimeMillis();
    ////        List<long[]> keyList = new ArrayList<long[]>();
    ////        //
    ////        addPointKeyInList(filterModel.getIncludePredicateKeys(), -DIMENSION_DEFAULT, new long[dimTables.length], keyList);
    ////
    ////        Map<ByteArrayWrapper, MeasureAggregator[]> result = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(keyList.size());
    ////        //
    ////        for(int i = 0;i < keyList.size();i++)// Long[] key : keyList)
    ////        {
    ////            long[] key = keyList.get(i);
    ////            byte[] generateKey = keyGenerator.generateKey(key);
    ////            KeyValue data = dataStore.get(generateKey, null);
    ////            //
    ////            if(null == data)
    ////            {
    ////                continue;
    ////            }
    ////            //
    ////            ByteArrayWrapper dimensionsRowWrapper = new LocalByteArrayWrapper();
    ////            dimensionsRowWrapper.setActualData(data.getArray(), data.getKeyOffset(), data.getKeyLength());
    ////            MeasureAggregator[] aggregators = AggUtil.getAggregators(msrs, false, keyGenerator, slice);
    ////            int[] measureOrdinal = msrOrdinal;
    ////            for(int j = 0;j < measureOrdinal.length;j++)
    ////            {
    ////                aggregators[j].agg(data.getValue(measureOrdinal[j]), generateKey,0,generateKey.length);
    ////            }
    ////            result.put(dimensionsRowWrapper, aggregators);
    ////        }
    ////
    ////        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "##### Total time taken for point queries "
    ////                + (System.currentTimeMillis() - starttime) + " for " + keyList.size() + " number of keys ####");
    ////        return result;
    ////    }
    //
    ////    /**
    ////     * Project Name NSE V3R7C00 
    ////     * Module Name : MOLAP
    ////     * Author :C00900810
    ////     * Created Date :25-Jun-2013
    ////     * FileName : InMemoryQueryExecutor.java
    ////     * Class Description : 
    ////     * Version 1.0
    ////     */
    ////    private static class LocalByteArrayWrapper extends ByteArrayWrapper
    ////    {
    ////        @Override
    ////        public int hashCode()
    ////        {
    ////            return getHashCode(getData());
    ////        }
    ////
    ////        @Override
    ////        public boolean equals(Object other)
    ////        {
    ////            // check whether other is type of ByteArrayWrapper
    ////            if(other instanceof ByteArrayWrapper)
    ////            {
    ////                return super.equals(other);
    ////            }
    ////            return false;
    ////        }
    ////        
    ////    }
    //
    //    
    // 
    //    
    ////    private int[][] getMaskedByteRangeForSorting(Dimension[] queryDimensions, KeyGenerator generator,int[] maskedRanges)
    ////    {
    ////        int[][] dimensionCompareIndex= new int[queryDimensions.length][];
    ////        int index=0;
    ////        for(int i = 0;i < queryDimensions.length;i++)
    ////        {
    ////            Set<Integer> integers = new TreeSet<Integer>();
    ////            int[] range = generator.getKeyByteOffsets(queryDimensions[i].getOrdinal());
    ////            for(int j = range[0];j <= range[1];j++)
    ////            {
    ////                integers.add(j);
    ////            }
    ////            dimensionCompareIndex[index]=new int[integers.size()];
    ////            int j = 0;
    ////            for(Iterator<Integer> iterator = integers.iterator();iterator.hasNext();)
    ////            {
    ////                Integer integer = (Integer)iterator.next();
    ////                 dimensionCompareIndex[index][j++] = integer.intValue();
    ////            }
    ////            index++;
    ////        }
    ////        
    ////        for(int i = 0;i < dimensionCompareIndex.length;i++)
    ////        {
    ////            int[] range = dimensionCompareIndex[i];
    ////            for(int j = 0;j < range.length;j++)
    ////            {
    ////                for(int k = 0;k < maskedRanges.length;k++)
    ////                {
    ////                    if(range[j] == maskedRanges[k])
    ////                    {
    ////                        range[j] = k;
    ////                        break;
    ////                    }
    ////                }
    ////            }
    ////        }
    ////        
    ////        return dimensionCompareIndex;
    ////    }
    //    
    //    /**
    //     * Get order of dimensions which should be in actual dimension order.
    //     * @param queryDimensions
    //     * @param actualDims
    //     * @return
    //     */
    //    private Dimension[] getOrderAsperActualDims(Dimension[] queryDimensions,Dimension[] actualDims,Dimension[] actualDimsRows,Dimension[] actualDimsCols,Dimension topNdim)
    //    {
    //        List<Dimension> foundDims = new ArrayList<Dimension>();
    //
    //        fillDimsAsperActualDimOrder(queryDimensions, actualDims, foundDims);
    //        
    //        if(topNdim != null)
    //        {
    //            int topNIndex = QueryExecutorUtil.getTopNIndex(actualDimsCols, topNdim);
    //            if(topNIndex >= 0)
    //            {
    //                fillDimsAsperActualDimOrder(queryDimensions, actualDimsCols, foundDims);
    //            }
    //        }
    //        
    //        if(foundDims.size() != queryDimensions.length)
    //        {
    //            for(int i = 0;i < queryDimensions.length;i++)
    //            {
    //                boolean found = false;
    //                for(int j = 0;j < actualDims.length;j++)
    //                {
    //                    if(actualDims[j].getOrdinal() == queryDimensions[i].getOrdinal())
    //                    {
    //                        found = true;
    //                        break;
    //                    }
    //                }
    //                if(!found && QueryExecutorUtil.contain(queryDimensions[i], foundDims))
    //                {
    //                    foundDims.add(queryDimensions[i]);
    //                }
    //            }
    //        }
    //        
    //        return foundDims.toArray(new Dimension[foundDims.size()]);
    //        
    //    }
    //
    //    /**
    //     * @param queryDimensions
    //     * @param actualDims
    //     * @param foundDims
    //     */
    //    public void fillDimsAsperActualDimOrder(Dimension[] queryDimensions, Dimension[] actualDims,
    //            List<Dimension> foundDims)
    //    {
    //        for(int i = 0;i < actualDims.length;i++)
    //        {
    //            for(int j = 0;j < queryDimensions.length;j++)
    //            {
    //                if(actualDims[i].getOrdinal() == queryDimensions[j].getOrdinal()
    //                        && actualDims[i].getHierName().equals(queryDimensions[j].getHierName())
    //                        && actualDims[i].getDimName().equals(queryDimensions[j].getDimName())
    //                        && actualDims[i].getName().equals(queryDimensions[j].getName()))
    //                {
    //                    if(!QueryExecutorUtil.contain(actualDims[i], foundDims))
    //                    {
    //                        foundDims.add(actualDims[i]);
    //                    }
    //                    break;
    //                }
    //            }
    //        }
    //    }
    //    
    //    private byte[][] getMaksedKeyForSorting(Dimension[] queryDimensions, KeyGenerator generator, int[][] dimensionCompareIndex,int[] maskedRanges) throws KeyGenException
    //    {
    //        byte[][] maskedKey = new byte[queryDimensions.length][];
    //        
    //        for(int i = 0;i < dimensionCompareIndex.length;i++)
    //        {
    //            long[] key = new long[generator.getDimCount()];
    //            maskedKey[i]=new byte[dimensionCompareIndex[i].length];
    //            key[queryDimensions[i].getOrdinal()] = Long.MAX_VALUE;
    //            
    //            byte[] mdKey = generator.generateKey(key);
    //            
    //            byte[] maskedMdKey = new byte[maskedRanges.length];
    //            
    //            for(int k = 0;k < maskedMdKey.length;k++)
    //            {
    //                maskedMdKey[k] = mdKey[maskedRanges[k]];
    //            }
    //            for(int j = 0;j < dimensionCompareIndex[i].length;j++)
    //            {
    //                maskedKey[i][j]=maskedMdKey[dimensionCompareIndex[i][j]];
    //            }
    //        }
    //        return maskedKey;
    //    }
    //    
    //
    //    
    //    /**
    //     * Create actual data from surrogated MD key
    //     * @param slices
    //     * @param hIterator
    //     * @param data
    //     * @param dims
    //     * @param properties
    //     * @param measureCount
    //     * @param isHeirarchy
    //     * @param tableName
    //     * @param avgIndexes
    //     * @param countMsrIndex
    //     * @param isCtMsrEXstInCurr
    //     * @param msrArray
    //     * @param keyGenerator
    //     * @param maskedKeyRanges
    //     * @param isNameColumnOnlyReq
    //     */
    //    private void createDataFromAggregates(List<InMemoryCube> slices, MolapResultHolder hIterator,
    //            Map<ByteArrayWrapper, MeasureAggregator[]> data, Dimension[] dims, boolean properties, int measureCount,
    //            boolean isHeirarchy, String tableName, List<Integer> avgIndexes, int countMsrIndex, boolean isCtMsrEXstInCurr,
    //            Measure[] msrArray, KeyGenerator keyGenerator, int[] maskedKeyRanges, boolean isNameColumnOnlyReq, MolapQueryExecutorModel queryModel)
    //    {
    //        
    //        int currentRow = 0;
    //        
    //
    //        List<Integer> dimIndexMap = new ArrayList<Integer>();
    //
    //        int attributeCount = 0;
    //
    //        int dimensionCount = dims.length;
    //        for(int i = 0;i < dims.length;i++)
    //        {
    //            dimIndexMap.add(attributeCount + i);
    ////            if(properties)
    ////            {
    ////                int attLen = dims[i].getTotalAttributeCount();
    ////                // if(direct)
    ////                // {
    ////                // if(attLen > 0);
    ////                // {
    ////                // --attLen;
    ////                // }
    ////                // }
    ////                attributeCount += attLen;
    ////            }
    ////            else if(isNameColumnOnlyReq)
    ////            {
    ////                int attLen = dims[i].isHasNameColumn()?1:0;
    ////                // if(direct)
    ////                // {
    ////                // if(attLen > 0);
    ////                // {
    ////                // --attLen;
    ////                // }
    ////                // }
    ////                attributeCount += attLen;
    ////            }
    //                
    //        }
    //
    //        int msrIndex = dimensionCount;// +attributeCount;
    //        int f = 0;
    //        int counter = msrIndex;
    //        for(;f < measureCount;f++, counter++)
    //        {
    //            dimIndexMap.add(counter);
    //        }
    //
    //        if((null==data || data.isEmpty()))
    //        {
    //            int size = dimensionCount + attributeCount;
    //            if(!isHeirarchy)
    //            {
    //               size += measureCount;
    //            }
    //            Object[][] resultDataA = new Object[size][0];
    //            hIterator.setObject(resultDataA);
    //            return ;
    //        }
    //
    //        double[][] resultData = new double[data.size()][dimensionCount + measureCount];
    //        Set<Entry<ByteArrayWrapper, MeasureAggregator[]>> entries = data.entrySet();
    //        double valueIDCounter = 0;
    //        int[] measureWithCustomAgg =new int[measureCount];
    //        Double2ObjectMap<Object> idVsValueMap = new Double2ObjectOpenHashMap<Object>();
    //        for(Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> iterator = entries.iterator();iterator.hasNext();)
    //        {
    //            Entry<ByteArrayWrapper, MeasureAggregator[]> entry = iterator.next();
    //            ByteArrayWrapper keyWrapper = entry.getKey();
    //
    //            long[] keyArray = keyGenerator.getKeyArray(keyWrapper.getMaskedKey(),maskedKeyRanges);
    //
    //            for(int i = 0;i < dimensionCount;i++)
    //            {
    //                
    ////CHECKSTYLE:OFF    Approval No:Approval-286
    //                resultData[currentRow][i] = isHeirarchy?getSortIndexById(slices, dims[i], (int)keyArray[dims[i].getOrdinal()]):keyArray[dims[i].getOrdinal()];
    //            }//CHECKSTYLE:ON
    //
    //            // Fill the measures in data row from member
    //            MeasureAggregator[] d = entry.getValue();
    //            for(int i = 0;i < measureCount;i++)
    //            {
    //                if(d[i] instanceof AbstractMeasureAggregator)
    //                {
    //                    measureWithCustomAgg[i] = DIMENSION_DEFAULT;
    //                    idVsValueMap.put(++valueIDCounter, d[i].getValueObject());
    //                    resultData[currentRow][dimensionCount + i] = valueIDCounter;
    //                }
    //                else
    //                {
    //                    resultData[currentRow][dimensionCount + i] = d[i].getValue();
    //                }
    //            }
    //            currentRow++;
    //            if(currentRow%100 == 0)
    //            {
    //                isInterrupted();
    //            }
    //
    //        }
    //        double[][] result = getResult(slices, dims, isHeirarchy, resultData);
    ////        if(!isHeir)
    ////        {
    //            // added method for source monitor fix
    //           // MeasureFilterUtil.calculateAvgsAndUpdateData(result, avgIndexes, countMsrIndex, msrCount, dimensionCount);
    ////        }
    //        
    //        isInterrupted();
    //   
    ////        if(!isHeir && msrConstraints != null)
    ////        {
    ////           // result = MeasureFilterUtil.filterMeasures(result, msrConstraints, dimensionCount);
    ////        }
    ////        isInterrupted();
    ////        if(topNModel != null && result.length > 0)
    ////        {
    //////            result = TopNProcessor.getTopNRows(result, topNModel.getDimIndex(), topNModel.getMsrIndex()
    //////                    + dimensionCount, topNModel.getCount(), dimensionCount, topNModel.getTopNType(),
    //////                    msrArray[topNModel.getMsrIndex()].getAggName(),countMsrIndex,avgIndexes.indexOf(topNModel.getMsrIndex()));
    ////        }
    ////        
    ////        else if(msrSortModel != null)
    ////        {
    //////            sortMeasure(msrSortModel.getMeasureIndex(), dimensionCount, result, msrSortModel.getSortOrder());
    ////        }
    //        isInterrupted();
    //        if(result.length > 0)
    //        {
    //            result = encodeToRows(result);
    //            isInterrupted();
    //            //perf
    //            
    //            // int [][] sIndex = new int [dimensionCount][resultData.length];
    //            // result = encodeToRows(resultData, sliceIndex, sIndex,dimensionCount);
    //            sortAndSetResult(slices, hIterator, dims, properties, measureCount, dimIndexMap, attributeCount, dimensionCount,
    //                    msrIndex, result,measureWithCustomAgg,idVsValueMap,avgIndexes,countMsrIndex,isCtMsrEXstInCurr, isHeirarchy, msrArray,isNameColumnOnlyReq,queryModel);
    //        }
    //    }
    //
    //    /**
    //     * Create actual data from surrogated MD key
    //     * @param slices
    //     * @param hIterator
    //     * @param data
    //     * @param dims
    //     * @param properties
    //     * @param msrCount
    //     * @param isHeir
    //     * @param tableName
    //     * @param avgIndexes
    //     * @param countMsrIndex
    //     * @param isCtMsrEXstInCurr
    //     * @param msrArray
    //     * @param keyGenerator
    //     * @param maskedKeyRanges
    //     * @param isNameColumnOnlyReq
    //     */
    //    private void createDataFromAggregatesSpark(List<InMemoryCube> slices, MolapResultHolder hIterator,
    //            Map<ByteArrayWrapper, MeasureAggregator[]> data, Dimension[] dims, boolean properties, int msrCount,
    //            boolean isHeir, String tableName, List<Integer> avgIndexes, int countMsrIndex, boolean isCtMsrEXstInCurr,
    //            Measure[] msrArray, KeyGenerator keyGenerator, int[] maskedKeyRanges, boolean isNameColumnOnlyReq,
    //            Cube cube, String factTableName)
    //    {
    //        
    //        int currentRow = 0;
    //        
    //
    //        List<Integer> dimIndexMap = new ArrayList<Integer>();
    //
    //        int attributeCount = 0;
    //
    //        int dimensionCount = dims.length;
    //        for(int i = 0;i < dims.length;i++)
    //        {
    //            dimIndexMap.add(attributeCount + i);
    //            if(properties)
    //            {
    //                int attLen = dims[i].getTotalAttributeCount();
    //                attributeCount += attLen;
    //            }
    //            else if(isNameColumnOnlyReq)
    //            {
    //                int attLen = dims[i].isHasNameColumn()?1:0;
    //                attributeCount += attLen;
    //            }
    //                
    //        }
    //
    //        int msrIndex = dimensionCount;// +attributeCount;
    //        int f = 0;
    //        int counter = msrIndex;
    //        for(;f < msrCount;f++, counter++)
    //        {
    //            dimIndexMap.add(counter);
    //        }
    //
    //        if((null==data || data.isEmpty()))
    //        {
    //            int size = dimensionCount + attributeCount;
    //            if(!isHeir)
    //            {
    //               size += msrCount;
    //            }
    //            Object[][] resultDataA = new Object[size][0];
    //            hIterator.setObject(resultDataA);
    //            hIterator.setKeys(new ArrayList<MolapKey>());
    //            hIterator.setValues(new ArrayList<MolapValue>());
    //            return ;
    //        }
    //
    //        Object[][] resultData = new Object[data.size()][dimensionCount + msrCount];
    //        Set<Entry<ByteArrayWrapper, MeasureAggregator[]>> entries = data.entrySet();
    //        double valueIDCounter = 0;
    //        int[] measureWithCustomAgg =new int[msrCount];
    //        Double2ObjectMap<Object> idVsValueMap = new Double2ObjectOpenHashMap<Object>();
    //        for(Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> iterator = entries.iterator();iterator.hasNext();)
    //        {
    //            Entry<ByteArrayWrapper, MeasureAggregator[]> entry = iterator.next();
    //            ByteArrayWrapper keyWrapper = entry.getKey();
    //
    //            long[] keyArray = keyGenerator.getKeyArray(keyWrapper.getMaskedKey(),maskedKeyRanges);
    //
    //            for(int i = 0;i < dimensionCount;i++)
    //            {
    ////CHECKSTYLE:OFF    Approval No:Approval-286
    //                resultData[currentRow][i] = isHeir?getSortIndexById(slices, dims[i], (int)keyArray[dims[i].getOrdinal()]):keyArray[dims[i].getOrdinal()];
    //            }//CHECKSTYLE:ON
    //
    //            // Fill the measures in data row from member
    //            MeasureAggregator[] d = entry.getValue();
    //            for(int i = 0;i < msrCount;i++)
    //            {
    //                if(d[i] instanceof AbstractMeasureAggregator)
    //                {
    //                    measureWithCustomAgg[i] = DIMENSION_DEFAULT;
    //                    idVsValueMap.put(++valueIDCounter, d[i].getValueObject());
    //                    resultData[currentRow][dimensionCount + i] = valueIDCounter;
    //                }
    //                else
    //                {
    //                    resultData[currentRow][dimensionCount + i] = d[i];
    //                }
    //            }
    //            currentRow++;
    //            if(currentRow%100 == 0)
    //            {
    //                isInterrupted();
    //            }
    //
    //        }
    ////        double[][] result = getResult(slices, dims, isHeir, resultData);
    //        if(!isHeir)
    //        {
    //            // added method for source monitor fix
    //       }
    //        
    //        isInterrupted();
    //   
    //
    //        isInterrupted();
    //        if(data.size() > 0)
    //        {
    //            resultData = encodeToRows(resultData);
    //            isInterrupted();
    //            //perf
    //            
    //            sortAndSetResultSpark(slices, hIterator, dims, properties, msrCount, dimIndexMap, attributeCount, dimensionCount,
    //                    msrIndex, resultData,measureWithCustomAgg,idVsValueMap,avgIndexes,countMsrIndex,isCtMsrEXstInCurr, isHeir, msrArray,isNameColumnOnlyReq, cube, factTableName);
    //        }
    //        else
    //        {
    //            hIterator.setKeys(new ArrayList<MolapKey>());
    //            hIterator.setValues(new ArrayList<MolapValue>());
    //        }
    //    }
    //    /**
    //     * Create actual data from surrogated MD key
    //     * @param slices
    //     * @param hIterator
    //     * @param data
    //     * @param dims
    //     * @param properties
    //     * @param msrCount
    //     * @param isHeir
    //     * @param tableName
    //     * @param avgIndexes
    //     * @param countMsrIndex
    //     * @param isCtMsrEXstInCurr
    //     * @param msrArray
    //     * @param keyGenerator
    //     * @param maskedKeyRanges
    //     * @param isNameColumnOnlyReq
    //     */
    //    private void createDataFromAggregatesSpark(List<InMemoryCube> slices, MolapResultHolder hIterator,
    //            QueryResult data, Dimension[] dims, boolean properties, int msrCount,
    //            boolean isHeir, String tableName, List<Integer> avgIndexes, int countMsrIndex, boolean isCtMsrEXstInCurr,
    //            Measure[] msrArray, KeyGenerator keyGenerator, int[] maskedKeyRanges, boolean isNameColumnOnlyReq,
    //            Cube cube, String factTableName, boolean isDetailQuery)
    //    {
    //        
    //        int currentRow = 0;
    //        
    //
    //        List<Integer> dimIndexMap = new ArrayList<Integer>();
    //
    //        int attributeCount = 0;
    //
    //        int dimensionCount = dims.length;
    //        
    ////        for(int i = 0;i < dims.length;i++)
    ////        {
    ////            if(!dims[i].isQueryForDistinctCount())
    ////            {
    ////                dimensionCount++;
    ////            }
    ////            else
    ////            {
    ////                msrCount
    ////            }
    ////        }
    //        for(int i = 0;i < dims.length;i++)
    //        {
    //            dimIndexMap.add(attributeCount + i);
    //            if(properties)
    //            {
    //                int attLen = dims[i].getTotalAttributeCount();
    //                attributeCount += attLen;
    //            }
    //            else if(isNameColumnOnlyReq)
    //            {
    //                int attLen = dims[i].isHasNameColumn()?1:0;
    //                attributeCount += attLen;
    //            }
    //                
    //        }
    //
    //        int msrIndex = dimensionCount;// +attributeCount;
    //        int f = 0;
    //        int counter = msrIndex;
    //        for(;f < msrCount;f++, counter++)
    //        {
    //            dimIndexMap.add(counter);
    //        }
    //
    //        if((null==data || data.size()<1))
    //        {
    //            int size = dimensionCount + attributeCount;
    //            if(!isHeir)
    //            {
    //               size += msrCount;
    //            }
    //            Object[][] resultDataA = new Object[size][0];
    //            hIterator.setObject(resultDataA);
    //            hIterator.setKeys(new ArrayList<MolapKey>());
    //            hIterator.setValues(new ArrayList<MolapValue>());
    //            return ;
    //        }
    //
    //        Object[][] resultData = new Object[data.size()][dimensionCount + msrCount];
    //        QueryResultIterator iterator=data.iterator();
    //        double valueIDCounter = 0;
    //        int[] measureWithCustomAgg =new int[msrCount];
    //        Double2ObjectMap<Object> idVsValueMap = new Double2ObjectOpenHashMap<Object>();
    //        while(iterator.hasNext())
    //        {
    //            ByteArrayWrapper keyWrapper = iterator.getKey();
    //
    //            long[] keyArray = keyGenerator.getKeyArray(keyWrapper.getMaskedKey(),maskedKeyRanges);
    //
    //            for(int i = 0;i < dimensionCount;i++)
    //            {
    ////CHECKSTYLE:OFF    Approval No:Approval-286
    //                resultData[currentRow][i] = isHeir?getSortIndexById(slices, dims[i], (int)keyArray[dims[i].getOrdinal()]):keyArray[dims[i].getOrdinal()];
    //            }//CHECKSTYLE:ON
    //
    //            // Fill the measures in data row from member
    //            MeasureAggregator[] d = iterator.getValue();
    //            for(int i = 0;i < msrCount;i++)
    //            {
    //                if(d[i] instanceof AbstractMeasureAggregator)
    //                {
    //                    measureWithCustomAgg[i] = DIMENSION_DEFAULT;
    //                    idVsValueMap.put(++valueIDCounter, d[i].getValueObject());
    //                    resultData[currentRow][dimensionCount + i] = valueIDCounter;
    //                }
    //                else
    //                {
    //                    resultData[currentRow][dimensionCount + i] = d[i];
    //                }
    //            }
    //            currentRow++;
    //            if(currentRow%100 == 0)
    //            {
    //                isInterrupted();
    //            }
    //
    //        }
    ////        double[][] result = getResult(slices, dims, isHeir, resultData);
    //        if(!isHeir)
    //        {
    //            // added method for source monitor fix
    //       }
    //        
    //        isInterrupted();
    //   
    //
    //        isInterrupted();
    //        if(data.size() > 0)
    //        {
    //            resultData = encodeToRows(resultData);
    //            isInterrupted();
    //            //perf
    //            if(!isDetailQuery)
    //            {
    //                sortAndSetResultSpark(slices, hIterator, dims, properties, msrCount, dimIndexMap, attributeCount, dimensionCount,
    //                        msrIndex, resultData,measureWithCustomAgg,idVsValueMap,avgIndexes,countMsrIndex,isCtMsrEXstInCurr, isHeir, msrArray,isNameColumnOnlyReq, cube, factTableName);
    //            }
    //            else
    //            {
    //                sortAndSetResultSparkDetail(slices, hIterator, dims, properties, msrCount, dimIndexMap, attributeCount, dimensionCount,
    //                        msrIndex, resultData,measureWithCustomAgg,idVsValueMap,avgIndexes,countMsrIndex,isCtMsrEXstInCurr, isHeir, msrArray,isNameColumnOnlyReq, cube, factTableName);
    //            }
    //        }
    //        else
    //        {
    //            hIterator.setKeys(new ArrayList<MolapKey>());
    //            hIterator.setValues(new ArrayList<MolapValue>());
    //        }
    //    }
    ////     * @param limit
    ////     */
    ////    private void raiseLimitExceedException(Map<ByteArrayWrapper, MeasureAggregator[]> data, Dimension[] dims,int[] maskedKeyRanges,KeyGenerator keyGenerator)
    ////    {
    ////        String limitExceededHint = getLimitHint(data,dims,maskedKeyRanges,keyGenerator);
    ////        
    ////        ResourceLimitExceededException ex = MondrianResource.instance().RowLimitExceededWithHint
    ////        .ex(data.size(),rowLimit,limitExceededHint);
    ////        throw ex;
    ////    }
    ////  
    //
    //    
    //
    //    
    //    private String getLimitHint(Map<ByteArrayWrapper, MeasureAggregator[]> data, Dimension[] dims,int[] maskedKeyRanges,KeyGenerator keyGenerator)
    //    {
    //        Set<Entry<ByteArrayWrapper, MeasureAggregator[]>> entries = data.entrySet();
    //        int dimensionCount = dims.length;
    //        String resultLimit = MolapProperties.getInstance().getProperty(MolapCommonConstants.MOLAP_RESULT_SIZE_KEY,MolapCommonConstants.MOLAP_RESULT_SIZE_DEFAULT);
    //        int limit = Integer.parseInt(resultLimit);
    //        if(dimensionCount == 0 || limit == 0)
    //        {
    //            return "Please have a higher limit for results";
    //        }
    //        
    //        Map<Dimension,Set<Integer>> dimMap = new HashMap<Dimension,Set<Integer>>();
    //        for(Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> iterator = entries.iterator();iterator.hasNext();)
    //        {
    //            Entry<ByteArrayWrapper, MeasureAggregator[]> entry = iterator.next();
    //            ByteArrayWrapper keyWrapper = entry.getKey();
    //
    //            long[] keyArray = keyGenerator.getKeyArray(keyWrapper.getMaskedKey(),maskedKeyRanges);
    //
    //            for(int i = 0;i < dimensionCount ;i++)
    //            {
    //                Set<Integer> set = dimMap.get(dims[i]);
    //                if(set == null)
    //                {
    //                    set = new HashSet<Integer>();
    //                }//CHECKSTYLE:OFF    Approval No:Approval-286
    //                set.add((int)keyArray[dims[i].getOrdinal()]);
    //                dimMap.put(dims[i],set);
    //            }//CHECKSTYLE:ON
    //        }
    //        
    //        Map<String,Integer> finalDims = new HashMap<String,Integer>();
    //        for(int i = 0;i < dimensionCount ;i++)
    //        {
    //            Dimension dim = dims[i];
    //            Set<Integer> set = dimMap.get(dim);
    //            finalDims.put(dim.getName(), set.size());
    //            if(i > 0 && dims[i].getHierName().equals(dims[i-1].getHierName()))
    //            {
    //                int previousCardinality = finalDims.remove(dims[i-1].getName());
    //                String currentDimName = dims[i].getName();
    //                finalDims.put(currentDimName,finalDims.get(currentDimName)*previousCardinality);
    //            }
    //        }
    //        StringBuilder s = new StringBuilder("$");
    //        int old = limit;
    //        boolean limitCrossed = false;
    //       for(int i = 0;i < dimensionCount ;i++)
    //        {
    //            String dimName=prepareDimensionUniqueName(dims[i]);
    //            Integer count = finalDims.get(dims[i].getName());
    //            if(count == null )
    //            {
    //                continue;
    //            }
    //            if(limitCrossed)
    //            {
    //                s.append(dimName+':'+1+'$');
    //                continue;
    //            }
    //           if(old > count)
    //           {
    //               old = old/count;
    //           }
    //           else
    //           {
    //               s.append(dimName+':'+old+'$');
    //               limitCrossed = true;
    //           }
    //        }
    //        return s.toString();
    //    }
    //    
    //    /*
    //     * prepares unique name of MOLAP Dimension
    //     * if hierarchy name is same as dimension name then 
    //     * [Dimsionname].[levelname]
    //     * else
    //     * [dimensionname.hierarchyname].[levelname]
    //     */
    //    private String prepareDimensionUniqueName(Dimension dimension)
    //    {
    //        StringBuilder sb= new StringBuilder();
    //        sb.append('[');
    //        sb.append(dimension.getDimName());
    //        if(!dimension.getDimName().equals(dimension.getHierName()))
    //        {
    //            sb.append('.');
    //            sb.append(dimension.getHierName());
    //        }
    //        sb.append(']');
    //        sb.append('.');
    //        sb.append('[');
    //        sb.append(dimension.getName());
    //        sb.append(']');
    //        return sb.toString();
    //    }
    //  
    //    
    //    
    //    /**
    //     * @param slices
    //     * @param hIterator
    //     * @param data
    //     * @param dims
    //     * @param properties
    //     * @param msrCount
    //     * @param isHeir
    //     * @param tableName
    //     * @param avgIndexes
    //     * @param countMsrIndex
    //     * @param isCtMsrEXstInCurr
    //     * @param keyGenerator
    //     * @return
    //     */
    //    private Int2ObjectMap<List<int[]>> createDataFromAggregatesWithSurrogates(List<InMemoryCube> slices, MolapResultHolder hIterator,
    //            Map<ByteArrayWrapper, MeasureAggregator[]> data, Dimension[] dims, boolean properties, int msrCount,
    //            boolean isHeir, String tableName,List<Integer> avgIndexes,int countMsrIndex,boolean isCtMsrEXstInCurr,int[] maskedKeyRanges,KeyGenerator keyGenerator)
    //    {
    //        int currentRow = 0;
    //        int dimensionCount = dims.length;
    //        int[][] resultData = new int[data.size()][dimensionCount + msrCount];
    //        Set<Entry<ByteArrayWrapper, MeasureAggregator[]>> entries = data.entrySet();
    //
    //        for(Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> iterator = entries.iterator();iterator.hasNext();)
    //        {
    //            Entry<ByteArrayWrapper, MeasureAggregator[]> entry = iterator.next();
    //            ByteArrayWrapper keyWrapper = entry.getKey();
    //
    //            long[] keyArray = keyGenerator.getKeyArray(keyWrapper.getMaskedKey(),maskedKeyRanges);
    //
    //            for(int i = 0;i < dimensionCount;i++)
    //            {
    ////CHECKSTYLE:OFF    Approval No:Approval-293
    //                resultData[currentRow][i] = (int)keyArray[dims[i].getOrdinal()];
    //            }//CHECKSTYLE:ON
    //            currentRow++;
    //        }
    //        // put data in set<int> to avoid duplicates
    //        Int2ObjectMap<List<int[]>> map = new Int2ObjectArrayMap<List<int[]>>();
    //        
    //        /*for(int i =0; i< resultData.length;i++)
    //        {
    //            Set<Integer> set = map.get(resultData[i][0]);
    //            if(null == set)
    //            {
    //                set = new TreeSet<Integer>();
    //                map.put(resultData[i][0], set);
    //            }
    //            set.add(resultData[i][1]);
    //        }*/
    //        
    //        if(null==resultData || resultData.length<1)
    //        {
    //            return map;
    //        }
    //        int primaryKeyIndex = resultData[0].length-1;
    //        
    //        /*for(int i =0; i< resultData.length;i++)
    //        {
    //            Set<Integer> set = map.get(resultData[i][0]);
    //            if(null == set)
    //            {
    //                set = new TreeSet<Integer>();
    //                map.put(resultData[i][0], set);
    //            }
    //            set.add(resultData[i][1]);
    //        }*/
    //        
    //        for(int i =0; i< resultData.length;i++)
    //        {
    //            List<int[]> set = map.get(resultData[i][primaryKeyIndex]);
    //            if(null == set)
    //            {
    //                set = new ArrayList<int[]>();
    //                map.put(resultData[i][primaryKeyIndex], set);
    //            }
    //            set.add(resultData[i]);
    //        }
    //        
    //        // Now convert set to int[] and return
    ////        Int2ObjectMap<int[]> map1 = new Int2ObjectArrayMap<int[]>();
    ////        for (Entry<Integer, Set<Integer>> entry : map.entrySet())
    ////        {
    ////            Set<Integer> intSet = entry.getValue();
    ////            int[] ints = new int[intSet.size()];
    ////            int index = 0;
    ////            for(Integer i : intSet){
    ////                ints[index++] = i;
    ////            }
    ////            map1.put(entry.getKey(), ints);
    ////        }
    //        
    //        return map;
    //    }
    //
    //    /**
    //     * 
    //     * @param slices
    //     * @param dims
    //     * @param isHeir
    //     * @param tableName
    //     * @param resultData
    //     * @return
    //     * 
    //     */
    //    private double[][] getResult(List<InMemoryCube> slices, Dimension[] dims, boolean isHeir,
    //            double[][] resultData)
    //    {
    //        double[][] result = resultData;
    //        if(dims.length > 0 && isHeir)
    //        {
    //            result = sortMembers(resultData,dims);
    //        }
    //        return result;
    //    }
    //    
    //
    //    
    //
    //    private void sortAndSetResult(List<InMemoryCube> slices, MolapResultHolder hIterator, Dimension[] dims,
    //            boolean properties, int msrCount, List<Integer> dimIndexMap, int attributeCount, int dimensionCount,
    //            int msrIndex, double[][] result, int[] measureWithCustomAgg, Double2ObjectMap<Object> idVsValueMap,
    //            List<Integer> avgIndexes, int countMsrIndex, boolean isCtMsrEXstInCurr, boolean isHeir, Measure[] msrList,
    //            boolean isNameColumnOnlyReq,MolapQueryExecutorModel queryModel)// , int [][]sliceIndex)
    //    {
    //        Member member;
    //
    //        if(!isCtMsrEXstInCurr && countMsrIndex >-1)
    //        {
    //            msrCount--;
    //        }
    //        Object[][] resultDataA = new Object[dimensionCount + attributeCount + msrCount][result[0].length];
    //        
    //        
    //        int decimalPointers = Byte.parseByte(MolapProperties
    //                .getInstance().getProperty(MolapCommonConstants.MOLAP_DECIMAL_POINTERS_AGG,
    //                        MolapCommonConstants.MOLAP_DECIMAL_POINTERS_AGG_DEFAULT));
    //        double multiplier = Math.pow(10, decimalPointers);
    //
    //        for(int j = 0;j < resultDataA[0].length;j++)
    //        {
    //
    //            for(int i = 0;i < dimensionCount;i++)
    //            {
    //                // Get the name by surrogate key
    //                // member = getMemberBySortIndex(slices.get(sliceIndex[i][j]),
    //                // dims[i],
    //                // (int)result[i][j]);
    ////CHECKSTYLE:OFF    Approval No:Approval-294
    //                // Get the name by surrogate key
    //                //itd ok to cast in integer as this part of loop is only for  dimension.!!
    //                member = getMemberBySortIndex(slices, dims[i], (int)result[i][j]);
    //                // Identify the location index to set in data row
    //                int currentDimIndex = dimIndexMap.get(i);
    //                String memString = MolapCommonConstants.MEMBER_DEFAULT_VAL;
    //                if (null != member)
    //                {
    //                    memString = member.toString();
    //                }
    ////CHECKSTYLE:ON                 
    //                resultDataA[currentDimIndex][j] = memString.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL) ? MolapCommonConstants.SQLNUllValue
    //                        : memString;
    //
    //                if((properties || isNameColumnOnlyReq) && (null != member) && (member.getAttributes() != null) && member.getAttributes().length > 0)
    //                {
    //                    //sourceMonitorFix
    //                    addPropertiesInResult(dims, properties, isNameColumnOnlyReq, member, resultDataA, j, i,
    //                            currentDimIndex);
    //                }
    //
    //            }
    //
    //            for(int i = 0;i < msrCount;i++)
    //            {
    ////                if(countMsrIndex>-1 && avgIndexes.contains(i))
    ////                {
    ////                    if(result[dimensionCount + countMsrIndex][j]==0)
    ////                    {
    ////                        resultDataA[dimensionCount + attributeCount + i][j]=result[dimensionCount + i][j];
    ////                    }
    ////                    else
    ////                    {
    ////                        resultDataA[dimensionCount + attributeCount + i][j] = result[dimensionCount + i][j]
    ////                                / result[dimensionCount + countMsrIndex][j];
    ////                    }
    ////                }
    ////                else
    ////                {//CHECKSTYLE:OFF    Approval No:Approval-295
    //                    resultDataA[dimensionCount + attributeCount + i][j] = result[dimensionCount + i][j];
    ////                }//CHECKSTYLE:ON
    //            }
    //            
    //            if(j%100 == 0)
    //            {
    //                isInterrupted();
    //            }
    //            
    //        }
    //        
    //        int startIndex = dimensionCount + attributeCount;
    //        for(int i = startIndex;i < resultDataA.length;i++)
    //        {
    //            if(measureWithCustomAgg[i - startIndex] == DIMENSION_DEFAULT)
    //            {
    //                for(int j = 0;j < resultDataA[0].length;j++)
    //                {
    //                    resultDataA[i][j] = idVsValueMap.get(resultDataA[i][j]);
    //                }
    //            }
    //              
    //        }
    //        isInterrupted();
    //        if(!isHeir)
    //        {
    //            getResult(attributeCount, dimensionCount, msrList, resultDataA, multiplier);
    //        }
    //        // below check will be called only for contains and does not contains filter and it has name column 
    //        if(isHeir && isNameColumnOnlyReq)
    //        {
    //            resultDataA=getUpdateResultData(dims, resultDataA,dimIndexMap);
    //        }
    //        if(null!=queryModel && queryModel.isSegmentCallWithFilterPresent())
    //        {
    //            resultDataA=getUpdateResultData(queryModel.getMsrs(),msrList,startIndex,resultDataA);
    //        }
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                "###########################################------ Total Number of records" + resultDataA[0].length);
    //        hIterator.setObject(resultDataA);
    //    }
    //
    //    private void sortAndSetResultSpark(List<InMemoryCube> slices, MolapResultHolder hIterator, Dimension[] dims, boolean properties,
    //            int msrCount, List<Integer> dimIndexMap, int attributeCount, int dimensionCount, int msrIndex,
    //            Object[][] result,int[] measureWithCustomAgg,Double2ObjectMap<Object> idVsValueMap,List<Integer> avgIndexes,int countMsrIndex,boolean isCtMsrEXstInCurr, boolean isHeir,
    //            Measure[] msrList,boolean isNameColumnOnlyReq, Cube cube, String faceTableName)// , int [][]sliceIndex)
    //    {
    //        Member member = null;
    //
    //        if(!isCtMsrEXstInCurr && countMsrIndex >-1)
    //        {
    //            msrCount--;
    //        }
    //        Object[][] resultDataA = new Object[dimensionCount + attributeCount + msrCount][result[0].length];
    //        
    //        List<MolapKey> keys = new ArrayList<MolapKey>();
    //        
    //        List<MolapValue> values = new ArrayList<MolapValue>();
    //        
    //        int decimalPointers = Byte.parseByte(MolapProperties
    //                .getInstance().getProperty(MolapCommonConstants.MOLAP_DECIMAL_POINTERS_AGG,
    //                        MolapCommonConstants.MOLAP_DECIMAL_POINTERS_AGG_DEFAULT));
    //        double multiplier = Math.pow(10, decimalPointers);
    //
    //        for(int j = 0;j < resultDataA[0].length;j++)
    //        {
    //            Object[] k = new Object[dimensionCount + attributeCount];
    //            for(int i = 0;i < dimensionCount;i++)
    //            {
    //                // Get the name by surrogate key
    //                // member = getMemberBySortIndex(slices.get(sliceIndex[i][j]),
    //                // dims[i],
    //                // (int)result[i][j]);
    ////CHECKSTYLE:OFF    Approval No:Approval-294
    //                // Get the name by surrogate key
    //                //itd ok to cast in integer as this part of loop is only for  dimension.!!
    //                int currentDimIndex = dimIndexMap.get(i);
    //                if(!dims[i].isQueryForDistinctCount())
    //                {
    //                    member = getMemberBySortIndex(slices, dims[i], ((Long)result[i][j]).intValue());
    //                    String memString = MolapCommonConstants.MEMBER_DEFAULT_VAL;
    //                    if (null != member)
    //                    {
    //                        memString = member.toString();
    //                    }
    //    //CHECKSTYLE:ON                 
    //                    k[currentDimIndex] = memString.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL) ? ""
    //                            : memString;
    //                }
    //                else
    //                {
    //                    k[currentDimIndex]=getGlobalSurrogates(slices, dims[i], ((Long)result[i][j]).intValue())+"";
    //                }
    //                // Identify the location index to set in data row
    //                
    //
    //                if((properties || isNameColumnOnlyReq) && (null != member) && (member.getAttributes() != null) && member.getAttributes().length > 0)
    //                {
    //                    int length = 0;
    //                    if(properties)
    //                    {
    //                        length = member.getAttributes().length;
    //                    }
    //                    else if(isNameColumnOnlyReq)
    //                    {
    //                        length = 1;
    //                    }
    //                    // if(direct)
    //                    // {
    //                    // --length;
    //                    // }
    //                    // Fill the member properties in data row
    //                    for(int pIndex = 0;pIndex < length;pIndex++)
    //                    {
    //                        Object attribute = member.getAttributes()[pIndex];
    //                        k[currentDimIndex + pIndex + DIMENSION_DEFAULT] = attribute.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL)?null: attribute;
    //                    }
    //                }
    //
    //            }
    //            keys.add(new MolapKey(k));
    //            MeasureAggregator[] v = new MeasureAggregator[msrCount];
    //            for(int i = 0;i < msrCount;i++)
    //            {
    ////                if(countMsrIndex>-1 && avgIndexes.contains(i))
    ////                {
    ////                    if(result[dimensionCount + countMsrIndex][j]==0)
    ////                    {
    ////                        resultDataA[dimensionCount + attributeCount + i][j]=result[dimensionCount + i][j];
    ////                    }
    ////                    else
    ////                    {
    ////                        resultDataA[dimensionCount + attributeCount + i][j] = result[dimensionCount + i][j]
    ////                                / result[dimensionCount + countMsrIndex][j];
    ////                    }
    ////                }
    ////                else
    ////                {//CHECKSTYLE:OFF    Approval No:Approval-295
    //                if(!msrList[i].isDistinctQuery())
    //                {
    //                    v[i] = ((MeasureAggregator)result[dimensionCount + i][j]).get();
    //                }
    //                else
    //                {
    //                    v[i] = ((MeasureAggregator)result[dimensionCount + i][j]);
    //                }
    ////                }//CHECKSTYLE:ON
    //            }
    //            
    //            for(int i = 0;i < msrCount;i++)
    //            {
    //                if(msrList[i].isDistinctQuery())
    //                {
    //                    Dimension mappedDim=null;
    //                    List<Dimension> dimensions = cube.getDimensions(faceTableName);
    //                    for(Dimension dim:dimensions)
    //                    {
    //                        if(dim.getColName().equals(msrList[i].getColName()))
    //                        {
    //                            mappedDim= dim;
    //                            break;
    //                        }
    //                    }
    //                    if(null!=mappedDim)
    //                    {
    //                        Iterator<Integer> iterator = ((DistinctCountAggregator)v[i]).getBitMap().iterator();
    //                        MeasureAggregator distinctCountAggregator = new DistinctCountAggregator(0);
    //                        int minValue = (int)((DistinctCountAggregator)v[i]).getMinValue();
    //                        while(iterator.hasNext())
    //                        {
    //                            distinctCountAggregator.agg(getGlobalSurrogates(slices, mappedDim, iterator.next()+minValue), null, 0, 0);
    //                        }
    //                        v[i]=distinctCountAggregator.get();
    //                    }
    //                    else
    //                    {
    //                        v[i]=v[i].get();
    //                    }
    //                    
    //                    
    //                    
    //                }
    //            }
    //            values.add(new MolapValue(v));
    //            if(j%100 == 0)
    //            {
    //                isInterrupted();
    //            }
    //            
    //        }
    //        
    //        int startIndex = dimensionCount + attributeCount;
    //        for(int i = startIndex;i < resultDataA.length;i++)
    //        {
    //            if(measureWithCustomAgg[i - startIndex] == DIMENSION_DEFAULT)
    //            {
    //                for(int j = 0;j < resultDataA[0].length;j++)
    //                {
    //                    resultDataA[i][j] = idVsValueMap.get(resultDataA[i][j]);
    //                }
    //            }
    //              
    //        }
    //        isInterrupted();
    ////        if(!isHeir)
    ////        {
    ////            int index = 0;
    ////            double uValue = 0;
    ////            double currentValue;
    ////            msrIndex=dimensionCount+attributeCount;
    ////            for(int i = msrIndex;i < resultDataA.length;i++)
    ////            {
    ////                Measure m = msrList[index++];
    ////
    ////                if(MolapCommonConstants.COUNT.equals(m.getAggName()) || MolapCommonConstants.DISTINCT_COUNT.equals(m.getAggName()))
    ////                {
    ////                    for(int j = 0;j < resultDataA[i].length;j++)
    ////                    {
    ////                        currentValue = (Double)resultDataA[i][j];
    ////                        double calValue = currentValue*multiplier;
    ////                        if(calValue < Long.MAX_VALUE)
    ////                        {
    ////                            resultDataA[i][j] = Math.round(calValue)/multiplier;
    ////                        }
    ////                    }
    ////                }
    ////                else if(!MolapCommonConstants.CUSTOM.equals(m.getAggName()))
    ////                {
    ////                    uValue = this.uniqueValue[m.getOrdinal()];
    ////                    for(int j = 0;j < resultDataA[i].length;j++)
    ////                    {
    ////                        currentValue = (Double)resultDataA[i][j];
    ////                        if(0 == Double.compare(currentValue, uValue))
    ////                        {
    ////                            resultDataA[i][j] = null;
    ////                        }
    ////                        else
    ////                        {
    ////                            double calValue = currentValue*multiplier;
    ////                            
    ////                            if(calValue < Long.MAX_VALUE)
    ////                            {
    ////                                resultDataA[i][j] = Math.round(calValue)/multiplier;
    ////                            }
    ////                        }
    ////                    }
    ////                }
    ////            }
    ////        }
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                "###########################################------ Total Number of records" + resultDataA[0].length);
    //        hIterator.setObject(resultDataA);
    //        hIterator.setKeys(keys);
    //        hIterator.setValues(values);
    //    }
    //    
    //    private void sortAndSetResultSparkDetail(List<InMemoryCube> slices, MolapResultHolder hIterator, Dimension[] dims, boolean properties,
    //            int msrCount, List<Integer> dimIndexMap, int attributeCount, int dimensionCount, int msrIndex,
    //            Object[][] result,int[] measureWithCustomAgg,Double2ObjectMap<Object> idVsValueMap,List<Integer> avgIndexes,int countMsrIndex,boolean isCtMsrEXstInCurr, boolean isHeir,
    //            Measure[] msrList,boolean isNameColumnOnlyReq, Cube cube, String faceTableName)// , int [][]sliceIndex)
    //    {
    //        Member member = null;
    //
    //        if(!isCtMsrEXstInCurr && countMsrIndex >-1)
    //        {
    //            msrCount--;
    //        }
    //        Object[][] resultDataA = new Object[dimensionCount + attributeCount + msrCount][result[0].length];
    //        
    //        List<MolapKey> keys = new ArrayList<MolapKey>();
    //        
    //        List<MolapValue> values = new ArrayList<MolapValue>();
    //        
    //        int decimalPointers = Byte.parseByte(MolapProperties
    //                .getInstance().getProperty(MolapCommonConstants.MOLAP_DECIMAL_POINTERS_AGG,
    //                        MolapCommonConstants.MOLAP_DECIMAL_POINTERS_AGG_DEFAULT));
    //        double multiplier = Math.pow(10, decimalPointers);
    //
    //        for(int j = 0;j < resultDataA[0].length;j++)
    //        {
    //            Object[] k = new Object[dimensionCount + attributeCount+msrCount];
    //            for(int i = 0;i < dimensionCount;i++)
    //            {
    //                // Get the name by surrogate key
    //                // member = getMemberBySortIndex(slices.get(sliceIndex[i][j]),
    //                // dims[i],
    //                // (int)result[i][j]);
    ////CHECKSTYLE:OFF    Approval No:Approval-294
    //                // Get the name by surrogate key
    //                //itd ok to cast in integer as this part of loop is only for  dimension.!!
    //                int currentDimIndex = dimIndexMap.get(i);
    //                if(!dims[i].isQueryForDistinctCount())
    //                {
    //                    member = getMemberBySortIndex(slices, dims[i], ((Long)result[i][j]).intValue());
    //                    String memString = MolapCommonConstants.MEMBER_DEFAULT_VAL;
    //                    if (null != member)
    //                    {
    //                        memString = member.toString();
    //                    }
    //    //CHECKSTYLE:ON                 
    //                    k[currentDimIndex] = memString.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL) ? ""
    //                            : memString;
    //                }
    //                else
    //                {
    //                    k[currentDimIndex]=getGlobalSurrogates(slices, dims[i], ((Long)result[i][j]).intValue())+"";
    //                }
    //                // Identify the location index to set in data row
    //                
    //
    //                if((properties || isNameColumnOnlyReq) && (null != member) && (member.getAttributes() != null) && member.getAttributes().length > 0)
    //                {
    //                    int length = 0;
    //                    if(properties)
    //                    {
    //                        length = member.getAttributes().length;
    //                    }
    //                    else if(isNameColumnOnlyReq)
    //                    {
    //                        length = 1;
    //                    }
    //                    // if(direct)
    //                    // {
    //                    // --length;
    //                    // }
    //                    // Fill the member properties in data row
    //                    for(int pIndex = 0;pIndex < length;pIndex++)
    //                    {
    //                        Object attribute = member.getAttributes()[pIndex];
    //                        k[currentDimIndex + pIndex + DIMENSION_DEFAULT] = attribute.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL)?null: attribute;
    //                    }
    //                }
    //
    //            }
    //            MeasureAggregator[] v = new MeasureAggregator[msrCount];
    //            for(int i = 0;i < msrCount;i++)
    //            {
    ////                if(countMsrIndex>-1 && avgIndexes.contains(i))
    ////                {
    ////                    if(result[dimensionCount + countMsrIndex][j]==0)
    ////                    {
    ////                        resultDataA[dimensionCount + attributeCount + i][j]=result[dimensionCount + i][j];
    ////                    }
    ////                    else
    ////                    {
    ////                        resultDataA[dimensionCount + attributeCount + i][j] = result[dimensionCount + i][j]
    ////                                / result[dimensionCount + countMsrIndex][j];
    ////                    }
    ////                }
    ////                else
    ////                {//CHECKSTYLE:OFF    Approval No:Approval-295
    ////                if(!msrList[i].isDistinctQuery())
    ////                {
    ////                    v[i] = ((MeasureAggregator)result[dimensionCount + i][j]).get();
    ////                }
    ////                else
    ////                {
    ////                    v[i] = ((MeasureAggregator)result[dimensionCount + i][j]);
    ////                }
    //                
    //                k[dimensionCount + i]=((MeasureAggregator)result[dimensionCount + i][j]).getValue();
    ////                }//CHECKSTYLE:ON
    //            }
    //            
    ////            for(int i = 0;i < msrCount;i++)
    ////            {
    ////                if(msrList[i].isDistinctQuery())
    ////                {
    ////                    Dimension mappedDim=null;
    ////                    List<Dimension> dimensions = cube.getDimensions(faceTableName);
    ////                    for(Dimension dim:dimensions)
    ////                    {
    ////                        if(dim.getColName().equals(msrList[i].getColName()))
    ////                        {
    ////                            mappedDim= dim;
    ////                            break;
    ////                        }
    ////                    }
    ////                    if(null!=mappedDim)
    ////                    {
    ////                        Iterator<Integer> iterator = ((DistinctCountAggregator)v[i]).getBitMap().iterator();
    ////                        MeasureAggregator distinctCountAggregator = new DistinctCountAggregator(0);
    ////                        int minValue = (int)((DistinctCountAggregator)v[i]).getMinValue();
    ////                        while(iterator.hasNext())
    ////                        {
    ////                            distinctCountAggregator.agg(getGlobalSurrogates(slices, mappedDim, iterator.next()+minValue), null, 0, 0);
    ////                        }
    ////                        v[i]=distinctCountAggregator.get();
    ////                    }
    ////                    else
    ////                    {
    ////                        v[i]=v[i].get();
    ////                    }
    ////                }
    ////            }
    ////            for(int i = 0;i < msrCount;i++)
    ////            {
    ////                k[dimensionCount + i]=v[i].getValue();
    ////            }
    //            keys.add(new MolapKey(k));
    //            values.add(new MolapValue(new MeasureAggregator[0]));
    //            if(j%100 == 0)
    //            {
    //                isInterrupted();
    //            }
    //            
    //        }
    //        
    //        int startIndex = dimensionCount + attributeCount;
    //        for(int i = startIndex;i < resultDataA.length;i++)
    //        {
    //            if(measureWithCustomAgg[i - startIndex] == DIMENSION_DEFAULT)
    //            {
    //                for(int j = 0;j < resultDataA[0].length;j++)
    //                {
    //                    resultDataA[i][j] = idVsValueMap.get(resultDataA[i][j]);
    //                }
    //            }
    //              
    //        }
    //        isInterrupted();
    ////        if(!isHeir)
    ////        {
    ////            int index = 0;
    ////            double uValue = 0;
    ////            double currentValue;
    ////            msrIndex=dimensionCount+attributeCount;
    ////            for(int i = msrIndex;i < resultDataA.length;i++)
    ////            {
    ////                Measure m = msrList[index++];
    ////
    ////                if(MolapCommonConstants.COUNT.equals(m.getAggName()) || MolapCommonConstants.DISTINCT_COUNT.equals(m.getAggName()))
    ////                {
    ////                    for(int j = 0;j < resultDataA[i].length;j++)
    ////                    {
    ////                        currentValue = (Double)resultDataA[i][j];
    ////                        double calValue = currentValue*multiplier;
    ////                        if(calValue < Long.MAX_VALUE)
    ////                        {
    ////                            resultDataA[i][j] = Math.round(calValue)/multiplier;
    ////                        }
    ////                    }
    ////                }
    ////                else if(!MolapCommonConstants.CUSTOM.equals(m.getAggName()))
    ////                {
    ////                    uValue = this.uniqueValue[m.getOrdinal()];
    ////                    for(int j = 0;j < resultDataA[i].length;j++)
    ////                    {
    ////                        currentValue = (Double)resultDataA[i][j];
    ////                        if(0 == Double.compare(currentValue, uValue))
    ////                        {
    ////                            resultDataA[i][j] = null;
    ////                        }
    ////                        else
    ////                        {
    ////                            double calValue = currentValue*multiplier;
    ////                            
    ////                            if(calValue < Long.MAX_VALUE)
    ////                            {
    ////                                resultDataA[i][j] = Math.round(calValue)/multiplier;
    ////                            }
    ////                        }
    ////                    }
    ////                }
    ////            }
    ////        }
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                "###########################################------ Total Number of records" + resultDataA[0].length);
    //        hIterator.setObject(resultDataA);
    //        hIterator.setKeys(keys);
    //        hIterator.setValues(values);
    //    }
    //    
    //    private Object[][] getUpdateResultData(List<Measure> msrs, Measure[] msrList, int startIndex,Object[][] resultDataA)
    //    {
    //        List<Measure> queryMeasure = Arrays.asList(msrList);
    //        int []measureIndexToRead = new int[msrs.size()];
    //        for(int i = 0;i < measureIndexToRead.length;i++)
    //        {
    //            int indexOf = queryMeasure.indexOf(msrs.get(i));
    //            if(indexOf!=-1)
    //            {
    //                measureIndexToRead[i]=startIndex+indexOf;
    //            }
    //        }
    //        Object[][] finalResult = new Object[startIndex+measureIndexToRead.length][];
    //        for(int i = 0;i < startIndex;i++)
    //        {
    //            finalResult[i]=resultDataA[i];
    //        }
    //        int index =startIndex;
    //        for(int i = 0;i < measureIndexToRead.length;i++)
    //        {
    //            finalResult[index++]=resultDataA[measureIndexToRead[i]];
    //        }
    //        return finalResult;
    //    }
    //
    //    private void getResult(int attributeCount, int dimensionCount, Measure[] msrList, Object[][] resultDataA,
    //            double multiplier)
    //    {
    //        int msrIndex;
    //        int index = 0;
    //        double uValue = 0;
    //        double currentValue;
    //        msrIndex=dimensionCount+attributeCount;
    //        //CHECKSTYLE:OFF    Approval No:V3R8C00_014
    //        for(int i = msrIndex;i < resultDataA.length;i++)
    //        {
    //            //CHECKSTYLE:ON
    //            Measure m = msrList[index];
    //          //CHECKSTYLE:OFF    Approval No:V3R8C00_014
    //            index++;
    //          //CHECKSTYLE:ON
    //
    //            if(MolapCommonConstants.COUNT.equals(m.getAggName()) || MolapCommonConstants.DISTINCT_COUNT.equals(m.getAggName()))
    //            {
    //                for(int j = 0;j < resultDataA[i].length;j++)
    //                {
    //                    currentValue = (Double)resultDataA[i][j];
    //                    double calValue = currentValue*multiplier;
    //                    if(calValue < Long.MAX_VALUE)
    //                    {
    //                        resultDataA[i][j] = Math.round(calValue)/multiplier;
    //                    }
    //                }
    //            }
    //            else if(!MolapCommonConstants.CUSTOM.equals(m.getAggName()))
    //            {
    //                // Check if the unique value length is >= to ordinal of the 
    //                // measure.
    //                if(m.getOrdinal() >= this.uniqueValue.length)
    //                {
    //                    continue;
    //                }
    //                uValue = this.uniqueValue[m.getOrdinal()];
    //                for(int j = 0;j < resultDataA[i].length;j++)
    //                {
    //                    currentValue = (Double)resultDataA[i][j];
    //                    if(0 == Double.compare(currentValue, uValue))
    //                    {
    //                        resultDataA[i][j] = null;
    //                    }
    //                    else
    //                    {
    //                        double calValue = currentValue*multiplier;
    //                        
    //                        if(calValue < Long.MAX_VALUE)
    //                        {
    //                            resultDataA[i][j] = Math.round(calValue)/multiplier;
    //                        }
    //                    }
    //                }
    //            }
    //        }
    //    }
    //
    //    /**
    //     * This method will be used to get the updated result if level contains name column then in level it will add name column value if not then level value
    //     * @param dims
    //     * @param resultDataA
    //     * @param dimIndexMap
    //     * @return
    //     */
    //    private Object[][] getUpdateResultData(Dimension[] dims, Object[][] resultDataA,List<Integer> dimIndexMap)
    //    {
    //        List<Integer> dataIndexArrayTopSelect = new ArrayList<Integer>(MolapCommonConstants.DEFAULT_COLLECTION_SIZE);
    //        for(int j = 0;j < dims.length;j++)
    //        {
    //            if(dims[j].isHasNameColumn())
    //            {
    //                dataIndexArrayTopSelect.add(dimIndexMap.get(j)+1);
    //            }
    //            else
    //            {
    //                dataIndexArrayTopSelect.add(dimIndexMap.get(j));
    //            }
    //        }
    //
    //        Object[][] resultArrayNew = new Object[dataIndexArrayTopSelect.size()][];
    //        for(int j = 0;j < resultArrayNew.length;j++)
    //        {
    //            resultArrayNew[j] = resultDataA[dataIndexArrayTopSelect.get(j)];
    //        }
    //        return resultArrayNew;
    //    }
    //    
    //    /**
    //     * 
    //     * @param dims
    //     * @param properties
    //     * @param isNameColumnOnlyReq
    //     * @param member
    //     * @param resultDataA
    //     * @param j
    //     * @param i
    //     * @param currentDimIndex
    //     */
    //    private void addPropertiesInResult(Dimension[] dims, boolean properties, boolean isNameColumnOnlyReq,
    //            Member member, Object[][] resultDataA, int j, int i, int currentDimIndex)
    //    {
    //        int length = 0;
    //        int startIndex = 0;
    //        if(properties)
    //        {
    //            int[] propertyIndexes = dims[i].getPropertyIndexes();
    //            // If level contains both name comulmn as well as properties, then we have to check for
    //            // both and then assign the startindex and length.
    //            if(dims[i].getNameColumnIndex() != -1 && null != propertyIndexes && propertyIndexes.length > 0)
    //            {
    //                startIndex = dims[i].getNameColumnIndex();
    //                length = startIndex + 1+ propertyIndexes.length;
    //            }
    //            else if(null != propertyIndexes && propertyIndexes.length > 0)
    //            {
    //                startIndex = propertyIndexes[0];
    //                length = startIndex + propertyIndexes.length;
    //            }
    //            else 
    //            {
    ////                            int props = member.getAttributes().length;
    //                if(dims[i].getNameColumnIndex() != -1)
    //                {
    //                    startIndex = dims[i].getNameColumnIndex();
    //                    length = startIndex + 1;
    //                }
    //            }
    ////                        length = member.getAttributes().length;
    //        }
    //        else if(isNameColumnOnlyReq && (-1 != dims[i].getNameColumnIndex()))
    //        {
    //            startIndex = dims[i].getNameColumnIndex();
    //            length = dims[i].getNameColumnIndex() + 1;
    //        }
    //        // if(direct)
    //        // {
    //        // --length;
    //        // }
    //        // Fill the member properties in data row
    //        int index = 0;
    //        for(int pIndex = startIndex;pIndex < length;pIndex++)
    //        {
    //            Object attribute = member.getAttributes()[pIndex];
    //            resultDataA[currentDimIndex + index + DIMENSION_DEFAULT][j] = attribute.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL)?null: attribute;
    //            index++;
    //        }
    //    }
    //
    //    private double[][] sortMembers(double[][] data, Dimension[] queryDimensions)
    //    {
    //        long currentTimeMillis = System.currentTimeMillis();
    //        int length = queryDimensions.length;
    //        List<ResultComparartor> compratorList = new ArrayList<ResultComparartor>();
    //        ResultComparartor comparator = null;
    //        for(byte i = 0;i < length;i++)
    //        {
    //            comparator = new ResultComparartor((byte)i, this.queryDimAndSortOrder[i]);
    //            compratorList.add(comparator);
    //        }
    //        isInterrupted();
    //        Arrays.sort(data, new ComparatorChain(compratorList));
    //       // double[][] encodeToRows = encodeToRows(data);
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                "Time take to sort the data: " + (System.currentTimeMillis() - currentTimeMillis));
    //        return data;
    //    }
    //    
    //
    //    
    //
    //    public int compare(double[] left, double[] right, int length)
    //    {
    //        for(int i = 0;i < length;i++)
    //        {
    //            if(left[i] > right[i])
    //            {
    //                return 1;
    //            }
    //            else if(left[i] < right[i])
    //            {
    //                return -1;
    //            }
    //        }
    //        return 0;
    //    }
    //
    //    private double[][] encodeToRows(double[][] data)
    //    {
    //        if(data.length == 0)
    //        {
    //            return data;
    //        }
    //        double[][] rData = new double[data[0].length][data.length];
    //        int len = data.length;
    //        for(int i = 0;i < rData.length;i++)
    //        {
    //            for(int j = 0;j < len;j++)
    //            {//CHECKSTYLE:OFF    Approval No:Approval-297
    //                rData[i][j] = data[j][i];
    //            }//CHECKSTYLE:ON
    //        }
    //        return rData;
    //    }
    //    
    //    
    //    private Object[][] encodeToRows(Object[][] data)
    //    {
    //        if(data.length == 0)
    //        {
    //            return data;
    //        }
    //        Object[][] rData = new Object[data[0].length][data.length];
    //        int len = data.length;
    //        for(int i = 0;i < rData.length;i++)
    //        {
    //            for(int j = 0;j < len;j++)
    //            {//CHECKSTYLE:OFF    Approval No:Approval-297
    //                rData[i][j] = data[j][i];
    //            }//CHECKSTYLE:ON
    //        }
    //        return rData;
    //    }
    //
    //
    //
    //    private Member getMemberBySortIndex(List<InMemoryCube> slices, Dimension columnName, int index)
    //    {
    //        for(InMemoryCube slice : slices)
    //        {
    //            Member member = slice.getMemberCache(columnName.getTableName()+'_'+columnName.getColName() + '_' + columnName.getDimName() + '_' + columnName.getHierName()).getActualKeyFromSortedIndex(index);
    //            if(member != null)
    //            {
    //                return member;
    //            }
    //        }
    //        return null;
    //    }
    //    
    ////    private int getGlobalSurrogates(List<InMemoryCube> slices, Dimension columnName, int index)
    ////    {
    ////        int globalSurrogates = -1;
    ////        for(InMemoryCube slice : slices)
    ////        {
    ////            globalSurrogates = slice.getMemberCache(columnName.getTableName()+'_'+columnName.getColName() + '_' + columnName.getDimName() + '_' + columnName.getHierName()).getGlobalSurrogateKey(index);
    ////            if(-1!=globalSurrogates)
    ////            {
    ////                return globalSurrogates;
    ////            }
    ////        }
    ////        return -1;
    ////    }
    //    private int getGlobalSurrogates(List<InMemoryCube> slices, Dimension columnName, int index)
    //    {
    //        int globalSurrogates = -1;
    //        for(InMemoryCube slice : slices)
    //        {
    //            globalSurrogates = slice.getMemberCache(columnName.getTableName()+'_'+columnName.getColName() + '_' + columnName.getDimName() + '_' + columnName.getHierName()).getGlobalSurrogateKey(index);
    //            if(-1!=globalSurrogates)
    //            {
    //                return globalSurrogates;
    //            }
    //        }
    //        return -1;
    //    }
    //
    //    private static int getSortIndexById(List<InMemoryCube> slices, Dimension columnName, int id)
    //    {
    //        for(InMemoryCube slice : slices)
    //        {
    //            int index = slice.getMemberCache(columnName.getTableName()+'_'+columnName.getColName() + '_' + columnName.getDimName() + '_' + columnName.getHierName()).getSortedIndex(id);
    //            if(index != -DIMENSION_DEFAULT)
    //            {
    //                return index;
    //            }
    //        }
    //
    //        return -DIMENSION_DEFAULT;
    //    }
    //
    //
    //
    //    /**
    //     * assume here dim as levels.
    //     * 
    //     * @see com.huawei.unibi.molap.engine.executer.MolapExecutor#executeHierarichies(java.lang.String,
    //     *      java.lang.Integer[], java.util.List, java.util.Map,
    //     *      com.huawei.unibi.molap.engine.util.MolapResultHolder)
    //     * 
    //     */
    //    @Override
    //    public void executeHierarichies(String hName, int[] queryDimOrdinals, List<MolapMetadata.Dimension> allDims,
    //            Map<Dimension, MolapFilterInfo> constraints, MolapResultHolder hIterator) throws IOException
    //    {
    //        executeHierarichies(hName, queryDimOrdinals, allDims, constraints, hIterator,true,this.keyGenerator,false);
    //    }
    //    
    //    
    //    /**
    //     * 
    //     * @param hName
    //     * @param queryDimOrdinals
    //     * @param allDims
    //     * @param constraints
    //     * @param hIterator
    //     * @param properties
    //     * @throws IOException
    //     */
    //    public void executeHierarichies(String hName, int[] queryDimOrdinals, List<MolapMetadata.Dimension> allDims,
    //            Map<Dimension, MolapFilterInfo> constraints, MolapResultHolder hIterator,boolean properties,KeyGenerator keyGenerator, boolean isContainMatchFilter) throws IOException
    //    {
    //        long st = System.currentTimeMillis();
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Start executeHierarichies");
    //        try
    //        {
    //            if(keyGenerator == null)
    //            {
    //                keyGenerator = getKeyGenerator(allDims.toArray(new Dimension[allDims.size()]));
    //            }
    //            Dimension[] queryDims = new Dimension[queryDimOrdinals.length];
    //            this.queryDimAndSortOrder = new byte[queryDims.length];
    //            if(allDims.get(0).isNormalized())
    //            {
    //                int dimsSize=allDims.size();
    //                int [] dims = new int[dimsSize];
    //                for(int i = 0;i < dimsSize;i++)
    //                {
    ////                    dims[i]=allDims.get(i).getNoOfbits();
    //                }
    //                
    //                keyGenerator=KeyGeneratorFactory.getKeyGenerator(dims);
    //            }
    //            Map<ByteArrayWrapper, MeasureAggregator[]> data = getHierarichyTuples(hName, queryDimOrdinals, allDims,
    //                    constraints, queryDims, keyGenerator, null, false);
    //
    //            boolean isNameColumnDataRequired = false;
    //            if(isContainMatchFilter)
    //            {
    //                for(int i = 0;i < queryDims.length;i++)
    //                {
    //                    if(queryDims[i].isHasNameColumn())
    //                    {
    //                        isNameColumnDataRequired = true;
    //                        break;
    //                    }
    //                }
    //            }
    //            createDataFromAggregates(slices, hIterator, data, queryDims, properties, 0, true, null, null, -1, false,
    //                    null, keyGenerator, null, isNameColumnDataRequired, null);
    //
    //        }
    //        catch(Exception e)
    //        {
    //           LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
    //        }
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                "End executeHierarichies : " +hName +' ' + (System.currentTimeMillis() - st));
    //    }
    //    
    //    /**
    //     * @param hName
    //     * @param queryDimOrdinals
    //     * @param allDims
    //     * @param constraints
    //     * @param hIterator
    //     * @return
    //     * @throws IOException
    //     */
    //    private Int2ObjectMap<List<int[]>> executeHierarichiesWithSurrogates(String hName, int[] queryDimOrdinals, List<MolapMetadata.Dimension> allDims,
    //            Map<Dimension, MolapFilterInfo> constraints, MolapResultHolder hIterator,KeyGenerator keyGenerator,Cube cube) throws IOException
    //    {
    //        long st = System.currentTimeMillis();
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Getting surrogates for " + hName);
    //   
    //        try
    //        {
    //            
    //            Dimension[] queryDims = new Dimension[queryDimOrdinals.length];
    //            Map<Dimension, MolapFilterInfo> cntrns = QueryExecutorUtil.updateConstraintsAsperDimensions(constraints, allDims);
    //            
    //            Map<ByteArrayWrapper, MeasureAggregator[]> data = getHierarichyTuples(hName, queryDimOrdinals, allDims,
    //                    cntrns, queryDims,keyGenerator,cube,true);
    //            
    //            
    //            return createDataFromAggregatesWithSurrogates(slices, hIterator, data, queryDims, true, 0, true, null,null,-1,false,null, keyGenerator);
    //
    //        }
    //        catch(Exception e)
    //        {
    //           LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
    //        }
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                "End executeHierarichies of : "+hName +' ' + (System.currentTimeMillis() - st));
    //        return null;
    //    }
    //
    //    private Map<ByteArrayWrapper, MeasureAggregator[]> getHierarichyTuples(String hName, int[] queryDimOrdinals,
    //            List<MolapMetadata.Dimension> allDims, Map<Dimension, MolapFilterInfo> constraints, Dimension[] queryDims,
    //            KeyGenerator keyGenerator,Cube cube,boolean filterGroups)
    //            throws Exception
    //    {
    //        Dimension[] dimTablesLocal = allDims.toArray(new Dimension[allDims.size()]);
    //        // List<List> result = new ArrayList<List>();
    //        List<Integer> dimList = QueryExecutorUtil.tolist(queryDimOrdinals);
    //        // List<Integer> dimList = java.util.Arrays.asList(queryDimOrdinals);
    //        List<Integer> sortedQueryLevels = new ArrayList<Integer>();
    //        sortedQueryLevels.addAll(dimList);
    //
    //        Collections.sort(sortedQueryLevels);
    //
    //        int c = 0;
    //        for(Dimension dimension : allDims)
    //        {
    //            for(int i = 0;i < queryDimOrdinals.length;i++)
    //            {
    //                if(dimension.getOrdinal() == queryDimOrdinals[i])
    //                {
    //                    queryDims[c] = dimension;
    //                    c++;
    //                    break;
    //                }
    //            }
    //        }
    //        
    //        // map for query dim id to sorted dim id
    //        // Map<Integer, Integer> dimesionsMap = new HashMap<Integer, Integer>();
    //
    //        // start index of dimensions in final result returned
    //        Map<Integer, Integer> resultDimIndexMap = new HashMap<Integer, Integer>();
    //
    //        int attributeCounter = 0;
    //        for(int i = 0;i < queryDimOrdinals.length;i++)
    //        {
    //            // dimesionsMap.put(i,
    //            // sortedQueryLevels.indexOf(queryDimOrdinals[i]));
    //            resultDimIndexMap.put(i, attributeCounter + i);
    //            attributeCounter += allDims.get(queryDimOrdinals[i]).getTotalAttributeCount();
    //        }
    //
    //        int attributeCount = 0;
    //        // index of the dimensions in the intermediate output query.
    //        // Store the index of each level start in data row
    //        Map<Integer, Integer> sortedDimIndexMap = new HashMap<Integer, Integer>();
    //        for(int j = 0;j < sortedQueryLevels.size();j++)
    //        {
    //            sortedDimIndexMap.put(sortedQueryLevels.get(j), attributeCount + j);
    //            attributeCount += allDims.get(sortedQueryLevels.get(j)).getTotalAttributeCount();
    //        }
    //        // Get data from all the available slices of the cube
    //        Map<ByteArrayWrapper, MeasureAggregator[]> data = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(100);
    //        // Get data from all the available slices of the cube
    //        
    //        /**
    //         * Fortify Fix: FORWARD_NULL
    //         */
    //        if(null != slices)
    //        {
    //            for(InMemoryCube slice : slices)
    //            {
    //                DimensionHierarichyStore dCache = getDimensionCache(slice, hName);
    //    
    //                // Coverity fix add null check
    //                if(null == dCache)
    //                {
    //                    continue;
    //                }
    //                HierarchyStore hCache = dCache.getHier(hName);
    //    
    //                // Map cache = hCache.getCache();
    //                HierarchyBtreeStore hierBTreeStore = hCache.getHierBTreeStore();
    //                if(null == hierBTreeStore)
    //                {
    //                    continue;
    //                }
    //                //CHECKSTYLE:OFF    Approval No:Approval-297
    //                InMemFilterModel filterModel = new InMemFilterModel();
    //                
    //                
    //                
    //                if(filterGroups)
    //                {
    //                    // Compute column include nd exclude predicate key and update the
    //                    // filter model.
    //                    int[][] dimensionGroups = QueryExecutorUtil.getDimensionGroups(cube, constraints);
    //                    
    //                    
    //                    // Set the max key in the memory Filter. (max key for each dimension
    //                    // on initialization)
    //                    filterModel.setMaxKey(new byte[dimTablesLocal.length][]);
    //                    filterModel.setMaxKeyExclude(new byte[dimTablesLocal.length][]);
    //                    
    //                    QueryExecutorUtil.computeColIncludePredicateKeys(constraints, filterModel, dimTablesLocal, keyGenerator,dimensionGroups,slices,cube, null);
    //                    QueryExecutorUtil.computeColExcludePredicateKeys(constraints, filterModel, dimTablesLocal, keyGenerator,dimensionGroups,slices,cube,null);
    //                }
    //                else
    //                {
    //                    // Set the max key in the memory Filter. (max key for each dimension
    //                    // on initialization)
    //                    filterModel.setMaxKey(new byte[dimTablesLocal.length][]);
    //                    filterModel.setMaxKeyExclude(new byte[dimTablesLocal.length][]);
    //                    // Compute column include nd exclude predicate key and update the
    //                    // filter model.
    //                    QueryExecutorUtil.computeColIncludePredicateKeys(constraints, filterModel, dimTablesLocal, keyGenerator,slices);
    //                    QueryExecutorUtil.computeColExcludePredicateKeys(constraints, filterModel, dimTablesLocal, keyGenerator,slices);
    //                }
    //                
    //                
    //                
    //                
    //    //            filterModel.setMaxKey(new byte[dimTablesLocal.length][]);
    //    //            QueryExecutorUtil.computeColIncludePredicateKeys(constraints, filterModel, dimTablesLocal, keyGenerator,
    //    //                    slices);
    //    //            filterModel.setMaxKeyExclude(new byte[dimTablesLocal.length][]);
    //    //            QueryExecutorUtil.computeColExcludePredicateKeys(constraints, filterModel, dimTablesLocal, keyGenerator,
    //    //                    slices);
    //                filterModel.setMaxSize(keyGenerator.getKeySizeInBytes());
    //                long[] startKey = new long[dimTablesLocal.length];
    //                long[] endKey = new long[dimTablesLocal.length];
    //                setStartAndEndKeys(startKey, endKey, filterModel.getIncludePredicateKeys(),filterModel.getIncludePredicateKeysOr(),
    //                        filterModel.getExcludePredicateKeys(), dimTablesLocal);
    //    //            if(constraints.size() == dimTablesLocal.length && constraintsExistsOnAllDimensions(constraints))
    //    //            {
    //    //                data.putAll(handlePointQueries(new int[0], filterModel, new Measure[0], null, hierBTreeStore,
    //    //                        dimTablesLocal, keyGenerator));
    //    //            }
    //    //            else
    //    //            {
    //                    FileHolder fileHolder = FileFactory.getFileHolder(FileFactory.getFileType());
    //                    Scanner scanner = getScanner(keyGenerator, startKey, endKey, constraints, filterModel, fileHolder);
    //                    hierBTreeStore.getNext(keyGenerator.generateKey(startKey), scanner);
    //                    ByteArrayWrapper dimensionsRowWrapper = new ByteArrayWrapper();
    //                    byte[] maxKeyBased = QueryExecutorUtil.getMaxKeyBasedOnDimensions(queryDims, keyGenerator,
    //                            dimTablesLocal);
    //                    while(!scanner.isDone())
    //                    {
    //                        KeyValue key = scanner.getNext();
    //                        // Set the data into the wrapper
    //                        dimensionsRowWrapper
    //                                .setData(key.getArray(), key.getKeyOffset(), key.getKeyLength(), maxKeyBased, 0);
    //                        // 2) Extract required measures
    //                        MeasureAggregator[] currentMsrRowData = data.get(dimensionsRowWrapper);
    //                        if(currentMsrRowData == null)
    //                        {
    //                            currentMsrRowData = new MeasureAggregator[0];
    //    //                        dimensionsRowWrapper.setActualData(key.getArray(), key.getKeyOffset(), key.getKeyLength());
    //                            data.put(dimensionsRowWrapper, currentMsrRowData);
    //                            dimensionsRowWrapper = new ByteArrayWrapper();
    //                        }
    //                    }
    //    //            }
    //            }
    //        }
    //        return data;
    //    }
    //
    //
    //    /**
    //     * It returns the dimension level members of requested dimension and constarints 
    //     * 
    //     * @see com.huawei.unibi.molap.engine.executer.MolapExecutor#executeDimension(java.lang.String, com.huawei.unibi.molap.metadata.MolapMetadata.Dimension, int[], java.util.Map, com.huawei.unibi.molap.engine.util.MolapResultHolder)
    //     *
    //     */
    //    @Override
    //    public void executeDimension(String hName, MolapMetadata.Dimension dim, int[] dims,
    //            Map<Dimension, MolapFilterInfo> constraints, MolapResultHolder hIterator) throws IOException
    //    {
    //        long st = System.currentTimeMillis();
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Start executeDimension");
    //
    //        List<Iterator<Member>> members = new ArrayList<Iterator<Member>>();
    //
    //        // Get data from all the available slices of the cube
    //        for(InMemoryCube slice : slices)
    //        {
    //            MemberStore memberCache = slice.getMemberCache(dim.getTableName()+'_'+dim.getColName() + "_" + dim.getDimName() + "_" + hName );
    //            // List<String> filterValues = constraints.get(dims[0]);
    //            
    //            SqlStatement.Type dType = dim.getDataType();
    //            MolapFilterInfo contraintInfo = constraints.get(dim);
    //            if(contraintInfo != null
    //                    && (contraintInfo.getExcludedMembers().size() > 0 || contraintInfo.getIncludedMembers().size() > 0))
    //            {
    //                members.add(memberCache.getMembers(contraintInfo,dType));
    //            }
    //            else
    //            {
    //                members.add(memberCache.getAllMembers());
    //            }
    //        }
    //
    //        List<List<Object>> result = new ArrayList<List<Object>>();
    //        result.add(new ArrayList<Object>());
    //
    //        for(Iterator<Member> iter : members)
    //        {
    //            for(;iter.hasNext();)
    //            {
    //                Member row = iter.next();
    //                String memberStr = row.toString(); 
    //                result.get(0).add(memberStr.equals(MolapCommonConstants.MEMBER_DEFAULT_VAL)? MolapCommonConstants.SQLNUllValue : memberStr);
    //                if(row.getAttributes() == null)
    //                {
    //                    continue;
    //                }
    //                int count = DIMENSION_DEFAULT;
    //                for(Object attr : row.getAttributes())
    //                {
    //                    if(result.size() <= count)
    //                    {
    //                        result.add(new ArrayList<Object>());
    //                    }
    //                    result.get(count).add(MolapCommonConstants.MEMBER_DEFAULT_VAL.equals(attr)? MolapCommonConstants.SQLNUllValue : attr);
    //                    count++;
    //                }
    //            }
    //        }
    //
    //        Object[][] resultData = new Object[result.size()][];
    //
    //        for(int i = 0;i < resultData.length;i++)
    //        {
    //            Object[] objArray=new Object[0];
    //            resultData[i] = result.get(i).toArray(objArray);
    //        }
    //
    //        hIterator.setObject(resultData);
    //
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "End executeDimension of : "+hName + (System.currentTimeMillis() - st));
    //
    //    }
    //
    //    /**
    //     * Get the cardinality of dimension level
    //     * 
    //     * @see com.huawei.unibi.molap.engine.executer.MolapExecutor#executeDimensionCount(com.huawei.unibi.molap.metadata.MolapMetadata.Dimension, com.huawei.unibi.molap.engine.util.MolapResultHolder)
    //     *
    //     */
    //    @Override
    //    public void executeDimensionCount(MolapMetadata.Dimension dimension, MolapResultHolder hIterator) throws IOException
    //    {
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Start executeDimensionCount");
    //
    //        MeasureAggregator aggregator = new SumAggregator();
    //
    //        aggregator.agg(findCount(dimension), 0);
    //        hIterator.createData(aggregator);
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "End executeDimensionCount of : "+dimension.getName());
    //    }
    //
    //    /**
    //     * Get the max surrogate of requested dimension level.
    //     */
    //    @Override
    //    protected Long getMaxValue(Dimension dim)
    //    {
    //        long max = 0;
    //        // Get data from all the available slices of the cube
    //        for(InMemoryCube slice : slices)
    //        {
    //            MemberStore memberCache = slice.getMemberCache(dim.getTableName()+'_'+dim.getColName() + "_" +dim.getDimName() + "_" + dim.getHierName());
    //            long sliceMax = memberCache.getMaxValue();
    //            if(max < sliceMax)
    //            {
    //                max = sliceMax;
    //            }
    //        }
    //
    //        return max;
    //    }
    //
    //    /**
    //     * Get the total count from all slices.
    //     * @param dim
    //     * @return
    //     */
    //    private int findCount(Dimension dim)
    //    {
    //        int count = 0;
    //        // Get data from all the available slices of the cube
    //        /**
    //         * Fortify Fix: NULL_RETURNS
    //         */
    //        if(null != dim)
    //        {
    //            for(InMemoryCube slice : slices)
    //            {
    //                MemberStore memberCache = slice.getMemberCache(dim.getTableName()+'_'+dim.getColName() + "_" + dim.getDimName() + "_" + dim.getHierName());
    //                count += memberCache.getCount();
    //            }
    //        }
    //
    //        return count;
    //    }
    //
    //    /**
    //     * Get the table count of requested aggregation table
    //     */
    //    @Override
    //    public void executeAggTableCount(String table, MolapResultHolder hIterator) throws IOException
    //    {
    //        // Get the data size of the table from each slice and make the actual
    //        // count
    //        long counter = executeTableCount(table);
    //        
    //        if(counter == 0)
    //        {
    //            counter =DIMENSION_DEFAULT;
    //        }
    //
    //        MeasureAggregator aggregator = new SumAggregator();
    //        aggregator.agg(counter, 0);
    //        hIterator.createData(aggregator);
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "End executeAggTableCount of : "+table);
    //    }
    //    
    //    /**
    //     * Get the table count of requested fact table.
    //     */
    //    @Override
    //    public long executeTableCount(String table) throws IOException
    //    {
    //        // Get the data size of the table from each slice and make the actual
    //        // count
    //        long counter = 0;
    //        for(InMemoryCube slice : slices)
    //        {
    //            CubeDataStore store = slice.getDataCache(table);
    //            if(store != null)
    //            {
    //                counter += slice.getDataCache(table).getSize();
    //            }
    //        }
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "End executeTableCount of : "+table);
    //
    //        return counter;
    //
    //    }
    //
    //    private DimensionHierarichyStore getDimensionCache(InMemoryCube inMemoryCube, String hierName)
    //    {
    //        MolapDef.CubeDimension[] cubeDimensions = inMemoryCube.getRolapCube().dimensions;
    //        Schema schema = inMemoryCube.getSchema();
    //        for(int i = 0;i < cubeDimensions.length;i++)
    //        {
    //            MolapDef.Hierarchy[] extractHierarchies = MolapSchemaReader.extractHierarchies(schema, cubeDimensions[i]);
    //            for(MolapDef.Hierarchy hier : extractHierarchies)
    //            {
    //                String hName =hier.name == null ? cubeDimensions[i].name : hier.name;
    //                if(hierName.equals(hName))
    //                {
    //                    return inMemoryCube.getDimensionAndHierarchyCache(cubeDimensions[i].name);
    //                }
    //            }
    //
    //        }
    //       
    //        return null;
    //    }
    //    
    //
    //
    //    /**
    //     * returns Sorted surrogate ids for the dimension members for the given
    //     * column <code>colName</code>
    //     * 
    //     * @see com.huawei.unibi.molap.engine.executer.AbstractMolapExecutor#getSurrogates(java.util.List,
    //     *      java.lang.String)
    //     * 
    //     */
    //    @Override
    //    public long[] getSurrogates(List<String> dimMem, Dimension dimName)
    //    {
    //        return QueryExecutorUtil.getSurrogates(dimMem, dimName, slices, true,false);
    //    }
    //
    //    public String getSchemaName()
    //    {
    //        return schemaName;
    //    }
    //
    //    public String getCubeUniqueName()
    //    {
    //        return cubeUniqueName;
    //    }
    //
    //    @Override
    //    public void interruptExecutor()
    //    {
    //      /*  
    //        interrupted = true;
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "The Query with id " + queryId
    //                + " is getting interrupted");
    //        for(ParallelSliceExecutor executor : sliceExecutors)
    //        {
    //            executor.interruptExecutor();
    //        }*/
    //    }
    //    
    //    /**
    //     * This method is called when row limit exceeds.
    //     */
    //    @Override
    //    public void rowLimitExceeded()
    //    {
    //        
    //            
    //       /* if(!paginationRequired)
    //        {
    //            if(!(null!=topNModel && topNModel.getCount()<rowLimit))
    //            {
    //                
    //            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "The Query with id " + queryId
    //                    + " exceeds row Limit : "+rowLimit);
    //            for(ParallelSliceExecutor executor : sliceExecutors)
    //            {
    //                executor.interruptExecutor();
    //            }
    //             // result limit exceeded, throw an exception
    //              throw MondrianResource.instance().MemberFetchLimitExceeded.ex(rowLimit);
    //         
    //            }
    //        }*/
    //    }
    //    
    //    private void isInterrupted()
    //    {
    //       /* if(interrupted)
    //        {
    //            throw new ResourceLimitExceededException("Memory is not enough to generate the report");
    //        }*/
    //    }
    //    
    //    public void setDimTables(List<Dimension> dimList)
    //    {
    //        if(dimList != null)
    //        {
    //            dimTables = dimList.toArray(new Dimension[dimList.size()]);
    //            keyGenerator = getKeyGenerator(dimTables);
    //        }
    //    }

}
