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

import java.util.Map;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.ParallelSliceExecutor;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.wrappers.ByteArrayWrapper;

/**
 * Class Description : This class executes the query for slice and return the
 * result map.
 * Version 1.0
 */
public class ParallelSliceExecutorImpl implements ParallelSliceExecutor {

    @Override public Map<ByteArrayWrapper, MeasureAggregator[]> executeSliceInParallel()
            throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override public QueryResult executeSlices() throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override public void interruptExecutor() {
        // TODO Auto-generated method stub

    }
    //    /**
    //     * Slice Execution information.
    //     */
    //    private List<SliceExecutionInfo> infos;
    //
    //    /**
    //     * LOGGER.
    //     */
    //    private static final LogService LOGGER = LogServiceFactory.getLogService(ParallelSliceExecutorImpl.class.getName());
    //
    //    /**
    //     * Executor Service.
    //     */
    //    private ExecutorService execService = Executors.newCachedThreadPool();
    //
    //    /**
    //     * rangeExecutors
    //     */
    //    private List<SliceRangeExecutor> rangeExecutors = new ArrayList<SliceRangeExecutor>();
    //
    //    /**
    //     * Query row counter
    //     */
    //    private QueryRowCounter rowCounter;
    //
    //    /**
    //     * rowLimit
    //     */
    //    private int rowLimit;
    //
    //    /**
    //     *
    //     */
    //    private MolapResultHolder resultHolder;
    //
    //    private boolean interrupted;
    //
    //    private SliceExecutionInfo latestInfo;
    //    /**
    //     *
    //     * @param info
    //     */
    //    public ParallelSliceExecutorImpl(List<SliceExecutionInfo> infos,SliceExecutionInfo latestInfo,QueryRowCounter rowCounter,int rowLimit,MolapResultHolder resultHolder)
    //    {
    //        this.infos = infos;
    //        this.latestInfo = latestInfo;
    //        this.rowCounter = rowCounter;
    //        this.resultHolder = resultHolder;
    //        this.rowLimit = rowLimit;
    //        if(infos.get(0).isPaginationRequired())
    //        {
    //            this.rowLimit = Integer.parseInt(MolapProperties.getInstance().getProperty(
    //                    MolapCommonConstants.PAGINATED_INTERNAL_FILE_ROW_LIMIT,
    //                    MolapCommonConstants.PAGINATED_INTERNAL_FILE_ROW_LIMIT_DEFAULT));
    //        }
    //        else
    //        {
    //            rowLimit = Integer.parseInt(MolapProperties.getInstance().getProperty(MolapCommonConstants.MOLAP_RESULT_SIZE_KEY,MolapCommonConstants.MOLAP_RESULT_SIZE_DEFAULT));
    //        }
    //        long[][][] ranges = infos.get(0).getRanges();
    //        if(ranges != null && ranges.length > 1)
    //        {
    //            this.rowLimit = this.rowLimit / ranges.length;
    //        }
    //
    //    }
    //
    //
    //    /**
    //     * create  {@link SliceRangeExecutor} instance and submit to execute task in parallel.
    //     *
    //     * @see com.huawei.unibi.molap.engine.executer.ParallelSliceExecutor#executeSliceInParallel(long[][][], java.lang.String)
    //     *
    //     */
    //    @Override
    //    public Map<ByteArrayWrapper, MeasureAggregator[]> executeSliceInParallel()
    //            throws KeyGenException
    //    {
    //        SliceRangeExecutor task = null;
    //        long startTime = System.currentTimeMillis();
    //        FileSizeBasedLRU lru = FileSizeBasedLRU.getInstance();
    //        LRUCacheKey holder = new LRUCacheKey();
    //        SliceExecutionInfo sliceExecutionInfo = latestInfo;
    //
    //        sliceExecutionInfo = getSliceExecutionInfo(sliceExecutionInfo);
    //
    //        holder.setQueryId(sliceExecutionInfo.getQueryId());
    ////        if(lru.get(holder) != null)
    ////        {
    ////            if(lru.get(holder).getCacheKey() != null)
    ////            {
    ////                lru.get(holder).getCacheKey().setChildQueries(holder);
    ////            }
    ////            holder.setQueryId(infos.get(0).getQueryId()+System.nanoTime());
    ////            infos.get(0).setQueryId(holder.getQueryId());
    ////        }
    //        holder.setSegmentHeader(sliceExecutionInfo.getSegmentHeader());
    //        holder.setLru(lru);
    //        boolean normalizedCase = sliceExecutionInfo.isNormalizedCase();
    //        holder.setMaskedKeyRanges(normalizedCase?sliceExecutionInfo.getMaskedBytesNormalized():sliceExecutionInfo.getMaskedBytePositions());
    //        holder.setByteCount(normalizedCase?sliceExecutionInfo.getByteCountNormalized():sliceExecutionInfo.getActualMaskedKeyByteSize());
    //        holder.setTableName(sliceExecutionInfo.getTableName());
    //        holder.setGenerator(normalizedCase?sliceExecutionInfo.getKeyGenNormalized():sliceExecutionInfo.getActualKeyGenerator());
    //        holder.setMeasures(sliceExecutionInfo.getQueryMsrs());
    //        GlobalPaginatedAggregator paginatedAggregator = new FileBasedGlobalPaginatedAggregatorImpl(sliceExecutionInfo,holder,rowCounter);
    //        // create callable class having start , end keys and execution info
    //        // object
    //        try
    //        {
    //            for(SliceExecutionInfo info : infos)
    //            {
    //                long[][][] ranges = info.getRanges();
    ////                List<Future<Map<ByteArrayWrapper, MeasureAggregator[]>>> taskFutures = new ArrayList<Future<Map<ByteArrayWrapper, MeasureAggregator[]>>>();
    //                if(ranges != null)
    //                {
    //                    for(int i = 0;i < ranges.length;i++)
    //                    {
    //                        // In the call Method, create a scanner with the set start and
    //                        // end keys
    //                        task = new SliceRangeExecutor(ranges[i][0], ranges[i][1], true, info,paginatedAggregator,rowLimit);
    //                        rangeExecutors.add(task);
    //                        execService.submit(task);
    ////                        taskFutures.add(execService.submit(task));
    //                    }
    //
    ////                    int tasksCompleted = 0;
    ////                    try
    ////                    {
    //
    ////                        //CHECKSTYLE:OFF    Approval No:Approval-249
    ////                    while(tasksCompleted != ranges.length)
    ////                    {//CHECKSTYLE:ON
    ////                            for(Future<Map<ByteArrayWrapper, MeasureAggregator[]>> future : taskFutures)
    ////                            {
    ////                                if(future.isDone())
    ////                                {
    ////                                    tasksCompleted++;
    ////    //                                if(!resultInititalized)
    ////    //                                {
    ////        //
    ////    //                                    result = future.get();
    ////    //
    ////    //                                    resultInititalized = true;
    ////    //                                }
    ////    //                                else
    ////    //                                {
    ////    //                                    InMemoryQueryExecutor.mergeByteArrayMapResult(future.get(), result);
    ////    //                                }
    ////                                    taskFutures.remove(future);
    ////                                    break;
    ////                                }
    ////                                else if(future.isCancelled())
    ////                                {
    ////                                    tasksCompleted++;
    ////                                    taskFutures.remove(future);
    ////                                    break;
    ////                                }
    ////                            }
    ////
    ////                            Thread.sleep(10);
    ////                        }
    //
    ////                    }
    ////                    catch(InterruptedException e)
    ////                    {
    ////                        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
    ////                    }
    //                }
    //                else
    //                {
    //                    task = new SliceRangeExecutor(info.getStartKey(), info.getEndKey(), false, info,paginatedAggregator,rowLimit);
    //                    rangeExecutors.add(task);
    //                    try
    //                    {
    //                        task.call();
    //                    }
    ////                    catch (ResourceLimitExceededException e)
    ////                    {
    ////                        throw e;
    ////                    }
    //                    catch(Exception e)
    //                    {
    //                        LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
    //                    }
    //                }
    //                // execService.shutdown();
    //            }
    //
    //            execService.shutdown();
    //            execService.awaitTermination(2, TimeUnit.DAYS);
    //
    //            paginatedAggregator.mergeDataFile(interrupted);
    //
    //            holder.setCompleted(true);
    //            lru.put(holder,paginatedAggregator.getRowCount());
    //        }
    ////        catch (ResourceLimitExceededException e)
    ////        {
    ////            throw e;
    ////        }
    //        catch (Exception e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e, e.getMessage());
    //        }
    ////        while(!lru.isSizeInLimits())
    ////        {
    ////            lru.remove(holder);
    ////            lru.put(holder);
    ////        }
    ////        else
    ////        {
    ////            // In the call Method, create a scanner with the set start and end
    ////            // keys
    ////            task = new SliceRangeExecutor(info.getStartKey(), info.getEndKey(), false, info,rowCounter,paginatedAggregator);
    ////            rangeExecutors.add(task);
    ////            result = task.call();
    ////        }
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                "Lru Cache Size " + lru.getCurrentSize() +" MB");
    //
    //        LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
    //                "Total time taken for scan " + (System.currentTimeMillis() - startTime));
    //
    //        try
    //        {
    //            int[] rowRange = sliceExecutionInfo.getRowRange();
    //            if(rowRange != null)
    //            {
    //                String outLocation = MolapProperties.getInstance().getProperty(MolapCommonConstants.STORE_LOCATION,
    //                        MolapCommonConstants.STORE_LOCATION_DEFAULT_VAL)
    //                        + File.separator + MolapCommonConstants.PAGINATED_CACHE_FOLDER;
    //
    //                DataFileReader fileReader = new DataFileReader(holder.getByteCount(),outLocation,
    //                        sliceExecutionInfo.getQueryId(), holder.getGenerator(), sliceExecutionInfo.getQueryMsrs(), sliceExecutionInfo.getCalculatedMeasures(),sliceExecutionInfo.getMeasureIndexToRead(),sliceExecutionInfo.getSlice());
    //
    //                TopNModel topNModel = sliceExecutionInfo.getTopNModel();
    //                if(topNModel != null && topNModel.isBreakHierarchy())
    //                {
    //
    //                    if(QueryExecutorUtil.isSameDimensionLevels(sliceExecutionInfo.getSegmentHeader().getDims(), sliceExecutionInfo.getOriginalDims()))
    //                    {
    //
    //                        resultHolder.setTotalRowCount(paginatedAggregator.getRowCount());
    //                        return fileReader.readData(0,paginatedAggregator.getRowCount());
    //
    //                    }
    //                    else if(QueryExecutorUtil.isSubsetDimensionLevels(holder.getSegmentHeader().getDims(), sliceExecutionInfo.getOriginalDims()))
    //                    {
    //                        resultHolder.setTotalRowCount(fileReader.getSizeAfterAggregate(0,paginatedAggregator.getRowCount(), holder.getSegmentHeader(), holder.getGenerator(), sliceExecutionInfo.getOriginalDims()));
    //                        return fileReader.readDataAndAggregate(0,paginatedAggregator.getRowCount(), holder.getSegmentHeader(), holder.getGenerator(), sliceExecutionInfo.getOriginalDims(),false);
    //                    }
    //
    //                }
    //
    //                if(QueryExecutorUtil.isSameDimensionLevels(sliceExecutionInfo.getSegmentHeader().getDims(), sliceExecutionInfo.getOriginalDims()))
    //                {
    //                    resultHolder.setTotalRowCount(paginatedAggregator.getRowCount());
    //                    return fileReader.readData(rowRange[0],rowRange[1]);
    //
    //                }
    //                else if(QueryExecutorUtil.isSubsetDimensionLevels(holder.getSegmentHeader().getDims(), sliceExecutionInfo.getOriginalDims()))
    //                {
    //                    resultHolder.setTotalRowCount(fileReader.getSizeAfterAggregate(0,paginatedAggregator.getRowCount(), holder.getSegmentHeader(), holder.getGenerator(), sliceExecutionInfo.getOriginalDims()));
    //                    return fileReader.readDataAndAggregate(rowRange[0],rowRange[1], holder.getSegmentHeader(), holder.getGenerator(), sliceExecutionInfo.getOriginalDims(),false);
    //                }
    //
    //            }
    //            return paginatedAggregator.getPage(0,0);
    //        }
    //        catch(IOException e)
    //        {
    //            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,e, "Error during reading the page");
    //        }
    //        return new HashMap<ByteArrayWrapper, MeasureAggregator[]>();
    //    }
    //
    //
    //    private SliceExecutionInfo getSliceExecutionInfo(SliceExecutionInfo sliceExecutionInfo)
    //    {
    //        if(sliceExecutionInfo == null)
    //        {
    //            sliceExecutionInfo = infos.get(infos.size()-1);
    //        }
    //        return sliceExecutionInfo;
    //    }
    //
    //
    //    @Override
    //    public void interruptExecutor()
    //    {
    //        interrupted = true;
    //        for(SliceRangeExecutor rangeExecutor : rangeExecutors)
    //        {
    //            rangeExecutor.interrupt();
    //        }
    //
    //    }
    //    @Override
    //    public QueryResult executeSlices() throws Exception
    //    {
    //        // TODO Auto-generated method stub
    //        return null;
    //    }
}
