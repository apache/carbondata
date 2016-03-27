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

package org.carbondata.query.executer.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.executer.exception.QueryExecutionException;
import org.carbondata.query.executer.pagination.impl.DataFileWriter;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.merger.MergerExecutor;
import org.carbondata.query.processor.DataProcessorExt;
import org.carbondata.query.processor.FileBasedLimitProcessor;
import org.carbondata.query.processor.MemoryBasedLimitProcessor;
import org.carbondata.query.processor.exception.DataProcessorException;
import org.carbondata.query.processor.row.AggreagtedRowProcessor;
import org.carbondata.query.processor.row.RowProcessor;
import org.carbondata.query.processor.writer.BlockWriterProcessor;
import org.carbondata.query.result.Result;
import org.carbondata.query.result.impl.ListBasedResult;
import org.carbondata.query.result.impl.MapBasedResult;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.schema.metadata.SliceExecutionInfo;
import org.carbondata.query.util.CarbonEngineLogEvent;
import org.carbondata.query.util.ScannedResultProcessorUtil;
import org.carbondata.query.writer.WriterExecutor;
import org.carbondata.query.writer.exception.ResultWriterException;

//import org.carbondata.core.engine.processor.writer.RowWriterProcessor;

/**
 * Project Name  : Carbon
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : ScannedResultProcessorImpl.java
 * Description   :
 * Class Version  : 1.0
 */
public class ScannedResultProcessorImpl implements ScannedResultProcessor {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(ScannedResultProcessorImpl.class.getName());
    private static long internalMergeLimit = Long.parseLong(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.PAGINATED_INTERNAL_MERGE_SIZE_LIMIT,
                    CarbonCommonConstants.PAGINATED_INTERNAL_MERGE_SIZE_LIMIT_DEFAULT))
            * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
            * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
    private final Map<String, String> processedFileMap;
    private Result mergedScannedResult;
    private SliceExecutionInfo info;
    private boolean isFileBased;
    private WriterExecutor writerExecutor;
    private MergerExecutor mergerExecutor;
    private String outLocation;
    private String interMediateLocation;
    private DataProcessorInfo dataProcessorInfo;
    private ExecutorService execService;
    private List<Result> scannedResultList;
    private long recordCounter;

    public ScannedResultProcessorImpl(SliceExecutionInfo info) {
        this.info = info;
        writerExecutor = new WriterExecutor();

        this.outLocation =
                info.getOutLocation() + '/' + CarbonCommonConstants.SPILL_OVER_DISK_PATH + info
                        .getSchemaName() + '/' + info.getCubeName() + '/' + System.nanoTime();
        this.interMediateLocation = outLocation + '/' + info.getQueryId();

        this.mergerExecutor = new MergerExecutor();
        this.processedFileMap =
                new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        dataProcessorInfo = ScannedResultProcessorUtil.getDataProcessorInfo(info,
                ScannedResultProcessorUtil
                        .getResultTempFileReaderComprator(info.getMaskedByteRangeForSorting(),
                                info.getDimensionSortOrder(), info.getDimensionMaskKeys()),
                ScannedResultProcessorUtil
                        .getMergerChainComparator(info.getMaskedByteRangeForSorting(),
                                info.getDimensionSortOrder(), info.getDimensionMaskKeys()));

        initialiseResult();
        execService = Executors.newFixedThreadPool(1);
        scannedResultList = new ArrayList<Result>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    }

    /**
     * for initializing the map based or list based result.
     */
    private void initialiseResult() {
        if (!info.isDetailQuery()) {
            //            mergedScannedResult = new TrieBasedResult(info.getColumnarSplitter().getKeySizeByBlock(
            //                    info.getQueryDimOrdinal()));
            mergedScannedResult = new MapBasedResult();
        } else {
            mergedScannedResult = new ListBasedResult();
        }
    }

    @Override
    public void addScannedResult(Result scannedResult) throws QueryExecutionException {
        synchronized (processedFileMap) {
            scannedResultList.add(scannedResult);
            recordCounter += scannedResult.size();
            if ((scannedResultList.size() > 3) || (this.info.isDetailQuery()
                    && recordCounter >= this.info.getNumberOfRecordsInMemory())) {
                List<Result> localResult = scannedResultList;
                scannedResultList =
                        new ArrayList<Result>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
                execService.submit(new MergerThread(localResult));
            }
        }
    }

    private void mergeScannedResultsAndWriteToFile(List<Result> scannedResult)
            throws QueryExecutionException {
        long start = System.currentTimeMillis();
        LOGGER.debug(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG, "Started a slice result merging");

        for (int i = 0; i < scannedResult.size(); i++) {
            mergedScannedResult.merge(scannedResult.get(i));
        }
        if (info.isDetailQuery() && info.isFileBasedQuery() && (isFileBased
                || info.getNumberOfRecordsInMemory() < mergedScannedResult.size())) {
            if (!isFileBased) {
                createSpillOverDirectory();
            }
            //            mergerIntermediateFiles(interMediateLocation,new String[]{MolapCommonConstants.QUERY_OUT_FILE_EXT});
            writerExecutor.writeResult(mergedScannedResult, ScannedResultProcessorUtil
                            .getMergerChainComparator(info.getMaskedByteRangeForSorting(),
                                    info.getDimensionSortOrder(), info.getDimensionMaskKeys()),
                    dataProcessorInfo, interMediateLocation);
            initialiseResult();
            isFileBased = true;
        }

        LOGGER.debug(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Finished current slice result merging in time (MS) " + (System.currentTimeMillis()
                        - start));
    }

    private void createSpillOverDirectory() throws QueryExecutionException {
        try {
            if (!FileFactory.isFileExist(interMediateLocation,
                    FileFactory.getFileType(interMediateLocation))) {
                if (!FileFactory.mkdirs(interMediateLocation,
                        FileFactory.getFileType(interMediateLocation))) {
                    throw new QueryExecutionException(
                            "Problem while creating Spill Over Directory");
                }
            }
            LOGGER.info(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "********************************File based query execution");
        } catch (IOException e) {
            throw new QueryExecutionException(e);
        }
    }

    @Override
    public CarbonIterator<QueryResult> getQueryResultIterator() throws QueryExecutionException {
        execService.shutdown();
        try {
            execService.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e1) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Problem in thread termination" + e1.getMessage());
        }

        if (scannedResultList.size() > 0) {
            mergeScannedResultsAndWriteToFile(scannedResultList);
            scannedResultList = null;
        }

        LOGGER.debug(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Finished result merging from all slices");

        DataProcessorExt processor = getProcessor();
        if (!isFileBased) {
            DataFileWriter.KeyValueHolder[] sortedResult = ScannedResultProcessorUtil
                    .getSortedResult(dataProcessorInfo, mergedScannedResult,
                            ScannedResultProcessorUtil
                                    .getMergerChainComparator(info.getMaskedByteRangeForSorting(),
                                            info.getDimensionSortOrder(),
                                            info.getDimensionMaskKeys()));
            try {
                processor.initialise(dataProcessorInfo);
                for (int i = 0; i < sortedResult.length; i++) {
                    if (sortedResult[i].key.getCompleteComplexTypeData() == null) {
                        processor.processRow(sortedResult[i].key.getMaskedKey(),
                                sortedResult[i].value);
                    } else {
                        processor.processRow(sortedResult[i].key, sortedResult[i].value);
                    }
                }
            } catch (DataProcessorException e) {
                throw new QueryExecutionException(e);
            }
        } else {
            if (mergedScannedResult.size() > 0) {
                writerExecutor.writeResult(mergedScannedResult, ScannedResultProcessorUtil
                                .getMergerChainComparator(info.getMaskedByteRangeForSorting(),
                                        info.getDimensionSortOrder(), info.getDimensionMaskKeys()),
                        dataProcessorInfo, interMediateLocation);
            }
            closeExecuters();

            try {
                mergerExecutor.mergeFinalResult(processor, dataProcessorInfo,
                        ScannedResultProcessorUtil.getFiles(interMediateLocation,
                                new String[] { CarbonCommonConstants.QUERY_OUT_FILE_EXT,
                                        CarbonCommonConstants.QUERY_MERGED_FILE_EXT }));
            } catch (Exception e) {
                throw new QueryExecutionException(e);
            }
        }
        return processor.getQueryResultIterator();
    }

    /**
     * check whether its file based or memory based processing
     * and return appropriate DataProcessor
     *
     * @return DataProcessor
     */
    private DataProcessorExt getProcessor() {
        if (!isFileBased) {
            return new MemoryBasedLimitProcessor();
        } else if (info.getLimit() != -1 && info.getLimit() < info.getNumberOfRecordsInMemory()) {
            if (info.isDetailQuery()) {
                return new RowProcessor(new MemoryBasedLimitProcessor());
            } else {
                return new AggreagtedRowProcessor(new MemoryBasedLimitProcessor());
            }
        } else {
            if (info.isDetailQuery()) {
                return new RowProcessor(
                        new FileBasedLimitProcessor(new BlockWriterProcessor(outLocation)));
            } else {
                return new AggreagtedRowProcessor(
                        new FileBasedLimitProcessor(new BlockWriterProcessor(outLocation)));
            }
        }

    }

    /**
     * For closing of the writerExecutor , mergerExecutor threads
     */
    private void closeExecuters() {
        try {
            writerExecutor.closeWriter();
        } catch (ResultWriterException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Problem while closing stream" + e.getMessage());
        }
        try {
            mergerExecutor.closeMerger();
        } catch (ResultWriterException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Problem while closing stream" + e.getMessage());
        }
    }

    private final class MergerThread implements Callable<Void> {
        private List<Result> scannedResult;

        private MergerThread(List<Result> scannedResult) {
            this.scannedResult = scannedResult;
        }

        @Override
        public Void call() throws Exception {
            mergeScannedResultsAndWriteToFile(scannedResult);
            return null;
        }

    }

}
