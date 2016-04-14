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

package org.carbondata.query.merger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.util.AggUtil;
import org.carbondata.query.merger.exception.ResultMergerException;
import org.carbondata.query.processor.DataProcessor;
import org.carbondata.query.processor.exception.DataProcessorException;
import org.carbondata.query.reader.ResultTempFileReader;
import org.carbondata.query.reader.exception.ResultReaderException;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.util.CarbonEngineLogEvent;

//import org.carbondata.core.engine.executer.calcexp.CarbonCalcFunction;

/**
 * Project Name  : Carbon
 * Module Name   : CARBON Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : UnSortedResultMerger.java
 * Description   : This class is responsible for handling the merge of the
 * result data present in the intermediate temp files in an unsorted
 * order. This will use arraylist to hold all data and process it
 * sequentially.
 * Class Version  : 1.0
 */
public class UnSortedResultMerger implements Callable<Void> {
    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(UnSortedResultMerger.class.getName());

    /**
     * dataProcessorInfo
     */
    private DataProcessorInfo dataProcessorInfo;

    /**
     * List to hold all the temp files.
     */
    private List<ResultTempFileReader> recordHolderList;

    /**
     * dataProcessor This is the data processor object which will process the data.
     */
    private DataProcessor dataProcessor;

    /**
     * files Temp files.
     */
    private CarbonFile[] files;

    /**
     * @param dataProcessor
     * @param dataProcessorInfo
     * @param files
     */
    public UnSortedResultMerger(DataProcessor dataProcessor, DataProcessorInfo dataProcessorInfo,
            CarbonFile[] files) {
        this.dataProcessorInfo = dataProcessorInfo;
        this.recordHolderList = new ArrayList<ResultTempFileReader>(files.length);
        this.dataProcessor = dataProcessor;
        this.files = files;
    }

    /**
     * @see Callable#call()
     */
    @Override
    public Void call() throws Exception {
        try {
            this.dataProcessor.initialise(dataProcessorInfo);

            // For each intermediate result file.
            for (CarbonFile file : files) {
                // reads the temp files and creates ResultTempFileReader object.
                ResultTempFileReader carbonSortTempFileChunkHolder =
                        new ResultTempFileReader(file.getAbsolutePath(),
                                dataProcessorInfo.getKeySize(),
                                AggUtil.getAggregators(dataProcessorInfo.getAggType(), false,
                                        dataProcessorInfo.getKeyGenerator(),
                                        dataProcessorInfo.getCubeUniqueName(),
                                        dataProcessorInfo.getMsrMinValue(),
                                        dataProcessorInfo.getNoDictionaryTypes(),
                                        dataProcessorInfo.getDataTypes()),
                                dataProcessorInfo.getFileBufferSize());
                // initialize
                carbonSortTempFileChunkHolder.initialize();
                // add to list
                this.recordHolderList.add(carbonSortTempFileChunkHolder);
            }
            // iterate through list and for each file process the each row of data.
            writeRecordToFile();
        } catch (ResultReaderException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
            throw new ResultMergerException(e);
        } finally {
            dataProcessor.finish();
            //delete the temp files.
            CarbonUtil.deleteFoldersAndFiles(files);
        }
        return null;
    }

    /**
     * This method will be used to get the record from file
     *
     * @throws ResultMergerException
     */
    private void writeRecordToFile() throws ResultMergerException {
        // for each file.
        for (ResultTempFileReader poll : this.recordHolderList) {      //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_007
            try {
                // for each entry in the file.
                while (poll.hasNext()) {
                    poll.readRow();
                    // process each data to the data processor.
                    dataProcessor.processRow(poll.getKey(), poll.getMeasures());

                    poll.setMeasureAggs(
                            AggUtil.getAggregators(dataProcessorInfo.getAggType(), false,
                                    dataProcessorInfo.getKeyGenerator(),
                                    dataProcessorInfo.getCubeUniqueName(),
                                    dataProcessorInfo.getMsrMinValue(),
                                    dataProcessorInfo.getNoDictionaryTypes(),
                                    dataProcessorInfo.getDataTypes()));
                }
            } catch (DataProcessorException e) {
                throw new ResultMergerException(e);
            } catch (ResultReaderException e) {
                throw new ResultMergerException(e);
            } finally {
                poll.closeStream();
            }
        }//CHECKSTYLE:ON
    }
}
