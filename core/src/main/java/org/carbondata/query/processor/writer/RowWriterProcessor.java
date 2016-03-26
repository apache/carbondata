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

package org.carbondata.query.processor.writer;

import java.io.DataOutputStream;
import java.io.IOException;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.MolapFile;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.iterator.MolapIterator;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.processor.DataProcessor;
import org.carbondata.query.processor.exception.DataProcessorException;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.util.MolapEngineLogEvent;
import org.carbondata.query.wrappers.ByteArrayWrapper;

/**
 * Project Name  : Carbon
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : RowWriterProcessor.java
 * Description   : This class is responsible for writing the data to a specified data output streams.
 * Class Version  : 1.0
 */
public class RowWriterProcessor implements DataProcessor {

    /**
     * LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(RowWriterProcessor.class.getName());

    /**
     * DataOutputStream dataOutput
     */
    protected DataOutputStream dataOutput;

    /**
     * int entryCount
     */
    private int entryCount;

    /**
     * String filePath
     */
    private String filePath;

    /**
     * String destPath
     */
    private String destPath;

    /**
     * String outputLocation
     */
    private String outputLocation;

    /**
     * @param outputLocation
     */
    public RowWriterProcessor(String outputLocation) {
        this.outputLocation = outputLocation;
    }

    @Override
    public void initialise(DataProcessorInfo model) throws DataProcessorException {
        try {
            filePath = outputLocation + '/' + model.getQueryId() + '/' + System.nanoTime() + ".tmp"
                    + MolapCommonConstants.QUERY_MERGED_FILE_EXT;
            destPath = outputLocation + '/' + model.getQueryId() + '/' + System.nanoTime()
                    + MolapCommonConstants.QUERY_MERGED_FILE_EXT;

            dataOutput = FileFactory
                    .getDataOutputStream(filePath, FileFactory.getFileType(filePath), (short) 1);
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw new DataProcessorException(e);
        }
    }

    @Override
    public void processRow(byte[] key, MeasureAggregator[] value) throws DataProcessorException {
        try {
            // write the key to output stream.
            dataOutput.write(key);

            // write MeasureAggregator [ ] to the stream.
            for (int i = 0; i < value.length; i++) {
                value[i].writeData(dataOutput);
            }
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw new DataProcessorException(e);
        }
        entryCount++;
    }

    @Override
    public void processRow(ByteArrayWrapper key, MeasureAggregator[] value)
            throws DataProcessorException {
        processRow(key.getMaskedKey(), value);
    }

    @Override
    public void finish() throws DataProcessorException {
        try {
            // write the total number of entries(rows) written to the stream.
            dataOutput.writeInt(entryCount);
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
            throw new DataProcessorException(e);
        } finally {
            MolapUtil.closeStreams(dataOutput);
            changeFileExtension();
        }
    }

    /**
     * Renaming the file.
     *
     * @throws DataProcessorException
     */
    private void changeFileExtension() throws DataProcessorException {
        MolapFile molapFile = FileFactory.getMolapFile(filePath, FileFactory.getFileType(filePath));
        if (!molapFile.renameTo(destPath)) {
            throw new DataProcessorException("Problem while renaming the file");
        }
    }

    @Override
    public MolapIterator<QueryResult> getQueryResultIterator() {
        // TODO Auto-generated method stub
        return null;
    }

}
