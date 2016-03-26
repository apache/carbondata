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

/**
 *
 */
package org.carbondata.query.processor.writer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import org.carbondata.core.iterator.MolapIterator;
import org.carbondata.core.writer.MolapDataWriter;
import org.carbondata.core.writer.exception.MolapDataWriterException;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.pagination.impl.QueryResult;
import org.carbondata.query.processor.DataProcessorExt;
import org.carbondata.query.processor.exception.DataProcessorException;
import org.carbondata.query.result.iterator.FileBasedResultIteartor;
import org.carbondata.query.schema.metadata.DataProcessorInfo;
import org.carbondata.query.util.MolapEngineLogEvent;
import org.carbondata.query.wrappers.ByteArrayWrapper;

/**
 * Project Name  : Carbon
 * Module Name   : MOLAP Data Processor
 * Author    : R00903928,k00900841
 * Created Date  : 27-Aug-2015
 * FileName   : BlockWriterProcessor.java
 * Description   : This class is responsible for dividing the data into blocks and
 * writing the blocks.
 * Class Version  : 1.0
 */
public class BlockWriterProcessor implements DataProcessorExt {
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(BlockWriterProcessor.class.getName());

    private DataProcessorInfo model;

    /**
     * dataWriter
     */
    private MolapDataWriter dataWriter;

    /**
     * blockDataArray
     */
    private ByteArrayOutputStream[] blockDataArray;

    /**
     * blockDataArray
     */
    private DataOutputStream[] msrDataOutStreams;

    /**
     * blockKeyArray
     */
    private byte[] blockKeyArray;

    /**
     * entryCount
     */
    private int entryCount;

    /**
     * startKey
     */
    private byte[] startKey;

    /**
     * endKey
     */
    private byte[] endKey;

    /**
     * rowCount
     */
    private long rowCount;

    /**
     * queryId
     */
    private String queryId;

    private String outputLocation;

    /**
     * @param outputLocation
     */
    public BlockWriterProcessor(String outputLocation) {
        this.outputLocation = outputLocation;
    }

    @Override
    public void initialise(DataProcessorInfo model) throws DataProcessorException {
        this.model = model;
        this.queryId = model.getQueryId();
        this.dataWriter =
                new MolapDataWriter(outputLocation, model.getAggType().length, model.getKeySize(),
                        queryId, MolapCommonConstants.QUERY_MERGED_FILE_EXT, false, false);
        try {
            this.dataWriter.initChannel();
        } catch (MolapDataWriterException e) {
            throw new DataProcessorException(e);
        }
        this.blockKeyArray = new byte[model.getBlockSize() * model.getKeySize()];
        createMsrDataOutStrms(model.getAggType().length);
    }

    /**
     * creating the data output streams.
     */
    private void createMsrDataOutStrms(int length) {
        this.blockDataArray = new ByteArrayOutputStream[length];
        msrDataOutStreams = new DataOutputStream[length];
        for (int i = 0; i < length; i++) {
            this.blockDataArray[i] = new ByteArrayOutputStream(
                    model.getBlockSize() * MolapCommonConstants.INT_SIZE_IN_BYTE);
            msrDataOutStreams[i] = new DataOutputStream(this.blockDataArray[i]);
        }
    }

    @Override
    public void processRow(final byte[] key, final MeasureAggregator[] value)
            throws DataProcessorException {
        try {
            if (this.entryCount == this.model.getBlockSize()) {
                try {
                    this.dataWriter
                            .writeDataToFile(SnappyByteCompression.INSTANCE.compress(blockKeyArray),
                                    convertToByteArray(), this.entryCount, this.startKey,
                                    this.endKey);
                    close();
                    createMsrDataOutStrms(model.getAggType().length);
                } catch (MolapDataWriterException e) {
                    throw new DataProcessorException("Problem while writing the data to file: ", e);
                }
                this.entryCount = 0;
            }

            if (this.entryCount == 0) {
                this.startKey = key;
            }
            this.endKey = key;

            System.arraycopy(key, 0, blockKeyArray, model.getKeySize() * entryCount, key.length);

            for (int i = 0; i < value.length; i++) {
                value[i].writeData(msrDataOutStreams[i]);
            }
        } catch (IOException e) {
            throw new DataProcessorException(e);
        }
        entryCount++;
        rowCount++;
    }

    /**
     * This method is for closing the streams.
     */
    private void close() {
        try {
            for (int i = 0; i < blockDataArray.length; i++) {
                blockDataArray[i].close();
                msrDataOutStreams[i].close();
            }
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e);
        }
    }

    /**
     * This method is for doing the snappyByteCompression on data.
     *
     * @return
     */
    private byte[][] convertToByteArray() {
        byte[][] b = new byte[blockDataArray.length][];
        for (int i = 0; i < b.length; i++) {
            b[i] = SnappyByteCompression.INSTANCE.compress(blockDataArray[i].toByteArray());
        }
        return b;
    }

    @Override
    public void finish() throws DataProcessorException {
        try {
            if (this.entryCount > 0) {

                this.dataWriter
                        .writeDataToFile(SnappyByteCompression.INSTANCE.compress(blockKeyArray),
                                convertToByteArray(), entryCount, startKey, endKey);
                this.dataWriter.writeleafMetaDataToFile();
            } else {
                this.dataWriter.writeleafMetaDataToFile();
            }
        } catch (Exception e) {
            throw new DataProcessorException(e);
        } finally {
            close();
            this.dataWriter.closeChannle();
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Total row count for query " + queryId + " : " + rowCount);
        }
    }

    @Override
    public MolapIterator<QueryResult> getQueryResultIterator() {
        return new FileBasedResultIteartor(outputLocation + '/' + queryId + "_0", model);
    }

    @Override
    public void processRow(ByteArrayWrapper key, MeasureAggregator[] value)
            throws DataProcessorException {
        // TODO Auto-generated method stub

    }

}
