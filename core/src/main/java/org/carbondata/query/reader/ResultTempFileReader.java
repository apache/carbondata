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

package org.carbondata.query.reader;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.reader.exception.ResultReaderException;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class ResultTempFileReader {

    /**
     * temp file
     */
    private String inFilePath;

    /**
     * read stream
     */
    private DataInputStream stream;

    /**
     * entry count
     */
    private int entryCount;

    /**
     * number record read
     */
    private int numberOfRecordRead;

    /**
     * fileBufferSize
     */
    private int fileBufferSize;

    /**
     * keySize
     */
    private int keySize;

    /**
     * MeasureAggregator
     */
    private MeasureAggregator[] measureAggregator;

    /**
     * byteArrayWrapper
     */
    private ByteArrayWrapper byteArrayWrapper;

    /**
     * CarbonSortTempFileChunkHolder Constructor
     *
     * @param inFile     temp file
     * @param recordSize measure count
     * @param keySize    mdkey length
     */
    public ResultTempFileReader(String inFilePath, int keySize,
            final MeasureAggregator[] measureAggregator, int fileBufferSize) {
        // set temp file 
        this.inFilePath = inFilePath;
        // key size
        this.keySize = keySize;
        // measure aggregator
        this.measureAggregator = measureAggregator;
        //fileBufferSize
        this.fileBufferSize = fileBufferSize;
        // byte array wrapper
        this.byteArrayWrapper = new ByteArrayWrapper();
        // out stream
    }

    /**
     * This method will be used to initialize
     *
     * @throws CarbonSortKeyAndGroupByException problem while initializing
     */
    public void initialize() throws ResultReaderException {
        FileHolder fileHolder = null;
        try {
            FileType fileType = FileFactory.getFileType(inFilePath);
            fileHolder = FileFactory.getFileHolder(fileType);
            if (fileType.equals(FileType.HDFS)) {
                this.entryCount = fileHolder.readInt(inFilePath, 0);
            } else {
                fileHolder = FileFactory.getFileHolder(fileType);
                this.entryCount = fileHolder.readInt(inFilePath, 0);
            }
            this.stream = FileFactory.getDataInputStream(inFilePath, fileType, this.fileBufferSize);
            this.stream.readInt();
        } catch (FileNotFoundException e) {
            throw new ResultReaderException(e);
        } catch (IOException e) {
            throw new ResultReaderException(e);
        } finally {
            if (null != fileHolder) {
                fileHolder.finish();
            }
        }
    }

    /**
     * This method will be used to read new row from file
     *
     * @throws CarbonSortKeyAndGroupByException problem while reading
     */
    public void readRow() throws ResultReaderException {
        try {
            byte[] mdKey = new byte[this.keySize];
            this.stream.readFully(mdKey);

            //                LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Problem while reading mdkey from pagination temp file");

            this.byteArrayWrapper.setMaskedKey(mdKey);
            for (int i = 0; i < this.measureAggregator.length; i++) {
                this.measureAggregator[i].readData(this.stream);
            }
            numberOfRecordRead++;
        } catch (FileNotFoundException e) {
            throw new ResultReaderException(this.inFilePath + " No Found ", e);
        } catch (IOException e) {
            throw new ResultReaderException(" Problem while reading" + this.inFilePath, e);
        }
    }

    /**
     * below method will be used to get the row
     *
     * @return row
     */
    public byte[] getRow() {
        return this.byteArrayWrapper.getMaskedKey();
    }

    /**
     * Below method will be used to close stream
     */
    public void closeStream() {
        CarbonUtil.closeStreams(this.stream);
    }

    /**
     * below method will be used to check whether any more records are present in file or not
     *
     * @return more row present in file
     */
    public boolean hasNext() {
        return this.numberOfRecordRead < this.entryCount;
    }

    /**
     * This method will return the data array
     *
     * @param index
     * @return
     */
    public MeasureAggregator[] getMeasures() {
        return this.measureAggregator;
    }

    /**
     * This method will return the key array
     *
     * @return
     */
    public byte[] getKey() {
        return this.byteArrayWrapper.getMaskedKey();
    }

    public void setMeasureAggs(final MeasureAggregator[] aggs) {
        this.measureAggregator = aggs;
    }

}
