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

package org.carbondata.query.schema.metadata;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.query.columnar.aggregator.ColumnarAggregatorInfo;
import org.carbondata.query.columnar.datastoreblockprocessor.DataStoreBlockProcessor;
import org.carbondata.query.datastorage.storeInterfaces.DataStoreBlock;
import org.carbondata.query.executer.impl.RestructureHolder;
import org.carbondata.query.executer.processor.ScannedResultProcessor;

public class ColumnarStorageScannerInfo {
    private DataStoreBlock datablock;

    private long totalNumberOfBlocksToScan;

    private DataStoreBlockProcessor blockProcessor;

    private RestructureHolder restructurHolder;

    private ColumnarAggregatorInfo columnarAggregatorInfo;

    private boolean isAutoAggregateTableRequest;

    private int keySize;

    private int dimColumnCount;

    private int msrColumnCount;

    private FileHolder fileHolder;

    private ScannedResultProcessor scannedResultProcessor;

    private String queryId;

    /**
     * partitionid
     */
    private String partitionId;

    /**
     * @return the datablock
     */
    public DataStoreBlock getDatablock() {
        return datablock;
    }

    /**
     * @param datablock the datablock to set
     */
    public void setDatablock(DataStoreBlock datablock) {
        this.datablock = datablock;
    }

    /**
     * @return the totalNumberOfBlocksToScan
     */
    public long getTotalNumberOfBlocksToScan() {
        return totalNumberOfBlocksToScan;
    }

    /**
     * @param totalNumberOfBlocksToScan the totalNumberOfBlocksToScan to set
     */
    public void setTotalNumberOfBlocksToScan(long totalNumberOfBlocksToScan) {
        this.totalNumberOfBlocksToScan = totalNumberOfBlocksToScan;
    }

    /**
     * @return the blockProcessor
     */
    public DataStoreBlockProcessor getBlockProcessor() {
        return blockProcessor;
    }

    /**
     * @param blockProcessor the blockProcessor to set
     */
    public void setBlockProcessor(DataStoreBlockProcessor blockProcessor) {
        this.blockProcessor = blockProcessor;
    }

    /**
     * @return the restructurHolder
     */
    public RestructureHolder getRestructurHolder() {
        return restructurHolder;
    }

    /**
     * @param restructurHolder the restructurHolder to set
     */
    public void setRestructurHolder(RestructureHolder restructurHolder) {
        this.restructurHolder = restructurHolder;
    }

    /**
     * @return the columnarAggregatorInfo
     */
    public ColumnarAggregatorInfo getColumnarAggregatorInfo() {
        return columnarAggregatorInfo;
    }

    /**
     * @param columnarAggregatorInfo the columnarAggregatorInfo to set
     */
    public void setColumnarAggregatorInfo(ColumnarAggregatorInfo columnarAggregatorInfo) {
        this.columnarAggregatorInfo = columnarAggregatorInfo;
    }

    public boolean isAutoAggregateTableRequest() {
        return isAutoAggregateTableRequest;
    }

    public void setAutoAggregateTableRequest(boolean isAutoAggregateTableRequest) {
        this.isAutoAggregateTableRequest = isAutoAggregateTableRequest;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public int getDimColumnCount() {
        return dimColumnCount;
    }

    public void setDimColumnCount(int dimColumnCount) {
        this.dimColumnCount = dimColumnCount;
    }

    public int getMsrColumnCount() {
        return msrColumnCount;
    }

    public void setMsrColumnCount(int msrColumnCount) {
        this.msrColumnCount = msrColumnCount;
    }

    public FileHolder getFileHolder() {
        return fileHolder;
    }

    public void setFileHolder(FileHolder fileHolder) {
        this.fileHolder = fileHolder;
    }

    public ScannedResultProcessor getScannedResultProcessor() {
        return scannedResultProcessor;
    }

    public void setScannedResultProcessor(ScannedResultProcessor scannedResultProcessor) {
        this.scannedResultProcessor = scannedResultProcessor;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;

    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;

    }
}
