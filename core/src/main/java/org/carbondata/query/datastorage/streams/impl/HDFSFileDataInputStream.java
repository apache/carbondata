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
package org.carbondata.query.datastorage.streams.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.util.MolapUtil;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.query.schema.metadata.Pair;
import org.carbondata.query.util.MolapEngineLogEvent;

/**
 * @author R00900208
 */
public class HDFSFileDataInputStream extends AbstractFileDataInputStream {
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(FileDataInputStream.class.getName());

    /**
     * HIERARCHY_FILE_EXTENSION
     */
    private static final String HIERARCHY_FILE_EXTENSION = ".hierarchy";

    //    /**
    //     *
    //     */
    //    private String filesLocation;

    //    /**
    //     *
    //     */
    //    private int mdkeysize;
    //
    //    /**
    //     *
    //     */
    //    private int msrCount;

    /**
     *
     */
    private String persistenceFileLocation;

    /**
     *
     */
    //    protected boolean hasFactCount;

    /**
     *
     */
    private String tableName;

    /**
     *
     */
    private FSDataInputStream fsChannel;

    /**
     *
     */
    private ValueCompressionModel valueCompressionModel;

    //    /**
    //     *
    //     */
    //    private long offSet;

    //    /**
    //     *
    //     */
    //    private FileHolder fileHolder;

    /**
     *
     */
    private long fileSize;

    //    /**
    //     *
    //     */
    //    private int totalMetaDataLength;

    //    /**
    //     * start key
    //     */
    //    private byte[] startKey;

    /**
     * @param filesLocation
     * @param mdkeysize
     * @param msrCount
     * @param aggregateNames
     * @param tableName
     * @param hasFactCount
     */
    public HDFSFileDataInputStream(String filesLocation, int mdkeysize, int msrCount,
            boolean hasFactCount, String persistenceFileLocation, String tableName) {
        super(filesLocation, mdkeysize, msrCount);
        //        this.hasFactCount = hasFact_count;
        //        this.lastKey = null;
        this.persistenceFileLocation = persistenceFileLocation;
        this.tableName = tableName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream#initInput
     * ()
     */
    @Override public void initInput() {
        //
        try {
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Reading from file: " + filesLocation);
            Path pt = new Path(filesLocation);
            FileSystem fs = pt.getFileSystem(new Configuration());
            fsChannel = fs.open(pt);
            // Don't need the following calculation for hierarchy file
            // Hence ignore for hierarchy files
            if (!filesLocation.endsWith(HIERARCHY_FILE_EXTENSION)) {
                FileStatus fileStatus = fs.getFileStatus(pt);
                fileSize = fileStatus.getLen() - MolapCommonConstants.LONG_SIZE_IN_BYTE;
                offSet = fileHolder.readDouble(filesLocation, fileSize);
                //
                valueCompressionModel = ValueCompressionUtil.getValueCompressionModel(
                        this.persistenceFileLocation
                                + MolapCommonConstants.MEASURE_METADATA_FILE_NAME + tableName
                                + MolapCommonConstants.MEASUREMETADATA_FILE_EXT, msrCount);
                this.totalMetaDataLength = (int) (fileSize - offSet);
            }
        } catch (FileNotFoundException fe) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "@@@@ Hirarchy file is missing @@@@ : " + filesLocation);
        } catch (IOException ex) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "@@@@ Error while reading hirarchy @@@@ : " + filesLocation);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream#closeInput
     * ()
     */
    @Override public void closeInput() {
        if (fsChannel != null) {
            //
            try {
                fsChannel.close();
            } catch (IOException ex) {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, ex,
                        "Could not close input stream for location : " + filesLocation);
            }
        }
        if (null != fileHolder) {
            fileHolder.finish();
        }

    }

    /**
     * This method will be used to read leaf meta data format of meta data will be
     * <entrycount><keylength><keyoffset><measure1length><measure1offset>
     *
     * @return will return leaf node info which will have all the meta data
     * related to leaf file
     */
    public List<LeafNodeInfoColumnar> getLeafNodeInfoColumnar() {
        //
        List<LeafNodeInfoColumnar> listOfNodeInfo =
                new ArrayList<LeafNodeInfoColumnar>(MolapCommonConstants.CONSTANT_SIZE_TEN);
        ByteBuffer buffer = ByteBuffer
                .wrap(this.fileHolder.readByteArray(filesLocation, offSet, totalMetaDataLength));
        buffer.rewind();
        while (buffer.hasRemaining()) {
            //
            long[] msrOffset = new long[msrCount];
            int[] msrLength = new int[msrCount];
            LeafNodeInfoColumnar nodeInfo = new LeafNodeInfoColumnar();
            byte[] startKey = new byte[this.mdkeysize];
            byte[] endKey = new byte[this.mdkeysize];
            nodeInfo.setFileName(this.filesLocation);
            nodeInfo.setNumberOfKeys(buffer.getInt());
            int keySplitValue = buffer.getInt();
            MolapUtil.setInfo(buffer, nodeInfo, startKey, endKey, keySplitValue);
            for (int j = 0; j < this.msrCount; j++) {
                msrLength[j] = buffer.getInt();
                msrOffset[j] = buffer.getLong();
            }
            int numberOfKeyBlockInfo = buffer.getInt();
            int[] keyIndexBlockLengths = new int[keySplitValue];
            long[] keyIndexBlockOffset = new long[keySplitValue];
            for (int j = 0; j < numberOfKeyBlockInfo; j++) {
                keyIndexBlockLengths[j] = buffer.getInt();
                keyIndexBlockOffset[j] = buffer.getLong();
            }
            int numberofAggKeyBlocks = buffer.getInt();
            int[] dataIndexMapLength = new int[numberofAggKeyBlocks];
            long[] dataIndexMapOffsets = new long[numberofAggKeyBlocks];
            for (int j = 0; j < numberofAggKeyBlocks; j++) {
                dataIndexMapLength[j] = buffer.getInt();
                dataIndexMapOffsets[j] = buffer.getLong();
            }
            nodeInfo.setDataIndexMapLength(dataIndexMapLength);
            nodeInfo.setDataIndexMapOffsets(dataIndexMapOffsets);
            nodeInfo.setKeyBlockIndexLength(keyIndexBlockLengths);
            nodeInfo.setKeyBlockIndexOffSets(keyIndexBlockOffset);
            nodeInfo.setMeasureLength(msrLength);
            nodeInfo.setMeasureOffset(msrOffset);
            listOfNodeInfo.add(nodeInfo);
        }
        // Fixed DTS:DTS2013092610515
        // if fact file empty then list size will 0 then it will throw index out of bound exception
        // if memory is less and cube loading failed that time list will be empty so it will throw out of bound exception
        if (listOfNodeInfo.size() > 0) {
            startKey = listOfNodeInfo.get(0).getStartKey();
        }
        return listOfNodeInfo;
    }

    @Override public ValueCompressionModel getValueCompressionMode() {
        return valueCompressionModel;
    }

    //TODO SIMIAN
    /*
     * (non-Javadoc)
     * 
     * @see com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream#
     * getNextHierTuple()
     */
    @Override public Pair getNextHierTuple() {
        // We are adding surrogate key also with mdkey.
        int lineLength = mdkeysize + 4;
        byte[] line = new byte[lineLength];
        byte[] mdkey = new byte[mdkeysize];
        try {
            //
            if (fsChannel.read(line, 0, lineLength) != -1) {
                System.arraycopy(line, 0, mdkey, 0, mdkeysize);
                Pair data = new Pair();
                data.setKey(mdkey);
                return data;
            }

        } catch (IOException exception) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, exception,
                    "Problem While Reading the Hier File : ");
        }
        return null;
    }

    public byte[] getStartKey() {
        return startKey;
    }
}
