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

package org.carbondata.query.datastorage.streams.impl;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.query.schema.metadata.Pair;
import org.carbondata.query.util.MolapEngineLogEvent;

public class FileDataInputStream extends AbstractFileDataInputStream {

    //    /**
    //     *
    //     */
    //    private String filesLocation;
    //
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
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(FileDataInputStream.class.getName());
    /**
     * HIERARCHY_FILE_EXTENSION
     */
    private static final String HIERARCHY_FILE_EXTENSION = ".hierarchy";
    /**
     *
     */
    protected boolean hasFactCount;
    /**
     *
     */
    private String persistenceFileLocation;
    /**
     *
     */
    private String tableName;

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
    private FileChannel channel;
    /**
     *
     */
    private ValueCompressionModel valueCompressionModel;

    //    /**
    //     *
    //     */
    //    private int totalMetaDataLength;
    /**
     *
     */
    private BufferedInputStream in;
    /**
     *
     */
    private long fileSize;

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
    public FileDataInputStream(String filesLocation, int mdkeysize, int msrCount,
            boolean hasFactCount, String persistenceFileLocation, String tableName) {
        super(filesLocation, mdkeysize, msrCount);
        this.hasFactCount = hasFactCount;
        this.filesLocation = filesLocation;
        this.mdkeysize = mdkeysize;
        this.msrCount = msrCount;
        //        this.lastKey = null;
        this.persistenceFileLocation = persistenceFileLocation;
        this.tableName = tableName;
        fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filesLocation));
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
            FileInputStream fileInputStream = new FileInputStream(filesLocation);
            channel = fileInputStream.getChannel();
            in = new BufferedInputStream(new FileInputStream(filesLocation));
            // Don't need the following calculation for hierarchy file
            // Hence ignore for hierarchy files
            if (!filesLocation.endsWith(HIERARCHY_FILE_EXTENSION)) {
                fileSize = channel.size() - MolapCommonConstants.LONG_SIZE_IN_BYTE;
                offSet = fileHolder.readDouble(filesLocation, fileSize);
                //
                valueCompressionModel = ValueCompressionUtil.getValueCompressionModel(
                        this.persistenceFileLocation
                                + MolapCommonConstants.MEASURE_METADATA_FILE_NAME + tableName
                                + MolapCommonConstants.MEASUREMETADATA_FILE_EXT, msrCount);
                this.totalMetaDataLength = (int) (fileSize - offSet);
            }
        } catch (FileNotFoundException f) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "@@@@ Hirarchy file is missing @@@@ : " + filesLocation);
        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "@@@@ Error while reading hirarchy @@@@ : " + filesLocation);
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
            int[] msrLength = new int[msrCount];
            long[] msrOffset = new long[msrCount];
            LeafNodeInfoColumnar info = new LeafNodeInfoColumnar();
            byte[] startKey = new byte[this.mdkeysize];
            byte[] endKey = new byte[this.mdkeysize];
            info.setFileName(this.filesLocation);
            info.setNumberOfKeys(buffer.getInt());
            int keySplitValue = buffer.getInt();
            int[] keyLengths = new int[keySplitValue];
            boolean[] isAlreadySorted = new boolean[keySplitValue];
            long[] keyOffset = new long[keySplitValue];
            for (int i = 0; i < keySplitValue; i++) {
                keyLengths[i] = buffer.getInt();
                keyOffset[i] = buffer.getLong();
                isAlreadySorted[i] = buffer.get() == (byte) 0 ? true : false;
            }
            info.setKeyOffSets(keyOffset);
            info.setKeyLengths(keyLengths);
            info.setIsSortedKeyColumn(isAlreadySorted);
            //read column min max data
            byte[][] columnMinMaxData = new byte[buffer.getInt()][];
            for (int i = 0; i < columnMinMaxData.length; i++) {
                columnMinMaxData[i] = new byte[buffer.getInt()];
                //TODO read the short value inorder to get the column min max data for direct surrogate
                buffer.get(columnMinMaxData[i]);
            }
            info.setColumnMinMaxData(columnMinMaxData);

            buffer.get(startKey);
            //
            buffer.get(endKey);
            info.setStartKey(startKey);
            for (int i = 0; i < this.msrCount; i++) {
                msrLength[i] = buffer.getInt();
                msrOffset[i] = buffer.getLong();
            }
            int numberOfKeyBlockInfo = buffer.getInt();
            int[] keyIndexBlockLengths = new int[keySplitValue];
            long[] keyIndexBlockOffset = new long[keySplitValue];
            for (int i = 0; i < numberOfKeyBlockInfo; i++) {
                keyIndexBlockLengths[i] = buffer.getInt();
                keyIndexBlockOffset[i] = buffer.getLong();
            }
            int numberofAggKeyBlocks = buffer.getInt();
            int[] dataIndexMapLength = new int[numberofAggKeyBlocks];
            long[] dataIndexMapOffsets = new long[numberofAggKeyBlocks];
            for (int i = 0; i < numberofAggKeyBlocks; i++) {
                dataIndexMapLength[i] = buffer.getInt();
                dataIndexMapOffsets[i] = buffer.getLong();
            }
            info.setDataIndexMapLength(dataIndexMapLength);
            info.setDataIndexMapOffsets(dataIndexMapOffsets);
            info.setKeyBlockIndexLength(keyIndexBlockLengths);
            info.setKeyBlockIndexOffSets(keyIndexBlockOffset);
            info.setMeasureLength(msrLength);
            info.setMeasureOffset(msrOffset);
            listOfNodeInfo.add(info);
        }
        // Fixed DTS:DTS2013092610515
        // if fact file empty then list size will 0 then it will throw index out of bound exception
        // if memory is less and cube loading failed that time list will be empty so it will throw out of bound exception
        if (listOfNodeInfo.size() > 0) {
            startKey = listOfNodeInfo.get(0).getStartKey();
        }
        return listOfNodeInfo;
    }

    //    public List<LeafNodeInfo> getLeafNodeInfo()
    //    {
    //        //
    //        List<LeafNodeInfo> listOfNodeInfo = new ArrayList<LeafNodeInfo>();
    //        ByteBuffer buffer = ByteBuffer.wrap(this.fileHolder.readByteArray(filesLocation, offSet, totalMetaDataLength));
    //        buffer.rewind();
    //        while(buffer.hasRemaining())
    //        {
    //            //
    //            int []msrLength= new int [msrCount];
    //            long []msrOffset = new long[msrCount];
    //            LeafNodeInfo info = new LeafNodeInfo();
    //            byte[] startKey = new byte[this.mdkeysize];
    //            byte[] endKey = new byte[this.mdkeysize];
    //            info.setFileName(this.filesLocation);
    //            info.setNumberOfKeys(buffer.getInt());
    //            info.setKeyLength(buffer.getInt());
    //            info.setKeyOffset(buffer.getLong());
    //            buffer.get(startKey);
    //            //
    //            buffer.get(endKey);
    //            info.setStartKey(startKey);
    //            for(int i = 0;i < this.msrCount;i++)
    //            {
    //                msrLength[i]=buffer.getInt();
    //                msrOffset[i]=buffer.getLong();
    //            }
    //            info.setMeasureLength(msrLength);
    //            info.setMeasureOffset(msrOffset);
    //            listOfNodeInfo.add(info);
    //        }
    //        // Fixed DTS:DTS2013092610515
    //        // if fact file empty then list size will 0 then it will throw index out of bound exception
    //        // if memory is less and cube loading failed that time list will be empty so it will throw out of bound exception
    //        if(listOfNodeInfo.size()>0)
    //        {
    //            startKey = listOfNodeInfo.get(0).getStartKey();
    //        }
    //        return listOfNodeInfo;
    //    }
    //
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.huawei.unibi.molap.engine.datastorage.streams.DataInputStream#closeInput
     * ()
     */
    @Override public void closeInput() {
        if (channel != null) {
            //
            try {
                channel.close();
            } catch (IOException e) {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                        "Could not close input stream for location : " + filesLocation);
            }
        }
        if (null != fileHolder) {
            fileHolder.finish();
        }
        //
        if (null != in) {
            try {
                in.close();
            } catch (IOException e) {
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                        "Could not close input stream for location : " + filesLocation);
            }
        }
    }

    @Override public ValueCompressionModel getValueCompressionMode() {
        return valueCompressionModel;
    }

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
            if (in.read(line, 0, lineLength) != -1) {
                System.arraycopy(line, 0, mdkey, 0, mdkeysize);
                Pair data = new Pair();
                data.setKey(mdkey);
                return data;
            }

        } catch (IOException e) {
            LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG, e,
                    "Problem While Reading the Hier File : ");
        }
        return null;
    }

    public byte[] getStartKey() {
        return startKey;
    }
}
