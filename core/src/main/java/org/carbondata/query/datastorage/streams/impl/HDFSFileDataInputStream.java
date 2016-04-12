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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.metadata.LeafNodeInfoColumnar;
import org.carbondata.core.reader.CarbonMetaDataReader;
import org.carbondata.core.util.CarbonMetadataUtil;
import org.carbondata.core.util.ValueCompressionUtil;
import org.carbondata.query.schema.metadata.Pair;
import org.carbondata.query.util.CarbonEngineLogEvent;

public class HDFSFileDataInputStream extends AbstractFileDataInputStream {
    /**
     * Attribute for Carbon LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(FileDataInputStream.class.getName());

    /**
     * HIERARCHY_FILE_EXTENSION
     */
    private static final String HIERARCHY_FILE_EXTENSION = ".hierarchy";

    private String persistenceFileLocation;

    private String tableName;

    private FSDataInputStream fsChannel;

    private ValueCompressionModel valueCompressionModel;

    private long fileSize;

    public HDFSFileDataInputStream(String filesLocation, int mdkeysize, int msrCount,
            boolean hasFactCount, String persistenceFileLocation, String tableName) {
        super(filesLocation, mdkeysize, msrCount);
        this.persistenceFileLocation = persistenceFileLocation;
        this.tableName = tableName;
    }

    @Override
    public void initInput() {
        //
        try {
            LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "Reading from file: " + filesLocation);
            Path pt = new Path(filesLocation);
            FileSystem fs = pt.getFileSystem(new Configuration());
            fsChannel = fs.open(pt);
            // Don't need the following calculation for hierarchy file
            // Hence ignore for hierarchy files
            if (!filesLocation.endsWith(HIERARCHY_FILE_EXTENSION)) {
                FileStatus fileStatus = fs.getFileStatus(pt);
                fileSize = fileStatus.getLen() - CarbonCommonConstants.LONG_SIZE_IN_BYTE;
                offSet = fileHolder.readDouble(filesLocation, fileSize);
                valueCompressionModel = ValueCompressionUtil.getValueCompressionModel(
                        this.persistenceFileLocation
                                + CarbonCommonConstants.MEASURE_METADATA_FILE_NAME + tableName
                                + CarbonCommonConstants.MEASUREMETADATA_FILE_EXT, msrCount);
                this.totalMetaDataLength = (int) (fileSize - offSet);
            }
        } catch (FileNotFoundException fe) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "@@@@ Hirarchy file is missing @@@@ : " + filesLocation);
        } catch (IOException ex) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "@@@@ Error while reading hirarchy @@@@ : " + filesLocation);
        }
    }

    @Override
    public void closeInput() {
        if (fsChannel != null) {
            //
            try {
                fsChannel.close();
            } catch (IOException ex) {
                LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, ex,
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
                new ArrayList<LeafNodeInfoColumnar>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
        CarbonMetaDataReader metaDataReader = new CarbonMetaDataReader(filesLocation, offSet);
        try {
            listOfNodeInfo = CarbonMetadataUtil.convertLeafNodeInfo(metaDataReader.readMetaData());
        } catch (IOException e) {
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
                    "Problem while reading metadata :: " + filesLocation, e);
        }
        for (LeafNodeInfoColumnar infoColumnar : listOfNodeInfo) {
            infoColumnar.setFileName(filesLocation);
        }

        // if fact file empty then list size will 0 then it will throw index out of bound exception
        // if memory is less and cube loading failed that time list will be empty so it will throw
        // out of bound exception
        if (listOfNodeInfo.size() > 0) {
            startKey = listOfNodeInfo.get(0).getStartKey();
        }
        return listOfNodeInfo;
    }

    @Override
    public ValueCompressionModel getValueCompressionMode() {
        return valueCompressionModel;
    }

    @Override
    public Pair getNextHierTuple() {
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
            LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, exception,
                    "Problem While Reading the Hier File : ");
        }
        return null;
    }

    public byte[] getStartKey() {
        return startKey;
    }
}
