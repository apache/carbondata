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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.metadata.LeafNodeInfo;
import org.carbondata.query.datastorage.streams.DataInputStream;

/**
 * @author m00258959
 */
public abstract class AbstractFileDataInputStream implements DataInputStream {

    /**
     *
     */
    protected FileHolder fileHolder;

    /**
     *
     */
    protected String filesLocation;

    /**
     *
     */
    protected int mdkeysize;

    /**
     *
     */
    protected int msrCount;

    /**
     *
     */
    protected long offSet;

    /**
     *
     */
    protected int totalMetaDataLength;

    /**
     * start key
     */
    protected byte[] startKey;

    /**
     * @param filesLocation
     * @param mdkeysize
     * @param msrCount
     */
    public AbstractFileDataInputStream(String filesLocation, int mdkeysize, int msrCount) {
        super();
        this.filesLocation = filesLocation;
        this.mdkeysize = mdkeysize;
        this.msrCount = msrCount;
        //        this.lastKey = null;
        fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(filesLocation));
    }

    /**
     * This method will be used to read leaf meta data format of meta data will be
     * <entrycount><keylength><keyoffset><measure1length><measure1offset>
     *
     * @return will return leaf node info which will have all the meta data
     * related to leaf file
     */
    public List<LeafNodeInfo> getLeafNodeInfo() {
        //
        List<LeafNodeInfo> listOfNodeInfo = new ArrayList<LeafNodeInfo>(20);
        ByteBuffer buffer = ByteBuffer
                .wrap(this.fileHolder.readByteArray(filesLocation, offSet, totalMetaDataLength));
        buffer.rewind();
        while (buffer.hasRemaining()) {
            //
            int[] msrLength = new int[msrCount];
            long[] msrOffset = new long[msrCount];
            LeafNodeInfo info = new LeafNodeInfo();
            byte[] startKey = new byte[this.mdkeysize];
            byte[] endKey = new byte[this.mdkeysize];
            info.setFileName(this.filesLocation);
            info.setNumberOfKeys(buffer.getInt());
            info.setKeyLength(buffer.getInt());
            info.setKeyOffset(buffer.getLong());
            buffer.get(startKey);
            //
            buffer.get(endKey);
            info.setStartKey(startKey);
            for (int i = 0; i < this.msrCount; i++) {
                msrLength[i] = buffer.getInt();
                msrOffset[i] = buffer.getLong();
            }
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

}
