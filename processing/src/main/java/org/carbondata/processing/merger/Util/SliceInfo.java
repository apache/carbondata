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

package org.carbondata.processing.merger.Util;

import java.io.File;
import java.util.List;

import org.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.carbondata.core.metadata.LeafNodeInfo;

public class SliceInfo {
    private File tableFolder;
    /**
     * leafNodeInfoList
     */
    private List<LeafNodeInfo> leafNodeInfoList;
    /**
     * compressionModel
     */
    private ValueCompressionModel compressionModel;
    /**
     * sliceLocation
     */
    private String sliceLocation;
    /**
     * measureCount
     */
    private int measureCount;

    /**
     * getTableFolder
     *
     * @return File
     */
    public File getTableFolder() {
        return tableFolder;
    }

    /**
     * setTableFolder
     *
     * @param tableFolder void
     */
    public void setTableFolder(File tableFolder) {
        this.tableFolder = tableFolder;
    }

    /**
     * Will return list of leaf meta data
     *
     * @return leafNodeInfoList
     * leafNodeInfoList
     */
    public List<LeafNodeInfo> getLeafNodeInfoList() {
        return leafNodeInfoList;
    }

    /**
     * This method will set the leafNodeinfo list
     *
     * @param leafNodeInfoList leafNodeInfoList
     */
    public void setLeafNodeInfoList(List<LeafNodeInfo> leafNodeInfoList) {
        this.leafNodeInfoList = leafNodeInfoList;
    }

    /**
     * This method will be used to get the compression model
     *
     * @return compressionModel
     * compressionModel
     */
    public ValueCompressionModel getCompressionModel() {
        return compressionModel;
    }

    /**
     * This method will be used to set the compression model
     *
     * @param compressionModel compressionModel
     */
    public void setCompressionModel(ValueCompressionModel compressionModel) {
        this.compressionModel = compressionModel;
    }

    /**
     * This method will be used to get the slice location
     *
     * @return slice location
     */
    public String getSliceLocation() {
        return sliceLocation;
    }

    /**
     * This method will be used to set the slice location
     *
     * @param sliceLocation sliceLocation
     */
    public void setSliceLocation(String sliceLocation) {
        this.sliceLocation = sliceLocation;
    }

    public int getMeasureCount() {
        return measureCount;
    }

    public void setMeasureCount(int measureCount) {
        this.measureCount = measureCount;
    }
}
