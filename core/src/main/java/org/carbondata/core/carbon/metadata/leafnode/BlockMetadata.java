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
package org.carbondata.core.carbon.metadata.leafnode;

import java.io.Serializable;
import java.util.List;

import org.carbondata.core.carbon.metadata.index.LeafNodeIndex;
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;

/**
 * Information of one data file
 */
public class BlockMetadata implements Serializable {

    /**
     * serialization id
     */
    private static final long serialVersionUID = -7284319972734500751L;

    /**
     * version used for data compatibility
     */
    private int versionId;

    /**
     * total number of rows in this file
     */
    private long numberOfRows;

    /**
     * Segment info (will be same/repeated for all block in this segment)
     */
    private SegmentInfo segmentInfo;

    /**
     * Information about leaf nodes of all columns in this file
     */
    private List<LeafNodeInfo> leafNodeList;

    /**
     * Leaf node index of all leaf nodes in this file
     */
    private LeafNodeIndex leafNodeIndex;

    /**
     * Description of columns in this file
     */
    private List<ColumnSchema> columnInTable;

    /**
     * @return the versionId
     */
    public int getVersionId() {
        return versionId;
    }

    /**
     * @param versionId the versionId to set
     */
    public void setVersionId(int versionId) {
        this.versionId = versionId;
    }

    /**
     * @return the numberOfRows
     */
    public long getNumberOfRows() {
        return numberOfRows;
    }

    /**
     * @param numberOfRows the numberOfRows to set
     */
    public void setNumberOfRows(long numberOfRows) {
        this.numberOfRows = numberOfRows;
    }

    /**
     * @return the segmentInfo
     */
    public SegmentInfo getSegmentInfo() {
        return segmentInfo;
    }

    /**
     * @param segmentInfo the segmentInfo to set
     */
    public void setSegmentInfo(SegmentInfo segmentInfo) {
        this.segmentInfo = segmentInfo;
    }

    /**
     * @return the leafNodeList
     */
    public List<LeafNodeInfo> getLeafNodeList() {
        return leafNodeList;
    }

    /**
     * @param leafNodeList the leafNodeList to set
     */
    public void setLeafNodeList(List<LeafNodeInfo> leafNodeList) {
        this.leafNodeList = leafNodeList;
    }

    /**
     * @return the leafNodeIndex
     */
    public LeafNodeIndex getLeafNodeIndex() {
        return leafNodeIndex;
    }

    /**
     * @param leafNodeIndex the leafNodeIndex to set
     */
    public void setLeafNodeIndex(LeafNodeIndex leafNodeIndex) {
        this.leafNodeIndex = leafNodeIndex;
    }

    /**
     * @return the columnInTable
     */
    public List<ColumnSchema> getColumnInTable() {
        return columnInTable;
    }

    /**
     * @param columnInTable the columnInTable to set
     */
    public void setColumnInTable(List<ColumnSchema> columnInTable) {
        this.columnInTable = columnInTable;
    }

}
