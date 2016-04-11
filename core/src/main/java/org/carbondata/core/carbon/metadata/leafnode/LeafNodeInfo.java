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

import org.carbondata.core.carbon.metadata.leafnode.datachunk.DataChunk;

/**
 * Leaf node info class to store the information about the Btree leaf
 */
public class LeafNodeInfo implements Serializable {

    /**
     * serialization id
     */
    private static final long serialVersionUID = 1873135459695635381L;

    /**
     * Number of rows in this leaf node
     */
    private int numberOfRows;

    /**
     * Information about dimension chunk of all dimensions in this leaf node
     */
    private List<DataChunk> dimensionColumnChunk;

    /**
     * Information about measure chunk of all measures in this leaf node
     */
    private List<DataChunk> measureColumnChunk;

    /**
     * @return the numberOfRows
     */
    public int getNumberOfRows() {
        return numberOfRows;
    }

    /**
     * @param numberOfRows the numberOfRows to set
     */
    public void setNumberOfRows(int numberOfRows) {
        this.numberOfRows = numberOfRows;
    }

    /**
     * @return the dimensionColumnChunk
     */
    public List<DataChunk> getDimensionColumnChunk() {
        return dimensionColumnChunk;
    }

    /**
     * @param dimensionColumnChunk the dimensionColumnChunk to set
     */
    public void setDimensionColumnChunk(List<DataChunk> dimensionColumnChunk) {
        this.dimensionColumnChunk = dimensionColumnChunk;
    }

    /**
     * @return the measureColumnChunk
     */
    public List<DataChunk> getMeasureColumnChunk() {
        return measureColumnChunk;
    }

    /**
     * @param measureColumnChunk the measureColumnChunk to set
     */
    public void setMeasureColumnChunk(List<DataChunk> measureColumnChunk) {
        this.measureColumnChunk = measureColumnChunk;
    }
}
