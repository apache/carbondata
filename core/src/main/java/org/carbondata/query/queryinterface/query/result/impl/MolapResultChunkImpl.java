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

package org.carbondata.query.queryinterface.query.result.impl;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.query.queryinterface.query.metadata.MolapTuple;
import org.carbondata.query.queryinterface.query.result.MolapResultChunk;

/**
 * Implementation class for MolapResultChunk interface.
 */
public class MolapResultChunkImpl implements MolapResultChunk {
    private static final long serialVersionUID = -8170271559294139918L;

    private List<MolapTuple> rowTuples = new ArrayList<MolapTuple>(10);

    private Object[][] data;

    /**
     * See interface comments
     */
    @Override public List<MolapTuple> getRowTuples() {
        return rowTuples;
    }

    /**
     * Set the row tuples to chunk
     *
     * @param rowTuples
     */
    public void setRowTuples(List<MolapTuple> rowTuples) {
        this.rowTuples = rowTuples;
    }

    /**
     * See interface comments
     */
    @Override public Object getCell(int columnIndex, int rowIndex) {
        return data[rowIndex][columnIndex];
    }

    /**
     * Set the cell data to chunk.
     *
     * @param data
     */
    public void setData(Object[][] data) {
        this.data = data;
    }
}
