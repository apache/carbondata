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
package org.carbondata.query.scope;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.carbondata.query.datastorage.InMemoryTable;

/**
 * The class holds the query slices and the loadNameAndModificationTimeStamp details
 */
public class QueryScopeObject implements Serializable {

    private static final long serialVersionUID = -12345678911121334L;
    /**
     * instance of map having segment name as key and modification time as value
     */
    private Map<String, Long> loadNameAndModificationTimeMap;
    /**
     *  Instance of segment cache
     */
    private List<InMemoryTable> querySlices;

    /**
     * Constructor of QueryScopeObject
     * @param loadNameAndModicationTimeMap
     * @param querySlices
     */
    public QueryScopeObject(Map<String, Long> loadNameAndModicationTimeMap,
            List<InMemoryTable> querySlices) {
        this.querySlices = querySlices;
        this.loadNameAndModificationTimeMap = loadNameAndModicationTimeMap;
    }

    /**
     * Returns the segment cache
     */
    public List<InMemoryTable> getQuerySlices() {
        return querySlices;
    }

    /**
     * returns map having segment name as key and modification time as value
     */
    public Map<String, Long> getLoadNameAndModificationTimeMap() {
        return loadNameAndModificationTimeMap;
    }
}
