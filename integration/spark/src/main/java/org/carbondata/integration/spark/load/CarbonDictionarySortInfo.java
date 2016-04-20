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
package org.carbondata.integration.spark.load;

import java.util.List;

/**
 * Model to hold the sortIndex and sortIndexInverted data
 */
public class CarbonDictionarySortInfo {
    /**
     * Sort index after members are sorted
     */
    private List<Integer> sortIndex;
    /**
     * inverted sort index to get the member
     */
    private List<Integer> sortIndexInverted;

    /**
     * The constructor to instantiate the CarbonDictionarySortInfo object
     * with sortIndex and sortInverted Index data
     *
     * @param sortIndex
     * @param sortIndexInverted
     */
    public CarbonDictionarySortInfo(List<Integer> sortIndex, List<Integer> sortIndexInverted) {
        this.sortIndex = sortIndex;
        this.sortIndexInverted = sortIndexInverted;
    }

    /**
     * return list of sortIndex
     *
     * @return
     */
    public List<Integer> getSortIndex() {
        return sortIndex;
    }

    /**
     * returns list of sortindexinverted
     *
     * @return
     */
    public List<Integer> getSortIndexInverted() {
        return sortIndexInverted;
    }
}
