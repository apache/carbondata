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

package org.carbondata.core.carbon;

/**
 * dictionary metadata object which will hold segment id,
 * min, max and end offset for a segment
 */
public class CarbonDictionaryMetadata {

    /**
     * segment id
     */
    private int segmentId;

    /**
     * min value for a segment
     */
    private int min;

    /**
     * max value for a segment
     */
    private int max;

    /**
     * dictionary file offset for a segment
     */
    private long offset;

    /**
     * constructor
     */
    public CarbonDictionaryMetadata(int segmentId, int min, int max, long offset) {
        this.segmentId = segmentId;
        this.min = min;
        this.max = max;
        this.offset = offset;
    }

    /**
     * return segment id
     */
    public int getSegmentId() {
        return segmentId;
    }

    /**
     * return minimum value for a segment
     */
    public int getMin() {
        return min;
    }

    /**
     * return maximum value for a segment
     */
    public int getMax() {
        return max;
    }

    /**
     * return end offset for a segment
     */
    public long getOffset() {
        return offset;
    }

}
