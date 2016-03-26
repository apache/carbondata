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

package org.carbondata.processing.factreader;

import org.carbondata.core.datastorage.store.MeasureDataWrapper;

public class MolapLeafNodeTuplesHolder {
    /**
     * mdkey
     */
    private byte[] mdkey;

    /**
     * measureDataWrapper
     */
    private MeasureDataWrapper measureDataWrapper;

    /**
     * entryCount
     */
    private int entryCount;

    /**
     * @return the mdkey
     */
    public byte[] getMdKey() {
        return mdkey;
    }

    /**
     * @param mdkey the mdkey to set
     */
    public void setMdKey(byte[] mdkey) {
        this.mdkey = mdkey;
    }

    /**
     * @return the measureDataWrapper
     */
    public MeasureDataWrapper getMeasureDataWrapper() {
        return measureDataWrapper;
    }

    /**
     * @param measureDataWrapper the measureDataWrapper to set
     */
    public void setMeasureDataWrapper(MeasureDataWrapper measureDataWrapper) {
        this.measureDataWrapper = measureDataWrapper;
    }

    /**
     * @return the entryCount
     */
    public int getEntryCount() {
        return entryCount;
    }

    /**
     * @param entryCount the entryCount to set
     */
    public void setEntryCount(int entryCount) {
        this.entryCount = entryCount;
    }
}
