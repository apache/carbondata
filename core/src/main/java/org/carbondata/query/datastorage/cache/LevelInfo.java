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

package org.carbondata.query.datastorage.cache;

import org.carbondata.core.cache.Cacheable;

public class LevelInfo implements Cacheable {

    /**
     * is level file loaded in memory
     */
    private boolean loaded;

    /**
     * fileSize
     */
    private long fileSize;

    /**
     * variable to mark for column access
     */
    private int accessCount;

    /**
     * file timestamp
     */
    private long fileTimeStamp;

    /**
     * offset till where file is read
     */
    private long offsetTillFileIsRead;

    public synchronized void setFileTimeStamp(long fileTimeStamp) {
        this.fileTimeStamp = fileTimeStamp;
    }

    @Override public synchronized long getFileTimeStamp() {
        return fileTimeStamp;
    }

    @Override public synchronized int getAccessCount() {
        return accessCount;
    }

    public synchronized void incrementAccessCount() {
        accessCount++;
    }

    public synchronized void decrementAccessCount() {
        accessCount--;
    }

    @Override public synchronized long getMemorySize() {
        return fileSize;
    }

    public synchronized void setMemorySize(long size) {
        this.fileSize = size;
    }

    public synchronized boolean isLoaded() {
        return loaded;
    }

    public synchronized void setLoaded(boolean loaded) {
        this.loaded = loaded;
    }

    public long getOffsetTillFileIsRead() {
        return offsetTillFileIsRead;
    }

    public void setOffsetTillFileIsRead(long offsetTillFileIsRead) {
        this.offsetTillFileIsRead = offsetTillFileIsRead;
    }
}
