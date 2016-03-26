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
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 */
package org.carbondata.query.datastorage.cache;

/**
 * @author m00258959
 */
public class LevelInfo {

    /**
     * is level file loaded in memory
     */
    private boolean loaded;

    /**
     * size of a level file
     */
    private long fileSize;

    /**
     * level name
     */
    private String name;

    /**
     * level actual name
     */
    private String column;

    /**
     * table for which the level file belongs
     */
    private String tableName;

    /**
     * filePath of a given level file
     */
    private String filePath;

    /**
     * Load folder number
     */
    private String loadName;

    /**
     * variable to mark for cube access
     */
    private int accessCount;

    /**
     * @param fileSize
     * @param name
     * @param column
     * @param tableName
     * @param filePath
     * @param loadName
     */
    public LevelInfo(long fileSize, String name, String column, String tableName, String filePath,
            String loadName) {
        this.fileSize = fileSize;
        this.name = name;
        this.column = column;
        this.tableName = tableName;
        this.filePath = filePath;
        this.loadName = loadName;
    }

    /**
     * @return Returns the loaded.
     */
    public synchronized boolean isLoaded() {
        return loaded;
    }

    /**
     * @param loaded The loaded to set.
     */
    public synchronized void setLoaded(boolean loaded) {
        this.loaded = loaded;
    }

    /**
     * @return Returns the fileSize.
     */
    public long getFileSize() {
        return fileSize;
    }

    /**
     * @return Returns the tableName.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return Returns the filePath.
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * @return Returns the name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Returns the column.
     */
    public String getColumn() {
        return column;
    }

    /**
     * @return Returns the loadName.
     */
    public String getLoadName() {
        return loadName;
    }

    /**
     * @return Returns the accessCount.
     */
    public synchronized int getAccessCount() {
        return accessCount;
    }

    /**
     * @param accessCount The accessCount to set.
     */
    public synchronized void incrementAccessCount() {
        this.accessCount++;
    }

    /**
     * @param accessCount The accessCount to set.
     */
    public synchronized void decrementAccessCount() {
        this.accessCount--;
    }

}
