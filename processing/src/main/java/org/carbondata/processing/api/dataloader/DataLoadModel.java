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

package org.carbondata.processing.api.dataloader;

public class DataLoadModel {
    /**
     * Schema Info
     */
    private SchemaInfo schemaInfo;

    /**
     * table table
     */
    private String tableName;

    /**
     * is CSV load
     */
    private boolean isCsvLoad;

    /**
     * Modified Dimension
     */
    private String[] modifiedDimesion;
    /**
     * loadNames separated by HASH_SPC_CHARACTER
     */
    private String loadNames;
    /**
     * modificationOrDeletionTime separated by HASH_SPC_CHARACTER
     */
    private String modificationOrDeletionTime;

    /**
     * @return Returns the schemaInfo.
     */
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }

    /**
     * @param schemaInfo The schemaInfo to set.
     */
    public void setSchemaInfo(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
    }

    /**
     * @return Returns the tableName.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName The tableName to set.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * @return Returns the isCsvLoad.
     */
    public boolean isCsvLoad() {
        return isCsvLoad;
    }

    /**
     * @param isCsvLoad The isCsvLoad to set.
     */
    public void setCsvLoad(boolean isCsvLoad) {
        this.isCsvLoad = isCsvLoad;
    }

    /**
     * @return Returns the modifiedDimesion.
     */
    public String[] getModifiedDimesion() {
        return modifiedDimesion;
    }

    /**
     * @param modifiedDimesion The modifiedDimesion to set.
     */
    public void setModifiedDimesion(String[] modifiedDimesion) {
        this.modifiedDimesion = modifiedDimesion;
    }

    /**
     * return modificationOrDeletionTime separated by HASH_SPC_CHARACTER
     */
    public String getModificationOrDeletionTime() {
        return modificationOrDeletionTime;
    }

    /**
     * set modificationOrDeletionTime separated by HASH_SPC_CHARACTER
     */
    public void setModificationOrDeletionTime(String modificationOrDeletionTime) {
        this.modificationOrDeletionTime = modificationOrDeletionTime;
    }

    /**
     * return loadNames separated by HASH_SPC_CHARACTER
     */
    public String getLoadNames() {
        return loadNames;
    }

    /**
     * set loadNames separated by HASH_SPC_CHARACTER
     */
    public void setLoadNames(String loadNames) {
        this.loadNames = loadNames;
    }
}

