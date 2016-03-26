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

package org.carbondata.processing.merger;

import java.util.List;

import org.carbondata.core.olap.MolapDef.Schema;

public class MolapSliceMergerInfo {
    private String schemaName;

    private String cubeName;

    private String tableName;

    private String partitionID;

    private Schema schema;

    private String schemaPath;

    private String metadataPath;

    private List<String> loadsToBeMerged;

    private String mergedLoadName;

    /**
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @param schemaName the schemaName to set
     */
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return the cubeName
     */
    public String getCubeName() {
        return cubeName;
    }

    /**
     * @param cubeName the cubeName to set
     */
    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * @return the partitionID
     */
    public String getPartitionID() {
        return partitionID;
    }

    /**
     * @param partitionID the partitionID to set
     */
    public void setPartitionID(String partitionID) {
        this.partitionID = partitionID;
    }

    /**
     * @return the schema
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * @param schema the schema to set
     */
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    /**
     * @return the schemaPath
     */
    public String getSchemaPath() {
        return schemaPath;
    }

    /**
     * @param schemaPath the schemaPath to set
     */
    public void setSchemaPath(String schemaPath) {
        this.schemaPath = schemaPath;
    }

    /**
     * @return the metadataPath
     */
    public String getMetadataPath() {
        return metadataPath;
    }

    /**
     * @param metadataPath the metadataPath to set
     */
    public void setMetadataPath(String metadataPath) {
        this.metadataPath = metadataPath;
    }

    /**
     * @return the loadsToBeMerged
     */
    public List<String> getLoadsToBeMerged() {
        return loadsToBeMerged;
    }

    /**
     * @param loadsToMerge the loadsToBeMerged to set
     */
    public void setLoadsToBeMerged(List<String> loadsToMerge) {
        this.loadsToBeMerged = loadsToMerge;
    }

    /**
     * @return the mergedLoadName
     */
    public String getMergedLoadName() {
        return mergedLoadName;
    }

    /**
     * @param mergedLoadName the mergedLoadName to set
     */
    public void setMergedLoadName(String mergedLoadName) {
        this.mergedLoadName = mergedLoadName;
    }

}
