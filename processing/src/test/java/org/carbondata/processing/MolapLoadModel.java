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
 *
 */
package org.carbondata.processing;

import java.io.Serializable;

import org.carbondata.core.carbon.CarbonDef.Schema;

/**
 * @author R00900208
 */
public class MolapLoadModel implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 6580168429197697465L;

    private String schemaName;

    private String cubeName;

    private String tableName;

    private String factFilePath;

    private String dimFolderPath;

    private String jdbcUrl;

    private String dbUserName;

    private String dbPwd;

    private String schemaPath;

    private String driverClass;

    private String partitionId;

    private Schema schema;

    /**
     * @return the schemaPath
     */
    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

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
     * @return the factFilePath
     */
    public String getFactFilePath() {
        return factFilePath;
    }

    /**
     * @param factFilePath the factFilePath to set
     */
    public void setFactFilePath(String factFilePath) {
        this.factFilePath = factFilePath;
    }

    /**
     * @return the dimFolderPath
     */
    public String getDimFolderPath() {
        return dimFolderPath;
    }

    /**
     * @param dimFolderPath the dimFolderPath to set
     */
    public void setDimFolderPath(String dimFolderPath) {
        this.dimFolderPath = dimFolderPath;
    }

    /**
     * @return the jdbcUrl
     */
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    /**
     * @param jdbcUrl the jdbcUrl to set
     */
    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * @return the dbUserName
     */
    public String getDbUserName() {
        return dbUserName;
    }

    /**
     * @param dbUserName the dbUserName to set
     */
    public void setDbUserName(String dbUserName) {
        this.dbUserName = dbUserName;
    }

    /**
     * @return the dbPwd
     */
    public String getDbPwd() {
        return dbPwd;
    }

    /**
     * @param dbPwd the dbPwd to set
     */
    public void setDbPwd(String dbPwd) {
        this.dbPwd = dbPwd;
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
     * @return the driverClass
     */
    public String getDriverClass() {
        return driverClass;
    }

    /**
     * @param driverClass the driverClass to set
     */
    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public MolapLoadModel getCopyWithPartition(String uniqueId) {
        MolapLoadModel copy = new MolapLoadModel();
        copy.cubeName = cubeName + "_" + uniqueId;
        copy.dbPwd = dbPwd;
        copy.dbUserName = dbUserName;
        copy.dimFolderPath = dimFolderPath;
        copy.driverClass = driverClass;
        copy.factFilePath = factFilePath + "/" + uniqueId;
        copy.jdbcUrl = jdbcUrl;
        copy.schemaName = schemaName + "_" + uniqueId;
        copy.schemaPath = schemaPath;
        copy.tableName = tableName;
        copy.partitionId = uniqueId;
        copy.schema = schema;

        if (uniqueId != null && schema != null) {
            String originalSchemaName = schema.name;
            String originalCubeName = schema.cubes[0].name;
            copy.schema.name = originalSchemaName + "_" + uniqueId;
            copy.schema.cubes[0].name = originalCubeName + "_" + uniqueId;
        }

        return copy;
    }

    /**
     * @return the partitionId
     */
    public String getPartitionId() {
        return partitionId;
    }

    /**
     * @param partitionId the partitionId to set
     */
    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

}
