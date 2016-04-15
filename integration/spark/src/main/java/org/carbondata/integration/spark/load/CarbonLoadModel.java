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
package org.carbondata.integration.spark.load;

import java.io.Serializable;
import java.util.List;

import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.load.LoadMetadataDetails;

public class CarbonLoadModel implements Serializable {
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
    
    private CarbonDataLoadSchema carbonDataLoadSchema;

	private String[] aggTables;

    private String aggTableName;

    private boolean aggLoadRequest;

    private String factStoreLocation;

    private boolean isRetentionRequest;

    private List<String> factFilesToProcess;
    private String csvHeader;
    private String csvDelimiter;
    private String complexDelimiterLevel1;
    private String complexDelimiterLevel2;

    private boolean isDirectLoad;
    private List<LoadMetadataDetails> loadMetadataDetails;

    private String blocksID;

    /**
     * get blocck id
     * @return
     */
    public String getBlocksID() {
        return blocksID;
    }

    /**
     * set block id for carbon load model
     * @param blocksDetailID
     */
    public void setBlocksID(String blocksDetailID) {
        this.blocksID = blocksID;
    }

    public String getCsvDelimiter() {
        return csvDelimiter;
    }

    public void setCsvDelimiter(String csvDelimiter) {
        this.csvDelimiter = csvDelimiter;
    }

    public String getComplexDelimiterLevel1() {
        return complexDelimiterLevel1;
    }

    public void setComplexDelimiterLevel1(String complexDelimiterLevel1) {
        this.complexDelimiterLevel1 = complexDelimiterLevel1;
    }

    public String getComplexDelimiterLevel2() {
        return complexDelimiterLevel2;
    }

    public void setComplexDelimiterLevel2(String complexDelimiterLevel2) {
        this.complexDelimiterLevel2 = complexDelimiterLevel2;
    }

    public boolean isDirectLoad() {
        return isDirectLoad;
    }

    public void setDirectLoad(boolean isDirectLoad) {
        this.isDirectLoad = isDirectLoad;
    }

    public List<String> getFactFilesToProcess() {
        return factFilesToProcess;
    }

    public void setFactFilesToProcess(List<String> factFilesToProcess) {
        this.factFilesToProcess = factFilesToProcess;
    }

    public String getCsvHeader() {
        return csvHeader;
    }

    public void setCsvHeader(String csvHeader) {
        this.csvHeader = csvHeader;
    }

    /**
     * @return the schemaPath
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * @param schema the schemaPath to set
     */
    public void setSchema(Schema schema) {
        this.schema = schema;
    }
    
    /**
     * @return carbon dataload schema
     */
    public CarbonDataLoadSchema getCarbonDataLoadSchema() {
    	return carbonDataLoadSchema;
    }
    
    /**
     * @param carbonDataLoadSchema
     */
    public void setCarbonDataLoadSchema(CarbonDataLoadSchema carbonDataLoadSchema) {
    	this.carbonDataLoadSchema = carbonDataLoadSchema;
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

    //TODO SIMIAN

    /**
     * @param dimFolderPath the dimFolderPath to set
     */
    public void setDimFolderPath(String dimFolderPath) {
        this.dimFolderPath = dimFolderPath;
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

    /**
     * get copy with parition and blocks id
     * @param uniqueId
     * @param blocksID
     * @return
     */
    public CarbonLoadModel getCopyWithPartition(String uniqueId,String blocksID) {
        CarbonLoadModel copy = new CarbonLoadModel();
        copy.cubeName = cubeName + '_' + uniqueId;
        copy.dbPwd = dbPwd;
        copy.dbUserName = dbUserName;
        copy.dimFolderPath = dimFolderPath;
        copy.driverClass = driverClass;
        copy.factFilePath = factFilePath + '/' + uniqueId;
        copy.jdbcUrl = jdbcUrl;
        copy.schemaName = schemaName + '_' + uniqueId;
        copy.schemaPath = schemaPath;
        copy.tableName = tableName;
        copy.partitionId = uniqueId;
        copy.aggTables = aggTables;
        copy.aggTableName = aggTableName;
        copy.schema = schema;
        copy.aggLoadRequest = aggLoadRequest;
        copy.loadMetadataDetails = loadMetadataDetails;
        copy.isRetentionRequest = isRetentionRequest;
        copy.complexDelimiterLevel1 = complexDelimiterLevel1;
        copy.complexDelimiterLevel2 = complexDelimiterLevel2;
        copy.carbonDataLoadSchema = carbonDataLoadSchema;
        copy.blocksID = blocksID;
        if (uniqueId != null && schema != null) {
            String originalSchemaName = schema.name;
            String originalCubeName = schema.cubes[0].name;
            copy.schema.name = originalSchemaName + '_' + uniqueId;
            copy.schema.cubes[0].name = originalCubeName + '_' + uniqueId;
        }

        return copy;
    }

    /**
     * get CarbonLoadModel with partition
     * @param uniqueId
     * @param filesForPartition
     * @param header
     * @param delimiter
     * @param blocksID
     * @return
     */
    public CarbonLoadModel getCopyWithPartition(String uniqueId, List<String> filesForPartition,
            String header, String delimiter, String blocksID) {
        CarbonLoadModel copyObj = new CarbonLoadModel();
        copyObj.cubeName = cubeName + '_' + uniqueId;
        copyObj.dbPwd = dbPwd;
        copyObj.dbUserName = dbUserName;
        copyObj.dimFolderPath = dimFolderPath;
        copyObj.driverClass = driverClass;
        copyObj.factFilePath = null;
        copyObj.jdbcUrl = jdbcUrl;
        copyObj.schemaName = schemaName + '_' + uniqueId;
        copyObj.schemaPath = schemaPath;
        copyObj.tableName = tableName;
        copyObj.partitionId = uniqueId;
        copyObj.aggTables = aggTables;
        copyObj.aggTableName = aggTableName;
        copyObj.schema = schema;
        copyObj.aggLoadRequest = aggLoadRequest;
        copyObj.loadMetadataDetails = loadMetadataDetails;
        copyObj.isRetentionRequest = isRetentionRequest;
        copyObj.carbonDataLoadSchema = carbonDataLoadSchema;
        
        if (uniqueId != null && schema != null) {
            String originalSchemaName = schema.name;
            String originalCubeName = schema.cubes[0].name;
            copyObj.schema.name = originalSchemaName + '_' + uniqueId;
            copyObj.schema.cubes[0].name = originalCubeName + '_' + uniqueId;
        }
        copyObj.csvHeader = header;
        copyObj.factFilesToProcess = filesForPartition;
        copyObj.isDirectLoad = true;
        copyObj.csvDelimiter = delimiter;
        copyObj.complexDelimiterLevel1 = complexDelimiterLevel1;
        copyObj.complexDelimiterLevel2 = complexDelimiterLevel2;
        copyObj.blocksID = blocksID;
        return copyObj;
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

    /**
     * @return the aggTables
     */
    public String[] getAggTables() {
        return aggTables;
    }

    /**
     * @param aggTables the aggTables to set
     */
    public void setAggTables(String[] aggTables) {
        this.aggTables = aggTables;
    }

    /**
     * @return the aggLoadRequest
     */
    public boolean isAggLoadRequest() {
        return aggLoadRequest;
    }

    /**
     * @param aggLoadRequest the aggLoadRequest to set
     */
    public void setAggLoadRequest(boolean aggLoadRequest) {
        this.aggLoadRequest = aggLoadRequest;
    }

    /**
     * @return Returns the factStoreLocation.
     */
    public String getFactStoreLocation() {
        return factStoreLocation;
    }

    /**
     * @param factStoreLocation The factStoreLocation to set.
     */
    public void setFactStoreLocation(String factStoreLocation) {
        this.factStoreLocation = factStoreLocation;
    }

    /**
     * @return Returns the aggTableName.
     */
    public String getAggTableName() {
        return aggTableName;
    }

    /**
     * @param aggTableName The aggTableName to set.
     */
    public void setAggTableName(String aggTableName) {
        this.aggTableName = aggTableName;
    }

    /**
     * isRetentionRequest
     *
     * @return
     */
    public boolean isRetentionRequest() {
        return isRetentionRequest;
    }

    /**
     * @param isRetentionRequest
     */
    public void setRetentionRequest(boolean isRetentionRequest) {
        this.isRetentionRequest = isRetentionRequest;
    }

    /**
     * getLoadMetadataDetails.
     *
     * @return
     */
    public List<LoadMetadataDetails> getLoadMetadataDetails() {
        return loadMetadataDetails;
    }

    /**
     * setLoadMetadataDetails.
     *
     * @param loadMetadataDetails
     */
    public void setLoadMetadataDetails(List<LoadMetadataDetails> loadMetadataDetails) {
        this.loadMetadataDetails = loadMetadataDetails;
    }

}
