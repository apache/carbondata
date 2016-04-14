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

package org.carbondata.processing.suggest.datastats.model;

import java.util.List;

import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.metadata.CarbonMetadata.Cube;

public class LoadModel {
    private CarbonDef.Schema schema;

    private CarbonDef.Cube cube;

    private String partitionId = "0";

    private String tableName;
    
    private List<String> partitionNames;

	private Cube metaCube;

    private String schemaName;

    private String cubeName;

    private String dataPath;

    private String metaDataPath;

    private List<String> validSlices;

    private List<String> validUpdateSlices;

    private List<String> calculatedLoads;

    private int restructureNo;

    private long cubeCreationtime;

    private long schemaLastUpdatedTime;

    /**
     * Array of LoadMetadataDetails
     */
    private LoadMetadataDetails[] loadMetadataDetails;

    /**
     * this will have all load of store
     */
    private List<String> allLoads;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;

    }

    public CarbonDef.Schema getSchema() {
        return schema;
    }

    public void setSchema(CarbonDef.Schema schema) {
        this.schema = schema;

    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;

    }

    public CarbonDef.Cube getCube() {
        return cube;
    }

    public void setCube(CarbonDef.Cube cube) {
        this.cube = cube;
    }

    public Cube getMetaCube() {
        return this.metaCube;
    }

    public void setMetaCube(Cube metaCube) {
        this.metaCube = metaCube;

    }

    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;

    }

    public String getCubeName() {
        return this.cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public String getDataPath() {
        return dataPath;
    }

    public void setDataPath(String dataPath) {
        this.dataPath = dataPath;
    }

    public String getMetaDataPath() {
        return metaDataPath;
    }

    public void setMetaDataPath(String metaDataPath) {
        this.metaDataPath = metaDataPath;
    }

    public List<String> getValidSlices() {
        return validSlices;
    }

    public void setValidSlices(List<String> validSlices) {
        this.validSlices = validSlices;
    }

    public List<String> getValidUpdateSlices() {
        return validUpdateSlices;
    }

    public void setValidUpdateSlices(List<String> validUpdateSlices) {
        this.validUpdateSlices = validUpdateSlices;
    }

    public int getRestructureNo() {
        return this.restructureNo;
    }

    public void setRestructureNo(int restructureNo) {
        this.restructureNo = restructureNo;
    }

    public List<String> getCalculatedLoads() {
        return calculatedLoads;
    }

    public void setCalculatedLoads(List<String> calculatedLoads) {
        this.calculatedLoads = calculatedLoads;
    }

    /**
     * @return Returns the cubeCreationtime.
     */
    public long getCubeCreationtime() {
        return cubeCreationtime;
    }

    /**
     * @param cubeCreationtime The cubeCreationtime to set.
     */
    public void setCubeCreationtime(long cubeCreationtime) {
        this.cubeCreationtime = cubeCreationtime;
    }

    public List<String> getAllLoads() {
        return this.allLoads;
    }

    public void setAllLoads(List<String> allLoads) {
        this.allLoads = allLoads;

    }

    /**
     * @return Returns the schemaLastUpdatedTime.
     */
    public long getSchemaLastUpdatedTime() {
        return schemaLastUpdatedTime;
    }

    /**
     * @param schemaLastUpdatedTime The schemaLastUpdatedTime to set.
     */
    public void setSchemaLastUpdatedTime(long schemaLastUpdatedTime) {
        this.schemaLastUpdatedTime = schemaLastUpdatedTime;
    }
    
    /**
     * API will return partion names.
     * @return
     */
    public List<String> getPartitionNames() {
		return partitionNames;
	}

    /**
     * setting the partition name for suggest aggregate
     * @param partitionNames
     */
	public void setPartitionNames(List<String> partitionNames) {
		this.partitionNames = partitionNames;
	}


    /**
     *  return loadMetadata Details
     */
    public LoadMetadataDetails[] getLoadMetadataDetails() {
        return loadMetadataDetails;
    }

    /**
     * Set loadMetadataDetails
     */
    public void setLoadMetadataDetails(LoadMetadataDetails[] loadMetadataDetails) {
        this.loadMetadataDetails = loadMetadataDetails;
    }
}
