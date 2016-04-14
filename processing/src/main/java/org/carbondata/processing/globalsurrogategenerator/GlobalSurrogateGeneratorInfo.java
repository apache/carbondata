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

package org.carbondata.processing.globalsurrogategenerator;

import org.carbondata.core.carbon.CarbonDef.CubeDimension;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;

public class GlobalSurrogateGeneratorInfo {

    private String cubeName;

    private String tableName;

    private int numberOfPartition;

    private Schema schema;

    private String storeLocation;

    private CubeDimension[] cubeDimensions;

    private String partiontionColumnName;

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getNumberOfPartition() {
        return numberOfPartition;
    }

    public void setNumberOfPartition(int numberOfPartition) {
        this.numberOfPartition = numberOfPartition;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public String getStoreLocation() {
        return storeLocation;
    }

    public void setStoreLocation(String storeLocation) {
        this.storeLocation = storeLocation;
    }

    public String getPartiontionColumnName() {
        return partiontionColumnName;
    }

    public void setPartiontionColumnName(String partiontionColumnName) {
        this.partiontionColumnName = partiontionColumnName;
    }

    public CubeDimension[] getCubeDimensions() {
        return cubeDimensions;
    }

    public void setCubeDimensions(CubeDimension[] cubeDimensions) {
        this.cubeDimensions = cubeDimensions;
    }
}