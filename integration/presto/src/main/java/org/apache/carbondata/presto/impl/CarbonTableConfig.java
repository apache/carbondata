/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.presto.impl;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

public class CarbonTableConfig {
    //read from config
    private String dbPtah;
    private String tablePath;
    private String storePath;

    @NotNull
    public String getDbPtah() {
        return dbPtah;
    }

    @Config("carbondata-store")
    public CarbonTableConfig setDbPtah(String dbPtah) {
        this.dbPtah = dbPtah;
        return this;
    }

    @NotNull
    public String getTablePath() {
        return tablePath;
    }

    @Config("carbondata-store")
    public CarbonTableConfig setTablePath(String tablePath) {
        this.tablePath = tablePath;
        return this;
    }

    @NotNull
    public String getStorePath() {
        return storePath;
    }

    @Config("carbondata-store")
    public CarbonTableConfig setStorePath(String storePath) {
        this.storePath = storePath;
        return this;
    }
}
