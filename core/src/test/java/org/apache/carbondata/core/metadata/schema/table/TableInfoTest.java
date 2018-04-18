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
package org.apache.carbondata.core.metadata.schema.table;

import junit.framework.TestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TableInfoTest extends TestCase {

    private TableInfo tableInfo;

    @BeforeClass public void setUp() {
        tableInfo = getTableInfo("tableInfoTestDatabase", "equalsTableInfoTestTable");
    }

    @AfterClass public void tearDown() {
        tableInfo = null;
    }

    @Test public void testTableInfoEquals() {
        TableInfo cmpEqualsTableInfo =
            getTableInfo("tableInfoTestDatabase", "equalsTableInfoTestTable");
        TableInfo cmpNotEqualsTableInfo =
            getTableInfo("tableInfoTestDatabase", "notEqualsTableInfoTestTable");
        assertTrue(tableInfo.equals(cmpEqualsTableInfo));
        assertTrue(!(tableInfo.equals(cmpNotEqualsTableInfo)));
    }

    private TableInfo getTableInfo(String databaseName, String tableName) {
        TableInfo info = new TableInfo();
        info.setDatabaseName("tableInfoTestDatabase");
        info.setLastUpdatedTime(1000L);
        info.setTableUniqueName(CarbonTable.buildUniqueName(databaseName, tableName));
        return info;
    }
}
