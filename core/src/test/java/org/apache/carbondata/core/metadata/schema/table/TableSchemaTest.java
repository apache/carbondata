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

public class TableSchemaTest extends TestCase {

    private TableSchema tableSchema;

    @BeforeClass public void setUp() {
        tableSchema = getTableSchema("1000", "tableSchemaTestTable");
    }

    @AfterClass public void tearDown() {
        tableSchema = null;
    }

    @Test public void testTableInfoEquals() {
        TableSchema cmpEqualsTableSchema = getTableSchema("1000", "tableSchemaTestTable");
        TableSchema cmpNotEqualsTableSchema = getTableSchema("1000", "tableSchemaTestTable2");
        assertTrue(tableSchema.equals(cmpEqualsTableSchema));
        assertTrue(!(tableSchema.equals(cmpNotEqualsTableSchema)));
    }

    private TableSchema getTableSchema(String tableId, String tableName) {
        TableSchema tableSchema = new TableSchema();
        tableSchema.setTableId(tableId);
        tableSchema.setTableName(tableName);
        return tableSchema;
    }
}
