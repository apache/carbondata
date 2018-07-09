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

package org.apache.carbondata.store;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.store.api.conf.StoreConf;
import org.apache.carbondata.store.api.descriptor.LoadDescriptor;
import org.apache.carbondata.store.api.descriptor.SelectDescriptor;
import org.apache.carbondata.store.api.descriptor.TableDescriptor;
import org.apache.carbondata.store.api.descriptor.TableIdentifier;
import org.apache.carbondata.store.api.exception.StoreException;
import org.apache.carbondata.store.impl.DistributedCarbonStore;
import org.apache.carbondata.store.impl.distributed.Worker;
import org.apache.carbondata.store.util.StoreUtil;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedCarbonStoreTest {

  private static String projectFolder;
  private static DistributedCarbonStore store;

  @BeforeClass
  public static void beforeAll() throws IOException {
    projectFolder = new File(DistributedCarbonStoreTest.class.getResource("/").getPath() +
        "../../../../").getCanonicalPath();
    String confFile = projectFolder + "/store/conf/store.conf";
    StoreConf storeConf = new StoreConf(confFile);

    String log4jFile = projectFolder + "/store/conf/log4j.properties";
    System.setProperty("log.path", projectFolder + "/store/core/target/master_worker.log");
    StoreUtil.initLog4j(log4jFile);

    store = new DistributedCarbonStore(storeConf);
    projectFolder = new File(LocalCarbonStoreTest.class.getResource("/").getPath() + "../../../../")
        .getCanonicalPath();

    // start worker
    Worker worker = new Worker(storeConf);
    worker.start();
  }

  @AfterClass
  public static void afterAll() throws IOException {
    store.close();
  }

  @Before
  public void cleanFile() {
    assert (TestUtil.cleanMdtFile());
  }

  @After
  public void verifyDMFile() {
    assert (!TestUtil.verifyMdtFile());
  }

  @Test
  public void testSelect() throws IOException, StoreException {
    TableIdentifier tableIdentifier = new TableIdentifier("table_1", "default");
    store.dropTable(tableIdentifier);
    TableDescriptor table = TableDescriptor
        .builder()
        .ifNotExists()
        .table(tableIdentifier)
        .comment("first table")
        .column("shortField", DataTypes.SHORT, "short field")
        .column("intField", DataTypes.INT, "int field")
        .column("bigintField", DataTypes.LONG, "long field")
        .column("doubleField", DataTypes.DOUBLE, "double field")
        .column("stringField", DataTypes.STRING, "string field")
        .column("timestampField", DataTypes.TIMESTAMP, "timestamp field")
        .column("decimalField", DataTypes.createDecimalType(18, 2), "decimal field")
        .column("dateField", DataTypes.DATE, "date field")
        .column("charField", DataTypes.STRING, "char field")
        .column("floatField", DataTypes.DOUBLE, "float field")
        .tblProperties(CarbonCommonConstants.SORT_COLUMNS, "intField")
        .create();
    store.createTable(table);

    // load one segment
    LoadDescriptor load = LoadDescriptor
        .builder()
        .table(tableIdentifier)
        .overwrite(false)
        .inputPath(projectFolder + "/store/core/src/test/resources/data1.csv")
        .options("header", "true")
        .create();
    store.loadData(load);

    // select row
    SelectDescriptor select = SelectDescriptor
        .builder()
        .table(tableIdentifier)
        .select("intField", "stringField")
        .limit(5)
        .create();
    List<CarbonRow> result = store.select(select);
    Assert.assertEquals(5, result.size());

    // select row with filter
    SelectDescriptor select2 = SelectDescriptor
        .builder()
        .table(tableIdentifier)
        .select("intField", "stringField")
        .filter(new EqualToExpression(
            new ColumnExpression("intField", DataTypes.INT),
            new LiteralExpression(11, DataTypes.INT)))
        .limit(5)
        .create();
    List<CarbonRow> result2 = store.select(select2);
    Assert.assertEquals(1, result2.size());

    store.dropTable(tableIdentifier);
    Assert.assertTrue(!FileFactory.isFileExist(store.getTablePath("table_1", "default")));

  }

}
