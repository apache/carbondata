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
package org.apache.carbondata.hive;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class HiveCarbonTest extends HiveTestUtils {

  private static Statement statement;

  @BeforeClass
  public static void setup() throws Exception {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT, "false");
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, "false");
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "hive");
    statement = connection.createStatement();
    statement.execute("drop table if exists hive_carbon_table1");
    statement.execute("drop table if exists hive_carbon_table2");
    statement.execute("drop table if exists hive_carbon_table3");
    statement.execute("drop table if exists hive_carbon_table4");
    statement.execute("drop table if exists hive_carbon_table5");
    statement.execute("drop table if exists hive_table");
    statement.execute("drop table if exists hive_table_complex");
    statement.execute("CREATE external TABLE hive_table_complex(arrayField  ARRAY<STRING>, mapField MAP<String, String>, structField STRUCT<city: String, pincode: int>) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ('field.delim'=',', 'collection.delim'='$', 'mapkey.delim'='@') location '/home/root1/projects/carbondata/integration/hive/src/main/resources/complex/' TBLPROPERTIES('external.table.purge'='false')");
    statement.execute("CREATE external TABLE hive_table( shortField SMALLINT, intField INT, bigintField BIGINT , doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location '/home/root1/projects/carbondata/integration/hive/src/main/resources/csv/' TBLPROPERTIES ('external.table.purge'='false')");
  }

  @Test
  public void verifyDataAfterLoad() throws Exception {
    statement.execute("drop table if exists hive_carbon_table4");
    statement.execute("CREATE TABLE hive_carbon_table4(shortField SMALLINT , intField INT, bigintField BIGINT , doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT) stored by 'org.apache.carbondata.hive.CarbonStorageHandler'");
    statement.execute("insert into hive_carbon_table4 select * from hive_table");
    checkAnswer(statement.executeQuery("select * from hive_carbon_table4"),
            connection.createStatement().executeQuery("select * from hive_table"));
  }

  @Test
  public void verifyDataAfterLoadUsingSortColumns() throws Exception {
    statement.execute("drop table if exists hive_carbon_table5");
    statement.execute(
        "CREATE TABLE hive_carbon_table5(shortField SMALLINT , intField INT, bigintField BIGINT , doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT) stored by 'org.apache.carbondata.hive.CarbonStorageHandler'");
    statement.execute("insert into hive_carbon_table5 select * from hive_table");
    ResultSet resultSet = connection.createStatement()
        .executeQuery("select * from hive_carbon_table5");
    ResultSet hiveResults = connection.createStatement()
        .executeQuery("select * from hive_table");
    checkAnswer(resultSet, hiveResults);
  }

  @Test
  public void testCreateAndLoadUsingComplexColumns() throws Exception {
    statement.execute("drop table if exists hive_carbon_table6");
    statement.execute(
        "CREATE TABLE hive_carbon_table6(arrayField  ARRAY<STRING>, mapField MAP<String, String>, structField STRUCT< city: String, pincode: int >) stored by 'org.apache.carbondata.hive.CarbonStorageHandler' TBLPROPERTIES ('complex_delimiter'='$,@')");
    statement.execute(
        "insert into hive_carbon_table6 select * from hive_table_complex");
    ResultSet hiveResult = connection.createStatement().executeQuery("select * from hive_table_complex where mapField['Key1']='Val1'");
    ResultSet carbonResult = connection.createStatement().executeQuery("select * from hive_carbon_table6 where mapField['Key1']='Val1'");
    checkAnswer(carbonResult, hiveResult);
  }

  @AfterClass
  public static void tearDown() {
    try {
      connection.close();
      hiveEmbeddedServer2.stop();
    } catch (Exception e) {
      throw new RuntimeException("Unable to close Hive Embedded Server", e);
    }
  }

}
