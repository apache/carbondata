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

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.carbondata.core.util.CarbonTestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HiveCarbonTest extends HiveTestUtils {

  private static Statement statement;

  // resource directory path
  private static String resourceDirectoryPath = HiveCarbonTest.class.getResource("/").getPath()
          +
          "../." +
          "./src/main/resources/";

  // "/csv" subdirectory name
  private static final String CSV = "csv";

  // "/complex" subdirectory name
  private static final String COMPLEX = "complex";

  // "/text" subdirectory name
  private static final String TEXT = "text";

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
    String csvFilePath = (resourceDirectoryPath + CSV).replace("\\", "/");
    String complexFilePath = (resourceDirectoryPath + COMPLEX).replace("\\", "/");
    statement.execute(String.format("CREATE external TABLE hive_table_complex(arrayField  ARRAY<STRING>, mapField MAP<String, String>, structField STRUCT<city: String, pincode: int>) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ('field.delim'=',', 'collection.delim'='$', 'mapkey.delim'='@') location '%s' TBLPROPERTIES('external.table.purge'='false')", complexFilePath));
    statement.execute(String.format("CREATE external TABLE hive_table(shortField SMALLINT, intField INT, bigintField BIGINT, doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location '%s' TBLPROPERTIES ('external.table.purge'='false')", csvFilePath));
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
  public void verifyLocalDictionaryValues() throws Exception {
    statement.execute("drop table if exists hive_carbon_table");
    statement.execute("CREATE TABLE hive_carbon_table(shortField SMALLINT , intField INT, bigintField BIGINT , doubleField DOUBLE, stringField STRING, timestampField TIMESTAMP, decimalField DECIMAL(18,2), dateField DATE, charField CHAR(5), floatField FLOAT) stored by 'org.apache.carbondata.hive.CarbonStorageHandler' TBLPROPERTIES ('local_dictionary_enable'='true','local_dictionary_include'='stringField')");
    statement.execute("insert into hive_carbon_table select * from hive_table");
    File rootPath = new File(HiveTestUtils.class.getResource("/").getPath() + "../../../..");
    String storePath = rootPath.getAbsolutePath() + "/integration/hive/target/warehouse/warehouse/hive_carbon_table/";
    ArrayList<DimensionRawColumnChunk> dimRawChunk = CarbonTestUtil.getDimRawChunk(storePath, 0);
    String[] dictionaryData = new String[]{"hive", "impala", "flink", "spark"};
    assert(CarbonTestUtil.validateDictionary(dimRawChunk.get(0), dictionaryData));
  }

  @Test
  public void verifyInsertIntoSelectOperation() throws Exception {
    statement.execute("drop table if exists hive_carbon_table1");
    statement.execute("CREATE TABLE hive_carbon_table1(id INT, name STRING, scale DECIMAL, country STRING, salary DOUBLE) stored by 'org.apache.carbondata.hive.CarbonStorageHandler'");
    statement.execute("INSERT into hive_carbon_table1 SELECT 1, 'RAM', '2.3', 'INDIA', 3500");
    statement.execute("INSERT into hive_carbon_table1 SELECT 2, 'RAJU', '2.4', 'RUSSIA', 3600");
    statement.execute("drop table if exists hive_carbon_table2");
    statement.execute("CREATE TABLE hive_carbon_table2(id INT, name STRING, scale DECIMAL, country STRING, salary DOUBLE) stored by 'org.apache.carbondata.hive.CarbonStorageHandler'");
    statement.execute("INSERT into hive_carbon_table2 SELECT * FROM hive_carbon_table1");
    checkAnswer(statement.executeQuery("SELECT * FROM hive_carbon_table2"),
        connection.createStatement().executeQuery("select * from hive_carbon_table1"));
    statement.execute("drop table if exists hive_carbon_table1");
    statement.execute("drop table if exists hive_carbon_table2");
  }

  @Test
  public void verifyEmptyTableSelectQuery() throws Exception {
    statement.execute("drop table if exists hive_carbon_table1");
    statement.execute("CREATE TABLE hive_carbon_table1(id INT, name STRING, scale DECIMAL, country STRING, salary DOUBLE) stored by 'org.apache.carbondata.hive.CarbonStorageHandler'");
    statement.execute("drop table if exists hive_carbon_table2");
    statement.execute("CREATE TABLE hive_carbon_table2(id INT, name STRING, scale DECIMAL, country STRING, salary DOUBLE) stored by 'org.apache.carbondata.hive.CarbonStorageHandler'");
    statement.execute("INSERT into hive_carbon_table2 SELECT * FROM hive_carbon_table1");
    checkAnswer(statement.executeQuery("SELECT * FROM hive_carbon_table2"),
        connection.createStatement().executeQuery("select * from hive_carbon_table1"));
    statement.execute("drop table if exists hive_carbon_table1");
    statement.execute("drop table if exists hive_carbon_table2");
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

  @Test
  public void testArrayType() throws Exception {
    String complexArrayPath = (resourceDirectoryPath + "array").replace("\\", "/");
    statement.execute("drop table if exists hive_table_complexArray");
    statement.execute(String.format("CREATE external TABLE hive_table_complexArray(arrayString  ARRAY<STRING>,"
        + " arrayShort ARRAY<SMALLINT>, arrayInt ARRAY<INT>, arrayLong ARRAY<BIGINT>, arrayFloat ARRAY<FLOAT>,"
        + " arrayDouble ARRAY<DOUBLE>, arrayDecimal ARRAY<DECIMAL(8,2)>, arrayChar ARRAY<CHAR(5)>, "
        + "arrayBoolean ARRAY<BOOLEAN>, arrayVarchar ARRAY<VARCHAR(50)>, arrayByte ARRAY<TINYINT>, arrayDate ARRAY<DATE>)"
        + " ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ('field.delim'=',', 'collection.delim'='$', 'mapkey.delim'='@') location '%s' TBLPROPERTIES('external.table.purge'='false')", complexArrayPath));

    statement.execute("drop table if exists hive_carbon_table7");
    statement.execute(
        "CREATE TABLE hive_carbon_table7(arrayString  ARRAY<STRING>, arrayShort ARRAY<SMALLINT>, arrayInt ARRAY<INT>, "
            + "arrayLong ARRAY<BIGINT>, arrayFloat ARRAY<FLOAT>, arrayDouble ARRAY<DOUBLE>, "
            + "arrayDecimal ARRAY<DECIMAL(8,2)>, arrayChar ARRAY<CHAR(5)>, arrayBoolean ARRAY<BOOLEAN>, arrayVarchar ARRAY<VARCHAR(50)>, "
            + "arrayByte ARRAY<TINYINT>, arrayDate ARRAY<DATE>) "
            + "stored by 'org.apache.carbondata.hive.CarbonStorageHandler' TBLPROPERTIES ('complex_delimiter'='$,@')");

    statement.execute(
        "insert into hive_carbon_table7 select * from hive_table_complexArray");

    ResultSet hiveResult = connection.createStatement().executeQuery("select * from hive_table_complexArray");
    ResultSet carbonResult = connection.createStatement().executeQuery("select * from hive_carbon_table7");
    checkAnswer(carbonResult, hiveResult);
  }


  @Test
  public void arrayOfTimestamp() throws Exception {
    statement.execute("drop table if exists hivee");
    statement.execute("CREATE external TABLE hivee(arrayInt ARRAY<timestamp>)"
        + " ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ('field.delim'=',', 'collection.delim'='$', 'mapkey.delim'='@') location '%s' TBLPROPERTIES('external.table.purge'='false')");
    statement.execute("insert into table hivee values (array(Timestamp('2000-03-12 15:00:00'),Timestamp('2001-04-15 15:58:00'),Timestamp('2002-05-27 15:20:00')))");
    statement.execute("drop table if exists carbonn");
    statement.execute(
        "CREATE TABLE carbonn(timestampField array<timestamp>) "
            + "stored by 'org.apache.carbondata.hive.CarbonStorageHandler' TBLPROPERTIES ('complex_delimiter'='$,@', 'BAD_RECORDS_LOGGER_ENABLE' = 'TRUE')");
    statement.execute("insert into carbonn select * from hivee");
    ResultSet resultSet = connection.createStatement()
        .executeQuery("select * from carbonn");
    ResultSet hiveResults = connection.createStatement()
        .executeQuery("select * from hivee");
   checkAnswer(resultSet, hiveResults);
  }

  @Test
  public void testMapType() throws Exception {
    String complexMapPath = (resourceDirectoryPath + "map").replace("\\", "/");
    statement.execute("drop table if exists hive_table_complexMap");
    statement.execute(String.format("CREATE external TABLE hive_table_complexMap(mapField1 MAP<STRING, STRING>, mapField2 MAP<INT, STRING>, mapField3 MAP<DOUBLE, "
        + "FLOAT>,mapField4 MAP<TINYINT, SMALLINT>, mapField5 MAP<DECIMAL(10,2), VARCHAR(50)>, mapField6 MAP<BIGINT,DATE>,mapField7 MAP<CHAR(5),INT>, "
        + "mapField8 MAP<BIGINT,BOOLEAN>)"
        + " ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ('field.delim'=',', 'collection.delim'='$', 'mapkey.delim'='@') location '%s' TBLPROPERTIES('external.table.purge'='false')", complexMapPath));

    statement.execute("drop table if exists hive_carbon_table8");
    statement.execute(
        "CREATE TABLE hive_carbon_table8(mapField1 MAP<STRING, STRING>, mapField2 MAP<INT, STRING>, mapField3 MAP<DOUBLE, FLOAT>,mapField4 MAP<TINYINT, SMALLINT>, "
            + "mapField5 MAP<DECIMAL(10,2), VARCHAR(50)>, mapField6 MAP<BIGINT, DATE>,mapField7 MAP<CHAR(5), INT>, mapField8 MAP<BIGINT,BOOLEAN>) "
            + "stored by 'org.apache.carbondata.hive.CarbonStorageHandler' TBLPROPERTIES ('complex_delimiter'='$,@')");
    statement.execute(
        "insert into hive_carbon_table8 select * from hive_table_complexMap");
    ResultSet hiveResult = connection.createStatement().executeQuery("select * from hive_table_complexMap");
    ResultSet carbonResult = connection.createStatement().executeQuery("select * from hive_carbon_table8");
    checkAnswer(carbonResult, hiveResult);
  }

  @Test
  public void testStructType() throws Exception {
    String complexStructPath = (resourceDirectoryPath + "struct").replace("\\", "/");
    statement.execute("drop table if exists hive_table_complexSTRUCT");
    statement.execute(String.format("CREATE external TABLE hive_table_complexSTRUCT(structField STRUCT<stringfield: STRING, shortfield: SMALLINT, intfield: INT, "
        + "longfield: BIGINT, floatfield: FLOAT, doublefield: DOUBLE, charfield: CHAR(4), boolfield: BOOLEAN, varcharfield: VARCHAR(50), bytefield: TINYINT, "
        + "datefield: DATE, decimalfield: DECIMAL(8,2)>) "
        + "ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES "
        + "('field.delim'=',', 'collection.delim'='$', 'mapkey.delim'='@') location '%s' TBLPROPERTIES('external.table.purge'='false')", complexStructPath));

    statement.execute("drop table if exists hive_carbon_table9");
    statement.execute(
        "CREATE TABLE hive_carbon_table9(structField STRUCT<stringfield: STRING, shortfield: SMALLINT, intfield: INT, longfield: BIGINT, floatfield: FLOAT, "
            + "doublefield: DOUBLE, charfield: CHAR(4), boolfield: BOOLEAN, varcharfield: VARCHAR(50), bytefield: TINYINT, datefield: DATE, decimalfield: DECIMAL(8,2)>) "
            + "stored by 'org.apache.carbondata.hive.CarbonStorageHandler' TBLPROPERTIES ('complex_delimiter'='$,@')");
    statement.execute(
        "insert into hive_carbon_table9 select * from hive_table_complexSTRUCT");
    ResultSet hiveResult = connection.createStatement().executeQuery("select * from hive_table_complexSTRUCT");
    ResultSet carbonResult = connection.createStatement().executeQuery("select * from hive_carbon_table9");
    checkAnswer(carbonResult, hiveResult);
  }

  @Test
  public void testBinaryDataTypeColumns() throws Exception {
    String dataPath = resourceDirectoryPath + TEXT + "/string.txt";
    statement.execute("drop table if exists mytable_n2");
    statement.execute("drop table if exists hive_carbon_table9");
    statement.execute("drop table if exists mytable_n21");
    statement.execute("CREATE TABLE mytable_n2(key binary, value int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '9'");
    statement.execute("LOAD DATA LOCAL INPATH '" + dataPath + "' INTO TABLE mytable_n2");
    statement.execute("CREATE TABLE mytable_n21(key binary, value int) ROW FORMAT DELIMITED FIELDS TERMINATED BY '9'");
    statement.execute("insert into mytable_n21 select * from mytable_n2");
    statement.execute(
        "CREATE TABLE hive_carbon_table9(key binary, value int) "
            + "stored by 'org.apache.carbondata.hive.CarbonStorageHandler'");
    statement.execute("insert into hive_carbon_table9 select * from mytable_n2");
    ResultSet hiveResult = connection.createStatement().executeQuery("select * from mytable_n2");
    ResultSet carbonResult = connection.createStatement().executeQuery("select * from hive_carbon_table9");
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
