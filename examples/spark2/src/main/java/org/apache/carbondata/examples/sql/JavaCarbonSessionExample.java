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

package org.apache.carbondata.examples.sql;

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.sql.CarbonSession;
import org.apache.spark.sql.SparkSession;

public class JavaCarbonSessionExample {

  public static void main(String[] args) {
    try {
      // configure log4j
      String rootPath =
          new File(JavaCarbonSessionExample.class.getResource("/").getPath() + "../../../..")
              .getCanonicalPath();
      System.setProperty("path.target", rootPath + "/examples/spark2/target");
      PropertyConfigurator.configure(rootPath + "/examples/spark2/src/main/resources/log4j.properties");

      // set timestamp and date format used in data.csv for loading
      CarbonProperties.getInstance()
          .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
          .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd");

      // create CarbonSession

      SparkSession.Builder builder = SparkSession.builder()
          .master("local")
          .appName("JavaCarbonSessionExample")
          .config("spark.driver.host", "localhost");

      SparkSession carbon = new CarbonSession.CarbonBuilder(builder)
          .getOrCreateCarbonSession();

      carbon.sql("DROP TABLE IF EXISTS carbonsession_table");
      carbon.sql("DROP TABLE IF EXISTS stored_as_carbondata_table");

      carbon.sql(
              "CREATE TABLE carbonsession_table( " +
                      "shortField SHORT, " +
                      "intField INT, " +
                      "bigintField LONG, " +
                      "doubleField DOUBLE, " +
                      "stringField STRING, " +
                      "timestampField TIMESTAMP, " +
                      "decimalField DECIMAL(18,2), " +
                      "dateField DATE, " +
                      "charField CHAR(5), " +
                      "floatField FLOAT " +
                      ") " +
                      "STORED AS carbondata"
      );

      String path = rootPath + "/examples/spark2/src/main/resources/data.csv";
      carbon.sql(
              "LOAD DATA LOCAL INPATH " + "\'" + path + "\' " +
                      "INTO TABLE carbonsession_table " +
                      "OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')"
      );

      carbon.sql(
              "SELECT charField, stringField, intField " +
                      "FROM carbonsession_table " +
                      "WHERE stringfield = 'spark' AND decimalField > 40"
      ).show();

      carbon.sql(
              "SELECT * " +
                      "FROM carbonsession_table WHERE length(stringField) = 5"
      ).show();

      carbon.sql(
              "SELECT * " +
                      "FROM carbonsession_table " +
                      "WHERE date_format(dateField, \'yyyy-MM-dd \') = \'2015-07-23\'"
      ).show();

      carbon.sql("SELECT count(stringField) FROM carbonsession_table").show();

      carbon.sql(
              "SELECT sum(intField), stringField " +
                      "FROM carbonsession_table " +
                      "GROUP BY stringField"
      ).show();

      carbon.sql(
              "SELECT t1.*, t2.* " +
                      "FROM carbonsession_table t1, carbonsession_table t2 " +
                      "WHERE t1.stringField = t2.stringField"
      ).show();

      carbon.sql(
              "WITH t1 AS ( " +
                      "SELECT * FROM carbonsession_table " +
                      "UNION ALL " +
                      "SELECT * FROM carbonsession_table" +
                      ") " +
                      "SELECT t1.*, t2.* " +
                      "FROM t1, carbonsession_table t2 " +
                      "WHERE t1.stringField = t2.stringField"
      ).show();

      carbon.sql(
              "SELECT * " +
                      "FROM carbonsession_table " +
                      "WHERE stringField = 'spark' and floatField > 2.8"
      ).show();

      carbon.sql(
              "CREATE TABLE stored_as_carbondata_table( " +
                      "name STRING, " +
                      "age INT" +
                      ") " +
                      "STORED AS carbondata"
      );

      carbon.sql("INSERT INTO stored_as_carbondata_table VALUES ('Bob',28) ");
      carbon.sql("SELECT * FROM stored_as_carbondata_table").show();

      carbon.sql("DROP TABLE IF EXISTS carbonsession_table");
      carbon.sql("DROP TABLE IF EXISTS stored_as_carbondata_table");

      carbon.close();
    }
    catch (IOException e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}
