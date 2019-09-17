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

import org.apache.spark.sql.CarbonEnv;
import org.apache.spark.sql.SparkSession;

public class JavaCarbonSessionExample {

  public static void main(String[] args) throws IOException {
    // set timestamp and date format used in data.csv for loading
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
        .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd");

    // create CarbonSession

    SparkSession.Builder builder = SparkSession.builder()
        .master("local")
        .appName("JavaCarbonSessionExample")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions");

    SparkSession carbon = builder.getOrCreate();

    CarbonEnv.getInstance(carbon);

    exampleBody(carbon);
    carbon.close();
  }

  public static void exampleBody(SparkSession carbon) throws IOException {
    carbon.sql("DROP TABLE IF EXISTS source");

    carbon.sql(
        "CREATE TABLE source( " + "shortField SHORT, " + "intField INT, " + "bigintField LONG, "
            + "doubleField DOUBLE, " + "stringField STRING, " + "timestampField TIMESTAMP, "
            + "decimalField DECIMAL(18,2), " + "dateField DATE, " + "charField CHAR(5), "
            + "floatField FLOAT " + ") " + "STORED AS carbondata");

    String rootPath =
        new File(JavaCarbonSessionExample.class.getResource("/").getPath() + "../../../..")
            .getCanonicalPath();
    String path = rootPath + "/examples/spark2/src/main/resources/data.csv";
    carbon.sql("LOAD DATA LOCAL INPATH " + "\'" + path + "\' " + "INTO TABLE source "
        + "OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')");

    carbon.sql("SELECT charField, stringField, intField " + "FROM source "
        + "WHERE stringfield = 'spark' AND decimalField > 40").show();

    carbon.sql("SELECT * " + "FROM source WHERE length(stringField) = 5").show();

    carbon.sql("SELECT * " + "FROM source "
        + "WHERE date_format(dateField, \'yyyy-MM-dd \') = \'2015-07-23\'").show();

    carbon.sql("SELECT count(stringField) FROM source").show();

    carbon.sql("SELECT sum(intField), stringField " + "FROM source " + "GROUP BY stringField")
        .show();

    carbon.sql("SELECT t1.*, t2.* " + "FROM source t1, source t2 "
        + "WHERE t1.stringField = t2.stringField").show();

    carbon.sql(
        "WITH t1 AS ( " + "SELECT * FROM source " + "UNION ALL " + "SELECT * FROM source" + ") "
            + "SELECT t1.*, t2.* " + "FROM t1, source t2 "
            + "WHERE t1.stringField = t2.stringField").show();

    carbon.sql("SELECT * " + "FROM source " + "WHERE stringField = 'spark' and floatField > 2.8")
        .show();

    carbon.sql("DROP TABLE IF EXISTS source");
  }
}
