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

package org.apache.carbondata.examples

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.{AllDictionaryUtil, ExampleUtils}


object AllDictionaryExample {

  def main(args: Array[String]) {
    val spark = ExampleUtils.createCarbonSession("AllDictionaryExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {
    val testData = ExampleUtils.currentPath + "/src/main/resources/dataSample.csv"
    val csvHeader = "ID,date,country,name,phonetype,serialname,salary"
    val dictCol = "|date|country|name|phonetype|serialname|"
    val allDictFile = ExampleUtils.currentPath + "/target/data.dictionary"

    // extract all dictionary files from source data
    AllDictionaryUtil.extractDictionary(spark.sparkContext,
      testData, allDictFile, csvHeader, dictCol)
    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    spark.sql("DROP TABLE IF EXISTS dictionary_table")

    spark.sql(
      s"""
         | CREATE TABLE IF NOT EXISTS dictionary_table(
         | ID Int,
         | date Date,
         | country String,
         | name String,
         | phonetype String,
         | serialname String,
         | salary Int,
         | floatField float
         | ) STORED BY 'carbondata'
       """.stripMargin)

    spark.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table dictionary_table
           options('ALL_DICTIONARY_PATH'='$allDictFile', 'SINGLE_PASS'='true')
           """)

    spark.sql("""
           SELECT * FROM dictionary_table
           """).show()

    spark.sql("""
           SELECT * FROM dictionary_table where floatField=3.5
           """).show()

    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    spark.sql("DROP TABLE IF EXISTS dictionary_table")

    // clean local dictionary files
    AllDictionaryUtil.cleanDictionary(allDictFile)
  }
}
