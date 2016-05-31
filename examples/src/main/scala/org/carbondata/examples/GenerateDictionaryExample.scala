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

package org.carbondata.examples

import java.io._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{CarbonContext, CarbonEnv, CarbonRelation}

import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.carbon.path.CarbonStorePath
import org.carbondata.examples.util.InitForExamples

/**
 * example for global dictionary generation
 * pls check directory of target/store/default and verify global dict file
 */
object GenerateDictionaryExample {

  def main(args: Array[String]) {
    val cc = InitForExamples.createCarbonContext("GenerateDictionaryExample")
    val factFilePath = InitForExamples.currentPath + "/src/main/resources/factSample.csv"
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(InitForExamples.storeLocation,
      new CarbonTableIdentifier("default", "dictSample"))
    val dictFolderPath = carbonTablePath.getMetadataDirectoryPath

    // execute sql statement
    cc.sql("DROP TABLE IF EXISTS dictSample")
    cc.sql("""
           CREATE TABLE IF NOT EXISTS dictSample(id Int, name String, city String, salary Int)
           STORED BY 'org.apache.carbondata.format'
           """)

    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$factFilePath' INTO TABLE dictSample
           """)

    // check generated dictionary
    val tableIdentifier = new CarbonTableIdentifier("default", "dictSample")
    printDictionary(cc, tableIdentifier, dictFolderPath)
  }

  def printDictionary(carbonContext: CarbonContext, carbonTableIdentifier: CarbonTableIdentifier,
                      dictFolderPath: String) {
    val dataBaseName = carbonTableIdentifier.getDatabaseName
    val tableName = carbonTableIdentifier.getTableName
    val catalog = CarbonEnv.getInstance(carbonContext).carbonCatalog
    val carbonRelation = catalog.lookupRelation1(Option(dataBaseName),
      tableName, None)(carbonContext)
      .asInstanceOf[CarbonRelation]
    val table = carbonRelation.cubeMeta.carbonTable
    val dimensions = table.getDimensionByTableName(tableName)
      .toArray.map(_.asInstanceOf[CarbonDimension])
    // scalastyle:off println
    // print dictionary information
    println("**********************************************************************************")
    println(s"Dictionary of table:$tableName in " +
      s"database:$dataBaseName")
    for (dimension <- dimensions) {
      val inputStream = new FileInputStream(new File(dictFolderPath + "/" +
        dimension.getColumnId + ".dict").getCanonicalPath)
      val bufferReader = new BufferedReader(new InputStreamReader(inputStream))
      println(s"column name: ${dimension.getColName}")
      var lineData = bufferReader.readLine
      while (lineData != null) {
        println(lineData)
        lineData = bufferReader.readLine
      }
    }
    println("**********************************************************************************")
    // scalastyle:on println
  }

}
