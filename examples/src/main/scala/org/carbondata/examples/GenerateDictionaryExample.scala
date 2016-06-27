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

import org.apache.spark.sql.{CarbonContext, CarbonEnv, CarbonRelation}

import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier
import org.carbondata.core.carbon.{CarbonTableIdentifier, ColumnIdentifier}
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.carbon.path.CarbonStorePath
import org.carbondata.examples.util.InitForExamples
import org.carbondata.spark.load.CarbonLoaderUtil

/**
 * example for global dictionary generation
 * pls check files under directory of target/store/default/dictSample/Metadata
 * and verify global dictionary values
 */
object GenerateDictionaryExample {

  def main(args: Array[String]) {
    val cc = InitForExamples.createCarbonContext("GenerateDictionaryExample")
    val factFilePath = InitForExamples.currentPath + "/src/main/resources/factSample.csv"
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(InitForExamples.storeLocation,
      new CarbonTableIdentifier("default", "dictSample", "1"))
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
    val tableIdentifier = new CarbonTableIdentifier("default", "dictSample", "1")
    printDictionary(cc, tableIdentifier, dictFolderPath)
  }

  def printDictionary(carbonContext: CarbonContext, carbonTableIdentifier: CarbonTableIdentifier,
                      dictFolderPath: String) {
    val dataBaseName = carbonTableIdentifier.getDatabaseName
    val tableName = carbonTableIdentifier.getTableName
    val carbonRelation = CarbonEnv.getInstance(carbonContext).carbonCatalog.
      lookupRelation1(Option(dataBaseName),
        tableName, None) (carbonContext).asInstanceOf[CarbonRelation]
    val carbonTable = carbonRelation.cubeMeta.carbonTable
    val dimensions = carbonTable.getDimensionByTableName(tableName.toLowerCase())
      .toArray.map(_.asInstanceOf[CarbonDimension])
    // scalastyle:off println
    // print dictionary information
    println("**********************************************************************************")
    println(s"table:$tableName in " + s"database:$dataBaseName")
    for (dimension <- dimensions) {
      println("**********************************************************************************")
      println(s"dictionary of dimension: ${dimension.getColName}")
      println(s"Key\t\t\tValue")
      val columnIdentifier = new DictionaryColumnUniqueIdentifier(carbonTableIdentifier,
        dimension.getColumnIdentifier, dimension.getDataType)
      val dict = CarbonLoaderUtil.getDictionary(columnIdentifier, carbonContext.storePath)
      var index: Int = 1
      var distinctValue = dict.getDictionaryValueForKey(index)
      while (distinctValue != null) {
        println(index + s"\t\t\t" + distinctValue)
        index += 1
        distinctValue = dict.getDictionaryValueForKey(index)
      }
    }
    println("**********************************************************************************")
    // scalastyle:on println
  }

}
