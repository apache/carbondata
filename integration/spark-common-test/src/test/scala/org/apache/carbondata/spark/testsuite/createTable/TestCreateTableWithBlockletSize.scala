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

package org.apache.carbondata.spark.testsuite.createTable

import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.reader.CarbonFooterReaderV3
import org.apache.carbondata.core.util.path.CarbonTablePath

/**
 * Test functionality of create table with blocklet size
 */
class TestCreateTableWithBlockletSize extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("use default")
    sql("drop table if exists source")
  }

  test("test create table with blocklet size") {
    val rdd = sqlContext.sparkContext.parallelize(1 to 1000000)
        .map(x => (Random.nextInt(), Random.nextInt().toString))
    sqlContext.createDataFrame(rdd)
        .write
        .format("carbondata")
        .option("table_blocksize", "8")
        .option("table_blocklet_size", "3")
        .option("tableName", "source")
        .save()

    // read footer and verify number of blocklets
    val table = CarbonEnv.getCarbonTable(None, "source")(sqlContext.sparkSession)
    val folder = FileFactory.getCarbonFile(table.getTablePath)
    val files = folder.listFiles(true)
    import scala.collection.JavaConverters._
    val dataFiles = files.asScala.filter(_.getName.endsWith(CarbonTablePath.CARBON_DATA_EXT))
    dataFiles.foreach { dataFile =>
      val fileReader = FileFactory
        .getFileHolder(FileFactory.getFileType(dataFile.getPath))
      val buffer = fileReader
        .readByteBuffer(FileFactory.getUpdatedFilePath(dataFile.getPath), dataFile.getSize - 8, 8)
      val footerReader = new CarbonFooterReaderV3(dataFile.getAbsolutePath, buffer.getLong)
      val footer = footerReader.readFooterVersion3
      assertResult(2)(footer.blocklet_index_list.size)
      assertResult(2)(footer.blocklet_info_list3.size)
    }
    sql("drop table source")
  }

  test("test create table with invalid blocklet size") {
    val ex = intercept[MalformedCarbonCommandException] {
      sql("CREATE TABLE T1(name String) STORED AS CARBONDATA TBLPROPERTIES('TABLE_BLOCKLET_SIZE'='3X')")
    }
    assert(ex.getMessage.toLowerCase.contains("invalid table_blocklet_size"))
  }

  override def afterAll {
    sql("use default")
    sql("drop table if exists source")
  }

}
