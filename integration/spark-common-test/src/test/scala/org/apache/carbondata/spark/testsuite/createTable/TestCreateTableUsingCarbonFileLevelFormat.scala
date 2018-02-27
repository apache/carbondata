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

import java.io.File

import org.apache.spark.sql.{AnalysisException, CarbonEnv}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestCreateTableUsingCarbonFileLevelFormat extends QueryTest with BeforeAndAfterAll {

  var writerOutputFilePath: String = _

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    // create carbon table and insert data
    writerOutputFilePath = new File(this.getClass.getResource("/").getPath
                                    +
                                    "../." +
                                    "./src/test/resources/carbonFileLevelFormat/WriterOutput/Fact" +
                                    "/Part0/Segment_null/").getCanonicalPath
    //getCanonicalPath gives path with \, so code expects /. Need to handle in code ?
    writerOutputFilePath = writerOutputFilePath.replace("\\", "/");


  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }

  //TO DO, need to remove segment dependency and tableIdentifier Dependency
  test("read the sdk Writer Output file using the CarbonFileLevelFormat") {
    assert(new File(writerOutputFilePath).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    sql(s"""CREATE TABLE sdkOutputTable USING CarbonDataFileFormat LOCATION '$writerOutputFilePath' """)

    sql("Describe formatted sdkOutputTable").show(false)

    sql("select * from sdkOutputTable").show(false)

    sql("select * from sdkOutputTable limit 3").show(false)

    sql("select name from sdkOutputTable").show(false)

    sql("select age from sdkOutputTable").show(false)

    sql("select * from sdkOutputTable where age > 2 and age < 8").show(200,false)

    sql("select * from sdkOutputTable where name = 'robot3'").show(200,false)

    sql("select * from sdkOutputTable where name like 'robo%' limit 5").show(200,false)

    sql("select * from sdkOutputTable where name like '%obot%' limit 2").show(200,false)

    sql("select sum(age) from sdkOutputTable where name like 'robot1%' ").show(200,false)

    sql("select count(*) from sdkOutputTable where name like 'robot%' ").show(200,false)

    sql("select count(*) from sdkOutputTable").show(200,false)

    sql("DROP TABLE sdkOutputTable")

    // drop table should not delete the files
    assert(new File(writerOutputFilePath).exists())
  }
}
