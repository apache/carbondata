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
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class TestCreateTableUsingCarbonFileLevelFormat extends QueryTest with BeforeAndAfterAll {

  var writerOutputFilePath1: String = _
  var writerOutputFilePath2: String = _
  var writerOutputFilePath3: String = _
  var writerOutputFilePath4: String = _
  var writerOutputFilePath5: String = _

  override def beforeAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
    // create carbon table and insert data
    writerOutputFilePath1 = new File(this.getClass.getResource("/").getPath
                                    +
                                    "../." +
                                    "./src/test/resources/carbonFileLevelFormat/WriterOutput1/Fact" +
                                    "/Part0/Segment_null/").getCanonicalPath
    //getCanonicalPath gives path with \, so code expects /. Need to handle in code ?
    writerOutputFilePath1 = writerOutputFilePath1.replace("\\", "/");


    writerOutputFilePath2 = new File(this.getClass.getResource("/").getPath
                                     +
                                     "../." +
                                     "./src/test/resources/carbonFileLevelFormat/WriterOutput2/Fact" +
                                     "/Part0/Segment_null/").getCanonicalPath
    //getCanonicalPath gives path with \, so code expects /. Need to handle in code ?
    writerOutputFilePath2 = writerOutputFilePath2.replace("\\", "/");

    writerOutputFilePath3 = new File(this.getClass.getResource("/").getPath
                                     +
                                     "../." +
                                     "./src/test/resources/carbonFileLevelFormat/WriterOutput3/Fact" +
                                     "/Part0/Segment_null/").getCanonicalPath
    //getCanonicalPath gives path with \, so code expects /. Need to handle in code ?
    writerOutputFilePath3 = writerOutputFilePath3.replace("\\", "/");

    writerOutputFilePath4 = new File(this.getClass.getResource("/").getPath
                                     +
                                     "../." +
                                     "./src/test/resources/carbonFileLevelFormat/WriterOutput4/Fact" +
                                     "/Part0/Segment_null/").getCanonicalPath
    //getCanonicalPath gives path with \, so code expects /. Need to handle in code ?
    writerOutputFilePath4 = writerOutputFilePath4.replace("\\", "/");


    writerOutputFilePath5 = new File(this.getClass.getResource("/").getPath
                                     +
                                     "../." +
                                     "./src/test/resources/carbonFileLevelFormat/WriterOutput5/Fact" +
                                     "/Part0/Segment_null/").getCanonicalPath
    //getCanonicalPath gives path with \, so code expects /. Need to handle in code ?
    writerOutputFilePath5 = writerOutputFilePath5.replace("\\", "/");


  }

  override def afterAll(): Unit = {
    sql("DROP TABLE IF EXISTS sdkOutputTable")
  }


  //TO DO, need to remove segment dependency and tableIdentifier Dependency
  test("read multiple carbondata files (sdk Writer Output) using the CarbonFileLevelFormat ") {
    assert(new File(writerOutputFilePath1).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    sql(s"""CREATE TABLE sdkOutputTable USING CarbonDataFileFormat LOCATION '$writerOutputFilePath1' """)

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
    assert(new File(writerOutputFilePath1).exists())
  }


  test("should not allow to alter datasource carbontable ") {
    assert(new File(writerOutputFilePath1).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    sql(
      s"""CREATE TABLE sdkOutputTable USING CarbonDataFileFormat LOCATION
         |'$writerOutputFilePath1' """.stripMargin)

    val exception = intercept[MalformedCarbonCommandException]
      {
        sql("Alter table sdkOutputTable change age age BIGINT")
      }
    assert(exception.getMessage().contains("Unsupported alter operation on hive table"))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerOutputFilePath1).exists())
  }


  test("Read sdk writer output file without index file should fail") {
    assert(new File(writerOutputFilePath2).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    sql(
      s"""CREATE TABLE sdkOutputTable USING CarbonDataFileFormat LOCATION
         |'$writerOutputFilePath2' """.stripMargin)

    //org.apache.spark.SparkException: Index file not present to read the carbondata file
    val exception = intercept[org.apache.spark.SparkException]
    {
      sql("select * from sdkOutputTable").show(false)
    }
    assert(exception.getMessage().contains("Index file not present to read the carbondata file"))

    sql("DROP TABLE sdkOutputTable")
    // drop table should not delete the files
    assert(new File(writerOutputFilePath2).exists())
  }


  test("Read sdk writer output file without Carbondata file should fail") {
    assert(new File(writerOutputFilePath3).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    val exception = intercept[org.apache.spark.SparkException] {
      //    data source file format
      sql(
        s"""CREATE TABLE sdkOutputTable USING CarbonDataFileFormat LOCATION
           |'$writerOutputFilePath3' """.stripMargin)
    }
    assert(exception.getMessage()
      .contains("CarbonData file is not present in the location mentioned in DDL"))

    // drop table should not delete the files
    assert(new File(writerOutputFilePath3).exists())
  }


  test("Read sdk writer output file without any file should fail") {
    assert(new File(writerOutputFilePath4).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    val exception = intercept[org.apache.spark.SparkException] {
      //data source file format
      sql(
        s"""CREATE TABLE sdkOutputTable USING CarbonDataFileFormat LOCATION
           |'$writerOutputFilePath4' """.stripMargin)

      sql("select * from sdkOutputTable").show(false)
    }
    assert(exception.getMessage()
      .contains("CarbonData file is not present in the location mentioned in DDL"))

    // drop table should not delete the files
    assert(new File(writerOutputFilePath4).exists())
  }

  test("Read sdk writer output file withSchema") {
    assert(new File(writerOutputFilePath5).exists())
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    sql("DROP TABLE IF EXISTS sdkOutputTable")

    //data source file format
    sql(
      s"""CREATE TABLE sdkOutputTable USING CarbonDataFileFormat LOCATION
         |'$writerOutputFilePath5' """.stripMargin)

    sql("Describe formatted sdkOutputTable").show(false)

    sql("select * from sdkOutputTable").show(false)

    sql("select * from sdkOutputTable limit 3").show(false)

    sql("select name from sdkOutputTable").show(false)

    sql("select age from sdkOutputTable").show(false)

    sql("select * from sdkOutputTable where age > 2 and age < 8").show(200, false)

    sql("select * from sdkOutputTable where name = 'robot3'").show(200, false)

    sql("select * from sdkOutputTable where name like 'robo%' limit 5").show(200, false)

    sql("select * from sdkOutputTable where name like '%obot%' limit 2").show(200, false)

    sql("select sum(age) from sdkOutputTable where name like 'robot1%' ").show(200, false)

    sql("select count(*) from sdkOutputTable where name like 'robot%' ").show(200, false)

    sql("select count(*) from sdkOutputTable").show(200, false)

    sql("DROP TABLE sdkOutputTable")

    // drop table should not delete the files
    assert(new File(writerOutputFilePath5).exists())
  }

}
