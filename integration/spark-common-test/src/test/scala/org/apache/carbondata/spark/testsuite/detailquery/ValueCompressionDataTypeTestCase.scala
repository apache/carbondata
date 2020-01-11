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
package org.apache.carbondata.spark.testsuite.detailquery

import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType
import org.apache.spark.sql.test.util.QueryTest

class ValueCompressionDataTypeTestCase extends QueryTest with BeforeAndAfterAll {
  val tempDirPath = s"$resourcesPath/tempdir"

  override def beforeAll {
    FileFactory.mkdirs(tempDirPath)
  }

  test("ActualDataType:double,ChangedDatatype:Short,CompressionType:NonDecimalMaxMin") {
    val tempFilePath = s"$tempDirPath/double2short.csv"
    try {
      sql("CREATE TABLE double2short (name String, value double) STORED AS carbondata")
      sql("CREATE TABLE double2short_hive (name String, value double)row format delimited fields terminated by ','")
      val data ="a,3.141111\nb,3.141212\nc,3.141313\nd,3.141515\ne,3.141616\nf,3.141616\ng,3.141717\nh,3.141818";
      writeData(tempFilePath, data)
      sql(s"LOAD data local inpath '${tempFilePath}' into table double2short options('fileheader'='name,value')")
      sql(s"LOAD data local inpath '${tempFilePath}' into table double2short_hive")
      checkAnswer(sql("select * from double2short"),
        sql("select * from double2short_hive"))

    } catch{
      case ex:Exception => ex.printStackTrace()
                           assert(false)
    } finally {
      sql("drop table if exists double2short")
      sql("drop table if exists double2short_hive")
      deleteFile(tempFilePath)
    }
  }

  test("ActualDataType:double,ChangedDatatype:byte,CompressionType:NonDecimalMaxMin") {
    val tempFilePath = s"$tempDirPath/double2byte.csv"
    try {
      sql("CREATE TABLE double2byte (name String, value double) STORED AS carbondata")
      sql("CREATE TABLE double2byte_hive (name String, value double)row format delimited fields terminated by ','")
      val data ="a,4.200001\nb,4.200009";
      writeData(tempFilePath, data)
      sql(s"LOAD data local inpath '${tempFilePath}' into table double2byte options('fileheader'='name,value')")
      sql(s"LOAD data local inpath '${tempFilePath}' into table double2byte_hive")
      checkAnswer(sql("select * from double2byte"),
        sql("select * from double2byte_hive"))

    } catch{
      case ex:Exception => ex.printStackTrace()
                           assert(false)
    } finally {
      sql("drop table if exists double2byte")
      sql("drop table if exists double2byte_hive")
      deleteFile(tempFilePath)
    }
  }

  test("When the values of Double datatype are negative values") {
    val tempFilePath = s"$tempDirPath/doubleISnegtive.csv"
    try {
      sql("drop table if exists doubleISnegtive")
      sql("drop table if exists doubleISnegtive_hive")
      sql("CREATE TABLE doubleISnegtive (name String, value double) STORED AS carbondata")
      sql("CREATE TABLE doubleISnegtive_hive (name String, value double)row format delimited fields terminated by ','")
      val data ="a,-7489.7976000000\nb,-11234567489.797\nc,-11234567489.7\nd,-1.2\ne,-2\nf,-11234567489.7976000000\ng,-11234567489.7976000000"
      writeData(tempFilePath, data)
      sql(s"LOAD data local inpath '${tempFilePath}' into table doubleISnegtive options('fileheader'='name,value')")
      sql(s"LOAD data local inpath '${tempFilePath}' into table doubleISnegtive_hive")

      checkAnswer(sql("select * from doubleISnegtive"),
        sql("select * from doubleISnegtive_hive"))
    } catch{
      case ex:Exception => ex.printStackTrace()
        assert(false)
    } finally {
      sql("drop table if exists doubleISnegtive")
      sql("drop table if exists doubleISnegtive_hive")
      deleteFile(tempFilePath)
    }
  }

  test("When the values of Double datatype have both postive and negative values") {
    val tempFilePath = s"$tempDirPath/doublePAN.csv"
    try {
      sql("drop table if exists doublePAN")
      sql("drop table if exists doublePAN_hive")
      sql("CREATE TABLE doublePAN (name String, value double) STORED AS carbondata")
      sql("CREATE TABLE doublePAN_hive (name String, value double)row format delimited fields terminated by ','")
      val data ="a,-7489.7976000000\nb,11234567489.797\nc,-11234567489.7\nd,-1.2\ne,2\nf,-11234567489.7976000000\ng,11234567489.7976000000"
      writeData(tempFilePath, data)
      sql(s"LOAD data local inpath '${tempFilePath}' into table doublePAN options('fileheader'='name,value')")
      sql(s"LOAD data local inpath '${tempFilePath}' into table doublePAN_hive")

      checkAnswer(sql("select * from doublePAN"),
        sql("select * from doublePAN_hive"))
    } catch{
      case ex:Exception => ex.printStackTrace()
        assert(false)
    } finally {
      sql("drop table if exists doublePAN")
      sql("drop table if exists doublePAN_hive")
      deleteFile(tempFilePath)
    }
  }

  def writeData(filePath: String, data: String) = {
    val dis = FileFactory.getDataOutputStream(filePath)
    dis.writeBytes(data.toString())
    dis.close()
  }

  def deleteFile(filePath: String) {
    val file = FileFactory.getCarbonFile(filePath)
    file.delete()
  }

  override def afterAll {
    deleteFile(tempDirPath)
  }
}