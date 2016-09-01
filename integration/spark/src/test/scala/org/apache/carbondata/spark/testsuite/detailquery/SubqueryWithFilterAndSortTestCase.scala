/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.detailquery

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.datastorage.store.impl.FileFactory.FileType

class SubqueryWithFilterAndSortTestCase extends QueryTest with BeforeAndAfterAll {
  val tempDirPath = "./src/test/resources/temp"
  val tempFilePath = "./src/test/resources/temp/subqueryfilterwithsort.csv"

  override def beforeAll {
    FileFactory.mkdirs(tempDirPath,FileType.LOCAL)
    sql("drop table if exists subqueryfilterwithsort")
    sql("drop table if exists subqueryfilterwithsort_hive")
    sql("CREATE TABLE subqueryfilterwithsort (name String, id int) STORED BY 'org.apache.carbondata.format'")
    sql("CREATE TABLE subqueryfilterwithsort_hive (name String, id int)row format delimited fields terminated by ','")
    val data ="name_a,1\nname_b,2\nname_c,3\nname_d,4\nname_e,5\nname_f,6"
    writedata(tempFilePath, data)
    sql(s"LOAD data local inpath '${tempFilePath}' into table subqueryfilterwithsort options('fileheader'='name,id')")
    sql(s"LOAD data local inpath '${tempFilePath}' into table subqueryfilterwithsort_hive")
  }

  test("When the query has sub-query with sort and has '=' filter") {
    try {
      checkAnswer(sql("select name,id from (select * from subqueryfilterwithsort order by id)t where name='name_c' "),
        sql("select name,id from (select * from subqueryfilterwithsort_hive order by id)t where name='name_c'"))
    } catch{
      case ex:Exception => ex.printStackTrace()
        assert(false)
    }
  }

  test("When the query has sub-query with sort and has 'like' filter") {
    try {
      checkAnswer(sql("select name,id from (select * from subqueryfilterwithsort order by id)t where name like 'name%' "),
        sql("select name,id from (select * from subqueryfilterwithsort_hive order by id)t where name like 'name%'"))
    } catch{
      case ex:Exception => ex.printStackTrace()
        assert(false)
    }
  }

  def writedata(filePath: String, data: String) = {
    val dis = FileFactory.getDataOutputStream(filePath, FileFactory.getFileType(filePath))
    dis.writeBytes(data.toString())
    dis.close()
  }
  def deleteFile(filePath: String) {
    val file = FileFactory.getCarbonFile(filePath, FileFactory.getFileType(filePath))
    file.delete()
  }

  override def afterAll {
    sql("drop table if exists subqueryfilterwithsort")
    sql("drop table if exists subqueryfilterwithsort_hive")
    deleteFile(tempFilePath)
    deleteFile(tempDirPath)
  }

}
