
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
package org.apache.carbondata.spark.testsuite.dataload


import java.io.File

import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class TestDataLoadWithComplexType extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("DROP TABLE IF EXISTS structinstruct")
  }


  test(" select * from structinstruct") {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    sql(
      "create table structinstruct(id int, structelem struct<id1:int, structelem: struct<id2:int, name:string>>)stored by 'carbondata'".stripMargin)

    val path = s"$rootPath/examples/spark2/src/main/resources/structinstructnull.csv"

    sql(s"load data inpath '$path' into table structinstruct options('delimiter'=',' , 'fileheader'='id,structelem','COMPLEX_DELIMITER_LEVEL_1'='#', 'COMPLEX_DELIMITER_LEVEL_2'='|')")

    checkExistence(sql(
      s"""
         select * from structinstruct
       """.stripMargin), true , "1[111,[1001,abc]]2[222,[2002,xyz]]null[333,[3003,def]]4[null,[4004,pqr]]5[555,[null,ghi]]6[666,[6006,null]]7[null,[null,null]]")
  }

  override def afterAll {
    sql("DROP TABLE IF EXISTS structinstruct")

  }



}

