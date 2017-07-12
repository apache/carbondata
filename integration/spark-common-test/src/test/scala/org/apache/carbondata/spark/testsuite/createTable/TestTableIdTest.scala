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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.junit.Assert
import org.scalatest.BeforeAndAfterAll
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.hadoop.CarbonInputFormat
import org.apache.spark.sql.test.util.QueryTest

/**
 * test functionality related the case change for database name
 */
class TestTableIdTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
    sql("drop table if exists carbontable")
  }

  def validateTableId: Unit = {
    val carbonInputFormat: CarbonInputFormat[Array[Object]] = new CarbonInputFormat[Array[Object]]
    val jobConf: JobConf = new JobConf(new Configuration)
    val job: Job = Job.getInstance(jobConf)
    val storePath: String = storeLocation.replaceAll("\\\\", "/")
    job.getConfiguration
      .set("mapreduce.input.fileinputformat.inputdir",
        storePath + "/default/carbontable")
    val carbonTable: CarbonTable = CarbonInputFormat.getCarbonTable(job.getConfiguration)
    val getAbsoluteTableIdentifier = classOf[CarbonInputFormat[Array[Object]]]
      .getDeclaredMethod("getAbsoluteTableIdentifier", classOf[Configuration])
    getAbsoluteTableIdentifier.setAccessible(true)
    val absoluteTableIdentifier: AbsoluteTableIdentifier = getAbsoluteTableIdentifier
      .invoke(carbonInputFormat, job.getConfiguration).asInstanceOf[AbsoluteTableIdentifier]

    Assert
      .assertEquals(carbonTable.getCarbonTableIdentifier.getTableId,
        absoluteTableIdentifier.getCarbonTableIdentifier.getTableId)
  }

  test("test create table with database case name change") {

    try {
      // table creation should be successful
      sql("create table carbontable(a int, b string)stored by 'carbondata'")
      assert(true)
    } catch {
      case ex: Exception =>
        assert(false)
    }
    validateTableId
  }

  override def afterAll {
    sql("drop table if exists carbontable")
  }
}
