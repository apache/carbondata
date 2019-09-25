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

package org.apache.spark.sql.profiler

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class ProfilerSuite extends CarbonQueryTest with BeforeAndAfterAll {
  var setupEndpointRef: RpcEndpointRef = _
  var statementMessages: ArrayBuffer[ProfilerMessage] = _
  var executionMessages: ArrayBuffer[ProfilerMessage] = _
  val profilerEndPoint = new ProfilerEndPoint
  val listener = new ProfilerListener

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
      "true"
    )
    Profiler.setIsEnable(true)
    setupEndpointRef = SparkEnv.get.rpcEnv.setupEndpoint(
      "CarbonProfiler",
      new ProfilerEndPoint() {
        // override method for testing
        override def processSQLStart(statementId: Long,
            messages: ArrayBuffer[ProfilerMessage]): Unit = {
          statementMessages = messages
        }
        // override method for testing
        override def processExecutionEnd(executionId: Long,
            messages: ArrayBuffer[ProfilerMessage]): Unit = {
          executionMessages = messages
        }
      })
    sqlContext.sparkContext.addSparkListener(listener)
  }

  def cleanMessages(): Unit = {
    statementMessages = null
    executionMessages = null
  }

  def processSQLStart(statementId: Long,
      messages: ArrayBuffer[ProfilerMessage]): Unit = {
    try {
      profilerEndPoint.processSQLStart(statementId, messages)
    } catch {
      case _: Throwable =>
        assert(false, "Failed to log StatementSummary")
    }
  }

  def processExecutionEnd(executionId: Long,
      messages: ArrayBuffer[ProfilerMessage]): Unit = {
    try {
      profilerEndPoint.processExecutionEnd(executionId, messages)
    } catch {
      case _: Throwable =>
        assert(false, "Failed to log ExecutionSummary")
    }
  }

  def checkCommand(sqlText: String): Unit = {
    sql(sqlText)
    Thread.sleep(1000)
    assertResult(1)(statementMessages.length)
    assert(statementMessages(0).isInstanceOf[SQLStart])
    val dropSQLStart = statementMessages(0).asInstanceOf[SQLStart]
    assertResult(sqlText)(dropSQLStart.sqlText)
    processSQLStart(dropSQLStart.statementId, statementMessages)
    cleanMessages()
  }

  def checkSelectQuery(
      sqlText: String,
      executionMsgCount: Int
  ): Unit = {
    sql(sqlText).collect()
    Thread.sleep(1000)
    assertResult(2)(statementMessages.length)
    assert(statementMessages(0).isInstanceOf[SQLStart])
    assert(statementMessages(1).isInstanceOf[Optimizer])
    val sqlStart = statementMessages(0).asInstanceOf[SQLStart]
    assertResult(sqlText)(sqlStart.sqlText)
    processSQLStart(sqlStart.statementId, statementMessages)
    assertResult(executionMsgCount)(executionMessages.length)
    assert(executionMessages(0).isInstanceOf[ExecutionStart])
    assert(executionMessages.exists(_.isInstanceOf[GetPartition]))
    assert(executionMessages.exists(_.isInstanceOf[QueryTaskEnd]))
    assert(executionMessages(executionMsgCount - 1).isInstanceOf[ExecutionEnd])
    val executionStart = executionMessages(0).asInstanceOf[ExecutionStart]
    processExecutionEnd(executionStart.executionId, executionMessages)
    cleanMessages()
  }

  ignore("collect messages to driver side") {
    // drop table
    checkCommand("DROP TABLE IF EXISTS mobile")
    checkCommand("DROP TABLE IF EXISTS emp")
    // create table
    checkCommand("CREATE TABLE mobile (mid string,mobileId string, color string, id int) STORED BY 'carbondata' TBLPROPERTIES('DICTIONARY_EXCLUDE'='Color')")
    checkCommand("CREATE TABLE emp (eid string,eName string, mobileId string,color string, id int) STORED BY 'carbondata' TBLPROPERTIES('DICTIONARY_EXCLUDE'='Color')")
    // load data
    checkCommand(s"LOAD DATA LOCAL INPATH '$resourcesPath/join/mobile.csv' INTO TABLE mobile OPTIONS('FILEHEADER'='mid,mobileId,color,id')")
    checkCommand(s"LOAD DATA LOCAL INPATH '$resourcesPath/join/employee.csv' INTO TABLE emp OPTIONS('FILEHEADER'='eid,eName,mobileId,color,id')")
    // select query
    checkSelectQuery("SELECT * FROM mobile where mid = 'mid89'", 4)
    checkSelectQuery("SELECT * FROM emp where eid = 'empid292'", 4)
    checkSelectQuery("SELECT * FROM emp JOIN mobile ON emp.mobileId=mobile.mobileId", 6)
    // drop table
    checkCommand("DROP TABLE IF EXISTS mobile")
    checkCommand("DROP TABLE IF EXISTS emp")
  }

  override def afterAll(): Unit = {
    SparkEnv.get.rpcEnv.stop(setupEndpointRef)
    sqlContext.sparkContext.listenerBus.removeListener(listener)
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.ENABLE_QUERY_STATISTICS,
      CarbonCommonConstants.ENABLE_QUERY_STATISTICS_DEFAULT
    )
    Profiler.setIsEnable(false)
    sql("DROP TABLE IF EXISTS mobile")
    sql("DROP TABLE IF EXISTS emp")
  }

}
