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

package org.apache.carbondata.events

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 * Example for event listener
 */
object EventListenerExample extends App {

  ListenerBus.getInstance()
    .addListener(LoadTablePreExecutionEvent.getClass.getName, new EventListener {

      override def onEvent(event: Event): Unit = {
        // write your logic here
      }
    })

  import org.apache.spark.sql.CarbonSession._

  val rootPath = new File(this.getClass.getResource("/").getPath
                          + "../../../..").getCanonicalPath
  val storeLocation = s"$rootPath/examples/spark2/target/store"
  val warehouse = s"$rootPath/examples/spark2/target/warehouse"
  val sparkSession = SparkSession
    .builder()
    .master("local")
    .appName("CarbonSessionExample")
    .config("spark.sql.warehouse.dir", warehouse)
    .config("spark.driver.host", "localhost")
    .getOrCreateCarbonSession(storeLocation)
  val carbonTableIdentifier = new CarbonTableIdentifier("db1", "tbl1", "tbl1-id")
  val carbonLoadModel = new CarbonLoadModel
  private val loadTablePreExecutionEvent = LoadTablePreExecutionEvent(sparkSession,
    carbonTableIdentifier,
    carbonLoadModel)
  ListenerBus.getInstance().fireEvent(loadTablePreExecutionEvent)
}
