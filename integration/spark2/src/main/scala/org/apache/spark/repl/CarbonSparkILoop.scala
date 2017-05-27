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

package org.apache.spark.repl

class CarbonSparkILoop extends SparkILoop {

  private def initOriginSpark(): Unit = {
    processLine("""
        @transient val spark = if (org.apache.spark.repl.Main.sparkSession != null) {
            org.apache.spark.repl.Main.sparkSession
          } else {
            org.apache.spark.repl.Main.createSparkSession()
          }
        @transient val sc = {
          val _sc = spark.sparkContext
          if (_sc.getConf.getBoolean("spark.ui.reverseProxy", false)) {
            val proxyUrl = _sc.getConf.get("spark.ui.reverseProxyUrl", null)
            if (proxyUrl != null) {
              println(s"Spark Context Web UI is available at " +
                s"${proxyUrl}/proxy/${_sc.applicationId}")
            } else {
              println(s"Spark Context Web UI is available at Spark Master Public URL")
            }
          } else {
            _sc.uiWebUrl.foreach {
              webUrl => println(s"Spark context Web UI available at ${webUrl}")
            }
          }
          println("Spark context available as 'sc' " +
            s"(master = ${_sc.master}, app id = ${_sc.applicationId}).")
          println("Spark session available as 'spark'.")
          _sc
        }
        """)
    processLine("import org.apache.spark.SparkContext._")
    processLine("import spark.implicits._")
    processLine("import spark.sql")
    processLine("import org.apache.spark.sql.functions._")
  }

  private def initCarbon(): Unit = {
    processLine("""
      import org.apache.spark.sql.SparkSession
      import org.apache.spark.sql.CarbonSession._
      @transient val carbon = {
        val _carbon = {
          import java.io.File
          val path = System.getenv("CARBON_HOME") + "/bin/carbonshellstore"
          val store = new File(path)
          store.mkdirs()
          val storePath = sc.getConf.getOption("spark.carbon.storepath")
                 .getOrElse(store.getCanonicalPath)
          SparkSession.builder().config(sc.getConf).getOrCreateCarbonSession(storePath)
        }
        println("Carbon session available as carbon.")
        _carbon
      }
      """)

    processLine("import carbon.implicits._")
    processLine("import carbon.sql")
  }
  override def initializeSpark() {
    intp.beQuietDuring {
      initOriginSpark()
      initCarbon()
    }
  }
}
