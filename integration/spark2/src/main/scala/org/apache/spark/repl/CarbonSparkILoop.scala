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

  override def initializeSpark() {
    intp.beQuietDuring {
      command("""
         if(org.apache.spark.repl.carbon.Main.interp == null) {
           org.apache.spark.repl.carbon.Main.main(Array[String]())
         }
              """)
      command("val i1 = org.apache.spark.repl.carbon.Main.interp")
      command("import i1._")
      command("""
         @transient val sc = {
           val _sc = i1.createSparkContext()
           println("Spark context available as sc.")
           _sc
         }
              """)
      command("import org.apache.spark.SparkContext._")
      command("import org.apache.spark.sql.CarbonContext")
      command("""
         @transient val cc = {
           val _cc = {
             import java.io.File
             val path = System.getenv("CARBON_HOME") + "/bin/carbonshellstore"
             val store = new File(path)
             store.mkdirs()
             val storePath = sc.getConf.getOption("spark.carbon.storepath")
                  .getOrElse(store.getCanonicalPath)
             new CarbonContext(sc, storePath, store.getCanonicalPath)
           }
           println("Carbon context available as cc.")
           _cc
         }
              """)

      command("import org.apache.spark.sql.SQLContext")
      command("""
         @transient val sqlContext = {
           val _sqlContext = new SQLContext(sc)
           println("SQL context available as sqlContext.")
           _sqlContext
         }
              """)
      command("import sqlContext.implicits._")
      command("import sqlContext.sql")

      command("import cc.implicits._")
      command("import cc.sql")
      command("import org.apache.spark.sql.functions._")
    }
  }
}
