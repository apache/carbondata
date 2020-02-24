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

package org.apache.spark.util

import java.util.Properties

import scala.collection.{immutable, mutable}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * load data api
 */
// scalastyle:off
object TableLoader {

  def extractOptions(propertiesFile: String): immutable.Map[String, String] = {
    val props = new Properties
    val path = new Path(propertiesFile)
    val fs = path.getFileSystem(FileFactory.getConfiguration)
    props.load(fs.open(path))
    val elments = props.entrySet().iterator()
    val map = new mutable.HashMap[String, String]()
    System.out.println("properties file:")
    while (elments.hasNext) {
      val elment = elments.next()
      System.out.println(s"${elment.getKey}=${elment.getValue}")
      map.put(elment.getKey.asInstanceOf[String], elment.getValue.asInstanceOf[String])
    }

    immutable.Map(map.toSeq: _*)
  }

  def extractStorePath(map: immutable.Map[String, String]): String = {
    map.get(CarbonCommonConstants.STORE_LOCATION) match {
      case Some(path) => path
      case None => throw new Exception(s"${CarbonCommonConstants.STORE_LOCATION} can't be empty")
    }
  }

  def loadTable(spark: SparkSession, dbName: Option[String], tableName: String, inputPaths: String,
      options: scala.collection.immutable.Map[String, String]): Unit = {
    CarbonLoadDataCommand(dbName, tableName, inputPaths, Nil, options, false).run(spark)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: TableLoader <properties file> <table name> <input files>")
      System.exit(1)
    }
    System.out.println("parameter list:")
    args.foreach(System.out.println)
    val map = extractOptions(TableAPIUtil.escape(args(0)))
    val storePath = extractStorePath(map)
    System.out.println(s"${CarbonCommonConstants.STORE_LOCATION}:$storePath")
    val (dbName, tableName) = TableAPIUtil.parseSchemaName(TableAPIUtil.escape(args(1)))
    System.out.println(s"table name: $dbName.$tableName")
    val inputPaths = TableAPIUtil.escape(args(2))

    val spark = TableAPIUtil.spark(storePath, s"TableLoader: $dbName.$tableName")

    loadTable(spark, Option(dbName), tableName, inputPaths, map)
  }

}
