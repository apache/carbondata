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

package com.huawei.datasight.test

import org.apache.spark.sql.OlapContext
import org.apache.spark.SparkConf
import java.util.Properties
import java.io.FileInputStream
import org.apache.spark.SparkContext
import com.huawei.unibi.molap.util.MolapProperties
import collection.JavaConversions._
import org.apache.spark.sql.cubemodel.Partitioner
import scala.io.Source._
import java.text.MessageFormat
import org.apache.spark.sql.CarbonEnv
import java.io.File
import org.apache.spark.sql.hive.HiveContext
import scala.util.control.Exception
import com.huawei.datasight.xml.XmlReportWriter

object StartTest {

  def main(args: Array[String]) {
    val reportWriter = new XmlReportWriter();
    val conf: String = args(0);

    val file: File = new File(conf)
    val basePath: String = file.getParentFile.getAbsolutePath
    val prop = new ScalaProp()
    try {
      prop.load(new FileInputStream(file))
    } catch {
      case e: Exception =>
        print(s"unable to load $conf")
    }
    prepareTest(basePath, prop)
    val resultFile: String = MessageFormat.format(prop.getProperty("molap.test.result"), basePath)
    val pw = new java.io.PrintWriter(new File(resultFile))
    var sparkConf = new SparkConf().setAppName("HiveFromSpark").setMaster("local").set("spark.sql.dialect", "hiveql")
    sparkConf.set("carbon.storelocation", prop.getProperty("carbon.storelocation"))
    sparkConf.set("molap.kettle.home", prop.getProperty("molap.kettle.home"))
    sparkConf.set("molap.is.columnar.storage", "true")
    sparkConf.set("spark.sql.huawei.register.dialect", "org.apache.spark.sql.MolapSqlParser")
    sparkConf.set("spark.sql.huawei.register.strategyRule", "org.apache.spark.sql.hive.CarbonStrategy")
    sparkConf.set("spark.sql.huawei.initFunction", "org.apache.spark.sql.CarbonEnv")
    sparkConf.set("spark.sql.huawei.acl.enable", "false")
    sparkConf.set("molap.tempstore.location", prop.getProperty("molap.tempstore.location"))
    val confKeys = prop.keySet.toList.map(_.toString).filter(_.startsWith("sparkconf."))
    confKeys.foreach(x => sparkConf.set(x.stripPrefix("sparkconf."), prop.getProperty(x)))
    sparkConf.setAppName("Functional Test Suite")
    MolapProperties.getInstance.addProperty("molap.kettle.home", prop.getProperty("molap.kettle.home"))
    val sc = new SparkContext(sparkConf);
    //val hc = new HiveContext(sc);
    val storeLocation=MessageFormat.format(prop.getProperty("carbon.storelocation"), basePath)
    val hc = new OlapContext(sc,storeLocation)
    val warmUpTime = MolapProperties.getInstance().getProperty("molap.spark.warmUpTime", prop.getProperty("molap.spark.warmUpTime"))
    println("Sleeping for millisecs:" + warmUpTime);
    try {
      Thread.sleep(Integer.parseInt(warmUpTime));
    } catch {
      case _ => { println("Wrong value for molap.spark.warmUpTime " + warmUpTime + "Using default Value and proceeding"); Thread.sleep(30000); }
    }

    val sqlFilePath: String = MessageFormat.format(prop.getProperty("molap.test.sqlFile"), basePath)
    sqlFilePath.split(",").foreach(readFileAndExec(_, hc, pw, reportWriter))
    pw.close
    reportWriter.flushToResultFile()
    hc.clearCache();

    sc.cancelAllJobs();
    sc.stop()

  }
  def readFileAndExec(fileName: String, hc: HiveContext, pw: java.io.PrintWriter, reportWriter: XmlReportWriter): Unit = {
    fromFile(fileName).getLines.filterNot(_.trim.startsWith("//")).foreach(_.split(";").filterNot(_.trim.equals("")).foreach(processCommand(_, hc, pw, reportWriter)))
  }
  def test(z: String) {
    println(z)
  }
  def processCommand(cmd: String, hc: HiveContext, pw: java.io.PrintWriter, reportWriter: XmlReportWriter): Unit = {
    if (!cmd.startsWith("#")) {
      try {

        println(s"executing>>>>>>$cmd")
        val result = hc.sql(cmd)
        result.show(100)
        println(s"executed>>>>>>$cmd")
        pw.write(s"$cmd  | Success \r\n")
        reportWriter.writePASS("1", s"$cmd")
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
          pw.write(s"$cmd | Failed \r\n")
          reportWriter.writeFAIL("1", s"$cmd", ex.getMessage)
        }

      }

    }
  }

  def prepareTest(basePath: String, prop: Properties) {
    // remove the store
    //    if(prop.get("olap.loaddata").equals("true"))
    //    {
    //        val fileName = MessageFormat.format(prop.getProperty("spark.store.path"),basePath);
    //        println(s"removing exising store content from $fileName")
    //        val f = new File(fileName)
    //        delete(f);
    //    }
    val resultFile = MessageFormat.format(prop.getProperty("molap.test.result"), basePath);
    println(s"removing exising $resultFile")
    val resultf = new File(resultFile)
    if (resultf.exists())
      resultf.delete()
  }
  def delete(file: File) {
    if (file.isDirectory() && file.listFiles().length > 0) {
      val files = file.listFiles()
      if (files.length > 0) files.foreach { delete(_) }
      file.delete();
    } else {
      file.delete();
    }
  }

  def filterComments() {
    val removedComments = fromFile("test.txt").getLines.reduce(
      (x, y) =>
        y match {
          case y if y.trim().startsWith("//") => x
          case _                              => x + "\n" + y
        })
  }
}

class ScalaProp extends java.util.Properties {

  private def get(key: String): Option[String] = {
    val value = getProperty(key)
    if (value != null) {
      Some(value)
    } else {
      None
    }
  }
  def getPropOrEmpty(key: String): String = {
    get(key) match {
      case Some(v) => v
      case None    => ""
    }
  }
  def getPropOrNull(key: String): String = {
    get(key) match {
      case Some(v) => v
      case None    => null
    }
  }
}