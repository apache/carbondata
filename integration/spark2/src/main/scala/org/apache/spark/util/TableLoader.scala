package org.apache.spark.util

import java.io.FileInputStream
import java.util
import java.util.Properties

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.LoadTable

import scala.collection.immutable
import scala.collection.mutable

/**
  * Created by david on 16-11-26.
  */
object TableLoader {

  def extractOptions(propertiesFile: String): immutable.Map[String, String] = {
    val props = new Properties
    val path = new Path(propertiesFile)
    val fs = path.getFileSystem(new Configuration())
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

  def parseDbAndTableName(tableName: String): (String, String) = {
    if (tableName.contains(".")) {
      val parts = tableName.split(".")
      (parts(0), parts(1))
    } else {
      ("default", tableName)
    }
  }

  def escape(str: String): String = {
    val newStr = str.trim
    if (newStr.startsWith("\"") && newStr.endsWith("\"")) {
      newStr.substring(1, newStr.length - 1)
    } else {
      str
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: TableLoader <properties file> <table name> <input files>");
      System.exit(1)
    }
    System.out.println("parameter list:")
    args.foreach(System.out.println(_))
    val map = extractOptions(escape(args(0)))
    val storePath = extractStorePath(map)
    System.out.println(s"${CarbonCommonConstants.STORE_LOCATION}:$storePath")
    val (dbName, tableName) = parseDbAndTableName(args(1))
    System.out.println(s"table name: $dbName.$tableName")
    val inputPaths = escape(args(2))

    val kettleHome = CarbonProperties.getInstance().getProperty("carbon.kettle.home")
    if (kettleHome == null) {
      CarbonProperties.getInstance().addProperty("carbon.kettle.home",
        map.getOrElse("carbon.kettle.home", ""))
    }

    val spark = SparkSession
        .builder
        .appName(s"Table Loader: $dbName.$tableName")
        .master("local")
        .config(CarbonCommonConstants.STORE_LOCATION, storePath)
        .getOrCreate()

    CarbonEnv.init(spark.sqlContext)

    LoadTable(Option(dbName), tableName, inputPaths, Nil, map).run(spark)
  }

}
