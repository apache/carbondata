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

package org.apache.carbondata.benchmark

import java.io.{File, FileInputStream, FileNotFoundException}
import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer
import scala.util.Random

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.examples.util.ExampleUtils

/**
 * It outputs the benchmark performance of IUD
 */
// scalastyle:off println
object UpdateBenchmark {
  object OptType extends Enumeration {
    type OptType = Value
    val Create = Value("create").toString
    val Update = Value("update").toString
  }

  val schema = StructType(
    Seq(
      StructField("intKey", IntegerType, nullable = false),
      StructField("randLong", LongType, nullable = false),
      StructField("randDouble", DoubleType, nullable = false),
      StructField("randFloat", FloatType, nullable = false),
      StructField("randString", StringType, nullable = false),
      StructField("partitionColumn", IntegerType, nullable = false)
    )
  )

  def usage(): Unit = {
    System.out.println("+-------------------------+--------------------------------------------+")
    System.out.println("| Command                 | Description                                |")
    System.out.println("+-------------------------+--------------------------------------------+")
    System.out.println("| Create <PropertiesPath> | Create Table                               |")
    System.out.println("| Update <PropertiesPath> | Update Table                               |")
    System.out.println("+------------------   ----+--------------------------------------------+")
  }


  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 1) {
      System.err.println("Illegal input..")
      usage()
      System.exit(0)
    }

    val spark = if (ToolProperties.runMode.equalsIgnoreCase("local")) {
      ExampleUtils.createSparkSession("UpdateBenchmark")
    } else {
      SparkSession.builder().appName("UpdateBenchmark")
        .enableHiveSupport().getOrCreate()
    }

    val command = args(0).toString
    command match {
      case OptType.Create =>
        createTable(spark)
      case OptType.Update =>
        updateTable(spark)
      case _ =>
        System.err.println("Unknown command: " + command)
        usage()
    }
  }

  def createTable(spark: SparkSession): Unit = {

    spark.sqlContext.sql(
      s"""drop table if exists ${ToolProperties.dbName}.${ToolProperties.targetTableName}""")
    spark.sqlContext.sql(
      s"""drop table if exists ${ToolProperties.dbName}.${ToolProperties.changeTableName}""")

    val rowsPerTask = ToolProperties.rowCount / ToolProperties.loadParallelize
    val inputRDD = spark.sparkContext.parallelize(0 until ToolProperties.loadParallelize,
      ToolProperties.loadParallelize).flatMap { index =>
      val base = new ListBuffer[Row]()
      val startRowId = index * rowsPerTask
      /* now we want to generate a loop and save the carbondata data */
      for (a <- 0L until rowsPerTask) {
        base += Row(startRowId + a.intValue(),
          UpdateDataGenerator.getNextLong,
          UpdateDataGenerator.getNextDouble,
          UpdateDataGenerator.getNextFloat,
          UpdateDataGenerator.getNextString(ToolProperties.variableSize,
            ToolProperties.affixRandom),
          UpdateDataGenerator.getNextInt(ToolProperties.partitionNumber))
      }
      base
    }
    val df = spark.createDataFrame(inputRDD, schema)
    if (ToolProperties.partitionNumber <= 1) df.write
      .format(ToolProperties.format)
      .option("dbName", ToolProperties.dbName)
      .option("tableName", ToolProperties.targetTableName)
      .mode(SaveMode.Overwrite)
      .save() else df.write
        .format(ToolProperties.format)
        .option("dbName", ToolProperties.dbName)
        .option("tableName", ToolProperties.targetTableName)
        .option("partitionColumns", "partitionColumn")
        .mode(SaveMode.Overwrite)
        .save()

    if (ToolProperties.fixChangeData) df.sample(ToolProperties.updateRate)
      .write
      .option("dbName", ToolProperties.dbName)
      .option("tableName", ToolProperties.changeTableName)
      .format(ToolProperties.format)
      .mode(SaveMode.Overwrite)
      .save()
  }

  def updateTable(spark: SparkSession): Unit = {
    Updater.spark = spark
    Updater.startUpdateTest()
  }
}


object UpdateDataGenerator extends Serializable {
  val random = new Random(System.nanoTime())

  /* affix payload variables */
  var affixStringBuilder: StringBuilder = null
  var affixByteArray: Array[Byte] = null

  def getNextString(size: Int, affix: Boolean): String = {
    if (affix) {
      synchronized{
        if (affixStringBuilder == null) {
          affixStringBuilder = new StringBuilder(
            random.alphanumeric.take(size).mkString)
        }
      }
      affixStringBuilder.setCharAt(random.nextInt(size),
        random.nextPrintableChar())
      affixStringBuilder.mkString
    } else {
      random.alphanumeric.take(size).mkString
    }
  }

  def getNextByteArray(size: Int, affix: Boolean): Array[Byte] = {
    val toReturn = new Array[Byte](size)
    if(!affix) {
      /* if not affix, then return completely new values in a new array */
      random.nextBytes(toReturn)
    } else {
      synchronized {
        if (affixByteArray == null) {
          affixByteArray = new Array[Byte](size)
          /* initialize */
          random.nextBytes(affixByteArray)
        }
      }
      /* just randomly change 1 byte - val is to make sure parquet
      * does not ignore the data - char will be casted to byte */
      affixByteArray(random.nextInt(size)) = random.nextPrintableChar().toByte
      /* now we copy affix array */
      Array.copy(affixByteArray, 0, toReturn, 0, size)
    }
    toReturn
  }

  def getNextInt: Int = {
    random.nextInt()
  }

  def getNextInt(max: Int): Int = {
    random.nextInt(max)
  }

  def getNextLong: Long = {
    random.nextLong()
  }

  def getNextDouble: Double = {
    random.nextDouble()
  }

  def getNextFloat: Float = {
    random.nextFloat()
  }

  def getNextValue(s: String, size: Int, affix: Boolean): String = {
    getNextString(size, affix)
  }

  def getNextValue(i: Int): Int = {
    getNextInt
  }

  def getNextValue(d: Double): Double = {
    getNextDouble
  }

  def getNextValue(l: Long): Long = {
    getNextLong
  }

  def getNextValueClass(cls: Any): Any = {
    val data = cls match {
      case _: String => UpdateDataGenerator.getNextString(10, false)
      case _ => throw new Exception("Data type not supported: ")
    }
    data
  }

  def getNextValuePrimitive(typeX: Class[_]): Any = {
    val data = typeX match {
      case java.lang.Integer.TYPE => UpdateDataGenerator.getNextInt
      case java.lang.Long.TYPE => UpdateDataGenerator.getNextLong
      case java.lang.Float.TYPE => UpdateDataGenerator.getNextFloat
      case java.lang.Double.TYPE => UpdateDataGenerator.getNextDouble
      case _ => println("others")
    }
    data
  }

  def getNextValue(typeX: Class[_]) : Any = {
    if (typeX.isPrimitive) {
      getNextValuePrimitive(typeX)
    } else {
      getNextValueClass(typeX.newInstance())
    }
  }
}

class Updater (private val spark: SparkSession) extends Thread{
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def run(): Unit = {
    for (i <- 0 until ToolProperties.updateTimes) {

      if (!ToolProperties.fixChangeData) {
        spark.sqlContext.sql(
          s"""drop table if exists ${ToolProperties.dbName}.${ToolProperties.changeTableName}""")
        val targetDataFrame = spark.sqlContext.sql(
          s"""select * from ${ToolProperties.dbName}.${ToolProperties.targetTableName}""")
        targetDataFrame.sample(ToolProperties.updateRate)
          .write
          .option("dbName", ToolProperties.dbName)
          .option("tableName", ToolProperties.changeTableName)
          .format(ToolProperties.format)
          .mode(SaveMode.Overwrite)
          .save()
      }

      val start = System.currentTimeMillis();
      try {
        spark.sqlContext.sql(
          s"""update ${ToolProperties.dbName}.${ToolProperties.targetTableName}
            | d set (randdouble,randfloat ) =
            | (select s.randdouble ,s.randfloat
            | from ${ToolProperties.dbName}.${ToolProperties.changeTableName}
            | s where d.intkey = s.intkey)
            | """.stripMargin).collect()
        Updater.sumTime.addAndGet(System.currentTimeMillis() - start);
        Updater.successCount.addAndGet(1);
      } catch {
        case e: Exception =>
          Updater.failedCount.addAndGet(1)
          LOGGER.warn("update failed!", e)
      }
    }
  }
}

object Updater {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  private val successCount: AtomicLong = new AtomicLong()

  private val failedCount: AtomicLong = new AtomicLong()

  private val lastSuccessCount: AtomicLong = new AtomicLong()

  private val sumTime: AtomicLong = new AtomicLong()

  private var maxLatency: Float = 0f

  var spark: SparkSession = null

  def startUpdateTest(): Unit = {
    val updater = new Updater(spark)
    updater.start()
    val printTime = 10 * 1000
    val startTime = System.currentTimeMillis
    var last = startTime
    while (true) {
      Thread.sleep(1000)
      val totalSuccess = successCount.get();
      val totalFail = failedCount.get()
      if (totalSuccess + totalFail >= ToolProperties.updateTimes) {
        val run = (System.currentTimeMillis - startTime) / 1000
        val avg = totalSuccess / run
        LOGGER.fatal("total success: " + totalSuccess + " runTime: " +
          run + "s average tps: " + avg + ", task termination!")
        System.exit(0)
      }
      val current = System.currentTimeMillis
      if ((current - last) >= printTime) {
        printResult(printTime, startTime)
        last = current
      }
    }
  }

  private def printResult(printTime: Int, startTime: Long) {
    val nowSumTime = sumTime.get();
    val nowSuccess = successCount.get();
    val nowFailed = failedCount.get();
    val currSuccess = nowSuccess - lastSuccessCount.get();
    if (currSuccess != 0) {
      val averageLatency = nowSumTime / nowSuccess
      val recentLatency = printTime/(currSuccess)
      if (recentLatency > maxLatency) {
        maxLatency = recentLatency
      }

      LOGGER.fatal("total success: " + nowSuccess.longValue()
        + " recent_latency: " + recentLatency + " time_per_request: "
        + averageLatency + "ms" + " max_latency: "
        + maxLatency + " total_failed: " + nowFailed)
      lastSuccessCount.set(nowSuccess);
    } else {
      LOGGER.fatal("there is no success update in last 10 seconds")
    }
  }
}

object ToolProperties {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  var properties = new Properties()
  try {
    val propertyPath =
      new File(this.getClass.getResource("/updatebenchmark.properties").getPath).getCanonicalPath
    val in = new FileInputStream(propertyPath)
    properties.load(in)
  } catch {
    case e: Exception =>
      LOGGER.warn("can not find updatebenchmark.properties, use default configuration")
  }

  var updateTimes = getConf("UPDATE_TIMES", 30)
  var loadParallelize = getConf("LOAD_PARALLELIZE", 10)
  val rowCount = getConf("ROW_COUNT", 100000);
  val variableSize = getConf("VARIABLE_SIZE", 100);
  val rangeInt = getConf("RANGE_INT", 10);
  val affixRandom = getConf("AFFIX_RANDOM", true);
  val updateRate = getConf("UPDATE_RATE", 0.01f);
  val targetTableName = getConf("TARGET_TABLE_NAME", "target_table_6");
  val changeTableName = getConf("CHANGE_TABLE_NAME", "change_table_6");
  val dbName = getConf("DB_NAME", "default");
  val format = getConf("FORMAT", "carbondata")
  var runMode = getConf("runMode", "local")
  var fixChangeData = getConf("FIX_CHANGE_DATA", true)
  var partitionNumber = getConf("PARTITION_NUMBER", 1)
  partitionNumber = if (partitionNumber <= 1) 1 else partitionNumber

  def getConf(property: String, defaultValue: Int): Int = {
    val value = properties.getProperty(property)
    if (value != null && !value.isEmpty) {
      Integer.valueOf(value)
    } else {
      defaultValue
    }
  }

  def getConf(property: String, defaultValue: Boolean): Boolean = {
    val value = properties.getProperty(property)
    if (value != null && !value.isEmpty) {
      value.toBoolean
    } else {
      defaultValue
    }
  }

  def getConf(property: String, defaultValue: Float): Float = {
    val value = properties.getProperty(property)
    if (value != null && !value.isEmpty) {
      Float.unbox(value)
    } else {
      defaultValue
    }
  }

  def getConf(property: String, defaultValue: String): String = {
    val value = properties.getProperty(property)
    if (value != null && !value.isEmpty) {
      value
    } else {
      defaultValue
    }
  }
}
// scalastyle:on println
