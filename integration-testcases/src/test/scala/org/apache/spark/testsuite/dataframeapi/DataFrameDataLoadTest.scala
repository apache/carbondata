package org.apache.spark.testsuite.dataframeapi

import java.io.File

import org.apache.spark.sql.common.util.CarbonHiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{CarbonContext, DataFrame, Row, SaveMode}
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import org.apache.carbondata.core.util.CarbonProperties

class DataFrameDataLoadTest extends FlatSpec with BeforeAndAfterAll {

  val carbonContext: CarbonContext = CarbonHiveContext

  import carbonContext.implicits._

  val tempDir = System.getProperty("java.io.tmpdir") + "/"
  val names = List("Alexander", "Russia", "Alexa", "Bliss", "Alexander")
  val ages = List(25, 30, 35, 40, 45)
  val content = (1 to 5).toList.map(item => (item, names(item - 1), ages(item - 1)))
  val dataFrame = carbonContext.sparkContext.parallelize(content)
    .toDF("id", "name", "age")

  override def beforeAll() {
    CarbonProperties.getInstance().addProperty("carbon.direct.surrogate", "false")
    carbonContext.sql(
      "create table if not exists test ( id Int, name String, age Int) stored " +
      "by 'carbondata'")
    if (new File(tempDir + "parquettest").exists()) {
      new File(tempDir + "parquettest").delete()
      dataFrame.write.mode(SaveMode.Overwrite).parquet(tempDir + "parquettest")
    } else {
      dataFrame.write.mode(SaveMode.Overwrite).parquet(tempDir + "parquettest")
    }
  }

  override def afterAll() {
    carbonContext.sql("DROP TABLE IF EXISTS test")
  }

  private def loadData: DataFrame = {
    carbonContext.read
      .format("carbondata")
      .option("tableName", "test")
      .load()
  }

  "CarbonData" should "be able read data from a table with multiple loads" in {
    dataFrame.write
      .format("carbondata")
      .option("tableName", "test")
      .option("compress", "true")
      .option("useKettle", "false")
      .mode(SaveMode.Overwrite)
      .save()

    val tableFrameLoad1 = loadData
    assert(tableFrameLoad1.count() == 5l)
    assert(tableFrameLoad1.collect().contains(Row(1, "Alexander", 25)))

    dataFrame.write
      .format("carbondata")
      .option("tableName", "test")
      .option("compress", "true")
      .option("useKettle", "false")
      .mode(SaveMode.Append)
      .save()

    val tableFrameLoad2 = loadData
    assert(tableFrameLoad2.count() == 10l)
    assert(tableFrameLoad2.collect().contains(org.apache.spark.sql.Row(1, "Alexander", 25)))
  }

  it should "be able to read data from a parquet file" in {
    val tableFrameParquet = carbonContext.read.parquet(tempDir + "parquettest")
    assert(tableFrameParquet.count() == 5l)
    assert(tableFrameParquet.collect().contains(org.apache.spark.sql.Row(1, "Alexander", 25)))
  }

  it should "be able to perform logical operation on dataframe created with table data" in {
    val filteredFrame = dataFrame.filter("id > 2")
    assert(filteredFrame.count() == 3l)

    val regexFrame = dataFrame.filter(dataFrame.col("name").like("%ss%"))
    assert(regexFrame.count() == 2l)

    val aggregateFrame = dataFrame.agg(avg(dataFrame.col("age")), max(dataFrame.col("age")))
    assert(aggregateFrame.first() == org.apache.spark.sql.Row(35.0, 45))

    val dataFrameTypes = dataFrame.dtypes
    assert(dataFrameTypes ===
           Array(("id", "IntegerType"), ("name", "StringType"), ("age", "IntegerType")))

    val columnNames = dataFrame.columns
    assert(columnNames === Array("id", "name", "age"))

    val duplicateRecord = carbonContext.sparkContext
      .parallelize(List((1, "Alexander", 25), (2, "Russia", 30)))
      .toDF("id", "name", "age")
    val duplicateRecordFrame = dataFrame.unionAll(duplicateRecord)

    assert(duplicateRecordFrame.count() == 7l)

    assert(duplicateRecordFrame.dropDuplicates().count() == 5l)

    assert(dataFrame.except(duplicateRecord).count() == 3l)

    assert(dataFrame.drop("age").columns.length == 2l)

    val groupedDataFrame = dataFrame.groupBy("name").count()
    val filterWithValue = groupedDataFrame.collect()
      .filter(row => row.getString(0).equalsIgnoreCase("Alexander"))

    assert(filterWithValue(0).getLong(1) == 2l)
  }

}
