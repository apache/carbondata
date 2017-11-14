package org.apache.carbondata.examples

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object S3Example {
  val logger: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  val rootPath: String = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  val metastoredb_S3 = s"$rootPath/examples/spark2/target/s3_metastore_db"
  val carbonTableName = "comparetest_hive_carbon"
  val warehouse = s"$rootPath/examples/spark2/target/warehouse"

  /**
   * To run the example, you need to add a valid access key,secret key and Bucket Name
   * @param args
   */
  def main(args: Array[String]) {
    CarbonProperties.getInstance()
      .addProperty("carbon.enable.vector.reader", "true")
      .addProperty("enable.unsafe.sort", "true")
      .addProperty("carbon.blockletgroup.size.in.mb", "32")
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.LOCK_TYPE, CarbonCommonConstants.CARBON_LOCK_TYPE_S3)
      .addProperty(CarbonCommonConstants.S3_ACCESS_KEY, "*********")
      .addProperty(CarbonCommonConstants.S3_SECRET_KEY, "*********")
      .addProperty(CarbonCommonConstants.S3_ENDPOINT, "***********")
      .addProperty(CarbonCommonConstants.S3_SSL_ENABLED, "false")
      .addProperty(CarbonCommonConstants.S3_MAX_ERROR_RETRIES, "2")
      .addProperty(CarbonCommonConstants.S3_MAX_CLIENT_RETRIES, "2")
      .addProperty(CarbonCommonConstants.S3_MAX_CONNECTIONS, "20")
      .addProperty(CarbonCommonConstants.S3_STAGING_DIRECTORY, "carbonData")
      .addProperty(CarbonCommonConstants.S3_USE_INSTANCE_CREDENTIALS, "false")
      .addProperty(CarbonCommonConstants.S3_PIN_CLIENT_TO_CURRENT_REGION, "false")
      .addProperty(CarbonCommonConstants.S3_SSE_ENABLED, "false")

    import org.apache.spark.sql.CarbonSession._

    /**
     * Provide AWS S3 Bucket-name
     */
    val carbon: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("CompareTestExample")
      .config("carbon.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(
        "s3a://<Bucket-Name>", metastoredb_S3)

    carbon.sql("DROP TABLE IF EXISTS carbon_table")

    // Create table
    carbon.sql(
      s"""
         | CREATE TABLE carbon_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES('SORT_COLUMNS'='', 'DICTIONARY_INCLUDE'='dateField, charField')
       """.stripMargin)

    val path = s"$rootPath/examples/spark2/src/main/resources/data.csv"

    // scalastyle:off
    carbon.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$path'
         | INTO TABLE carbon_table
         | OPTIONS('HEADER'='true', 'COMPLEX_DELIMITER_LEVEL_1'='#')
       """.stripMargin)
    // scalastyle:on

    carbon.sql(
      s"""
         | SELECT *
         | FROM carbon_table
      """.stripMargin).show()
  }
}
