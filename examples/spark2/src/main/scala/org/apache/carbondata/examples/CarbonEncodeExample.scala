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

package org.apache.carbondata.examples

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.SnappyCompressor
import org.apache.carbondata.core.datastore.page.{ColumnPage, SafeFixLengthColumnPage}
import org.apache.carbondata.core.datastore.page.encoding.DirectCompressCodec
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.util.CarbonProperties

object CarbonEncodeExample {
  def main(args: Array[String]): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")
    val primitivePageStatsCollector = PrimitivePageStatsCollector.newInstance(DataType.INT, 2, 0, 0)
    val directCompressCodec = DirectCompressCodec
      .newInstance(primitivePageStatsCollector, new SnappyCompressor)
    val columnPage = ColumnPage.newPage(DataType.INT, 2)
    columnPage.setStatsCollector(primitivePageStatsCollector)
    val encodedData = directCompressCodec.encode(columnPage)
    val inputData = encodedData.getEncodedData
    val newColumnPage = directCompressCodec.decode(inputData, 0, inputData.length)
    assert(newColumnPage.getDataType.equals(DataType.INT))
    assert(newColumnPage.getPageSize.equals(2))
    System.out.print("Test case passed.")
  }
}
