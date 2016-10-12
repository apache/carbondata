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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonExample {
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
<<<<<<< 978e08e9cdd2f8d828a45c1cccad5c610dadfead
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
=======
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
<<<<<<< ba48bd5be23af7a7e27021f381beaabec909f96a
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "true")
    val testData: Array[String]= new Array[String](3);
    testData(0) = "a"
    testData(1) = "b"
    testData(2) = "c"
    var writer: DataOutputStream = null
    try {
      val fileType = FileFactory.getFileType(outputPath)
      val file = FileFactory.getCarbonFile(outputPath, fileType)
      if (!file.exists()) {
        file.createNewFile()
      }
      writer = FileFactory.getDataOutputStream(outputPath, fileType)
      for (i <- 0 to 2) {
        for (j <- 0 to 240000) {
          writer.writeBytes(testData(i) + "," + j + "\n")
        }
      }
    }  finally {
      if (writer != null) {
        try {
          writer.close()
        }
      }
    }
>>>>>>> Fix
=======
>>>>>>> Fix

    cc.sql("DROP TABLE IF EXISTS t3")

    cc.sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
           """)

    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table t3
           """)

    cc.sql("""
           SELECT country, count(salary) AS amount
           FROM t3
           WHERE country IN ('china','france')
           GROUP BY country
           """).show()

    cc.sql("DROP TABLE IF EXISTS t3")
  }
}
