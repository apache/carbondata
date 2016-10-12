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

import java.io.{DataOutputStream, File}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonExample {
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonExample")

    def currentPath: String = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val outputPath = currentPath + "/block_prune_test.csv"
    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
<<<<<<< 978e08e9cdd2f8d828a45c1cccad5c610dadfead
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
=======
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
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

    cc.sql("DROP TABLE IF EXISTS blockprune")



    cc.sql(
      """
        CREATE TABLE IF NOT EXISTS blockprune (name string, id int)
        STORED BY 'org.apache.carbondata.format'
      """)
    cc.sql(
      s"LOAD DATA LOCAL INPATH '$outputPath' INTO table blockprune options('FILEHEADER'='name,id')"
    )
    // data is in all 7 blocks
    cc.sql(
      s"select count(*) from blockprune where name = 'a'"
    ).show()

  }
}
