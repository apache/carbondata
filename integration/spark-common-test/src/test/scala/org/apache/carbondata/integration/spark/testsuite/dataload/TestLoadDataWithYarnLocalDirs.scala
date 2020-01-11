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

package org.apache.carbondata.integration.spark.testsuite.dataload

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil4Test
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}

/**
 * Test Class for data loading using multiple temp yarn dirs.
 * It would be better to test with massive data,
 * since small amount of data will generate few (or none) temp files,
 * which will not fully utilize all the temp dirs configured.
 */
class TestLoadDataWithYarnLocalDirs extends QueryTest with BeforeAndAfterAll {
  override def beforeAll {
    sql("drop table if exists carbontable_yarnLocalDirs")
    sql("CREATE TABLE carbontable_yarnLocalDirs (id int, name string, city string, age int) " +
        "STORED AS carbondata")
  }

  private def getMockedYarnLocalDirs = {
    val multi_dir_root = System.getProperty("java.io.tmpdir") + File.separator +
              "yarn_local_multiple_dir" + File.separator
    (1 to 3).map(multi_dir_root + "multiple" + _).mkString(",")
  }

  private def initYarnLocalDir = {
    //set all the possible env for yarn local dirs in case of various deploy environment
    val sparkConf = SparkContext.getOrCreate().getConf
    sparkConf.set("SPARK_EXECUTOR_DIRS", getMockedYarnLocalDirs)
    sparkConf.set("SPARK_LOCAL_DIRS", getMockedYarnLocalDirs)
    sparkConf.set("MESOS_DIRECTORY", getMockedYarnLocalDirs)
    sparkConf.set("spark.local.dir", getMockedYarnLocalDirs)
    sparkConf.set("LOCAL_DIRS", getMockedYarnLocalDirs)

    SparkUtil4Test.getOrCreateLocalRootDirs(sparkConf)
  }

  private def cleanUpYarnLocalDir = {
    initYarnLocalDir
      .foreach(dir => CarbonUtil.deleteFoldersAndFiles(new File(dir)))
  }

  private def enableMultipleDir = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR, "true")
  }

  private def disableMultipleDir = {
    CarbonProperties.getInstance().addProperty(
      CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR,
      CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR_DEFAULT)
  }

  test("test carbon table data loading for multiple temp dir") {
    initYarnLocalDir

    enableMultipleDir

    sql(s"LOAD DATA LOCAL INPATH '${resourcesPath}/sample.csv' INTO TABLE " +
        "carbontable_yarnLocalDirs OPTIONS('DELIMITER'= ',')")

    disableMultipleDir

    checkAnswer(sql("select id from carbontable_yarnLocalDirs"),
      Seq(Row(1), Row(2), Row(3), Row(3), Row(4), Row(4)))

    cleanUpYarnLocalDir
  }

  override def afterAll {
    sql("drop table if exists carbontable_yarnLocalDirs")
    
    cleanUpYarnLocalDir
  }
}
