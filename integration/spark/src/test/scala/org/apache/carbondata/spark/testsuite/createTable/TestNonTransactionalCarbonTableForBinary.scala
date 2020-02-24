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

package org.apache.carbondata.spark.testsuite.createTable

import java.io._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.sdk.util.BinaryUtil

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.apache.spark.util.SparkUtil
import org.scalatest.BeforeAndAfterAll


class TestNonTransactionalCarbonTableForBinary extends QueryTest with BeforeAndAfterAll {

    var writerPath = new File(this.getClass.getResource("/").getPath
            + "../../target/SparkCarbonFileFormat/WriterOutput/")
            .getCanonicalPath
    var outputPath = writerPath + 2
    //getCanonicalPath gives path with \, but the code expects /.
    writerPath = writerPath.replace("\\", "/")

    var sdkPath = new File(this.getClass.getResource("/").getPath + "../../../../sdk/sdk/")
            .getCanonicalPath

    def buildTestBinaryData(): Any = {
        FileUtils.deleteDirectory(new File(writerPath))

        val sourceImageFolder = sdkPath + "/src/test/resources/image/flowers"
        val sufAnnotation = ".txt"
        BinaryUtil.binaryToCarbon(sourceImageFolder, writerPath, sufAnnotation, ".jpg")
    }

    def cleanTestData() = {
        FileUtils.deleteDirectory(new File(writerPath))
        FileUtils.deleteDirectory(new File(outputPath))
    }

    override def beforeAll(): Unit = {
        CarbonProperties.getInstance()
                .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                    CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
        buildTestBinaryData()

        FileUtils.deleteDirectory(new File(outputPath))
        sql("DROP TABLE IF EXISTS sdkOutputTable")
    }

    override def afterAll(): Unit = {
        cleanTestData()
        sql("DROP TABLE IF EXISTS sdkOutputTable")
    }

    test("test read image carbon with external table, generate by sdk, CTAS") {
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
        if (SparkUtil.isSparkVersionXandAbove("2.2")) {
            sql(s"CREATE EXTERNAL TABLE binaryCarbon STORED AS carbondata LOCATION '$writerPath'")
            sql(s"CREATE TABLE binaryCarbon3 STORED AS carbondata AS SELECT * FROM binaryCarbon")

            checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon"),
                Seq(Row(3)))
            checkAnswer(sql("SELECT COUNT(*) FROM binaryCarbon3"),
                Seq(Row(3)))

            val result = sql("desc formatted binaryCarbon").collect()
            var flag = false
            result.foreach { each =>
                if ("binary".equals(each.get(1))) {
                    flag = true
                }
            }
            assert(flag)
            val value = sql("SELECT * FROM binaryCarbon").collect()
            assert(3 == value.length)
            value.foreach { each =>
                val byteArray = each.getAs[Array[Byte]](2)
                assert(new String(byteArray).startsWith("����\u0000\u0010JFIF"))
            }

            val value3 = sql("SELECT * FROM binaryCarbon3").collect()
            assert(3 == value3.length)
            value3.foreach { each =>
                val byteArray = each.getAs[Array[Byte]](2)
                assert(new String(byteArray).startsWith("����\u0000\u0010JFIF"))
            }
            sql("DROP TABLE IF EXISTS binaryCarbon")
            sql("DROP TABLE IF EXISTS binaryCarbon3")
        }
    }

    test("Don't support insert into partition table") {
        sql("DROP TABLE IF EXISTS binaryCarbon")
        sql("DROP TABLE IF EXISTS binaryCarbon2")
        sql("DROP TABLE IF EXISTS binaryCarbon3")
        sql("DROP TABLE IF EXISTS binaryCarbon4")
        if (SparkUtil.isSparkVersionXandAbove("2.2")) {
            sql(s"CREATE TABLE binaryCarbon USING CARBON LOCATION '$writerPath'")
            sql(
                s"""
                   | CREATE TABLE binaryCarbon2(
                   |    binaryId INT,
                   |    binaryName STRING,
                   |    binary BINARY,
                   |    labelName STRING,
                   |    labelContent STRING
                   |) STORED AS carbondata""".stripMargin)
            sql(
                s"""
                   | CREATE TABLE binaryCarbon3(
                   |    binaryId INT,
                   |    binaryName STRING,
                   |    labelName STRING,
                   |    labelContent STRING
                   |)  partitioned by ( binary BINARY) STORED AS carbondata""".stripMargin)

            sql("insert into binaryCarbon2 select binaryId,binaryName,binary,labelName,labelContent from binaryCarbon where binaryId=0 ")
            val carbonResult2 = sql("SELECT * FROM binaryCarbon2")

            sql("create table binaryCarbon4 STORED AS carbondata select binaryId,binaryName,binary,labelName,labelContent from binaryCarbon where binaryId=0 ")
            val carbonResult4 = sql("SELECT * FROM binaryCarbon4")
            val carbonResult = sql("SELECT * FROM binaryCarbon")

            assert(3 == carbonResult.collect().length)
            assert(1 == carbonResult4.collect().length)
            assert(1 == carbonResult2.collect().length)
            checkAnswer(carbonResult4, carbonResult2)

            try {
                sql("insert into binaryCarbon3 select binaryId,binaryName,binary,labelName,labelContent from binaryCarbon where binaryId=0 ")
                assert(false)
            } catch {
                case e: Exception =>
                    e.printStackTrace()
                    assert(true)
            }
            sql("DROP TABLE IF EXISTS binaryCarbon")
            sql("DROP TABLE IF EXISTS binaryCarbon2")
            sql("DROP TABLE IF EXISTS binaryCarbon3")
            sql("DROP TABLE IF EXISTS binaryCarbon4")
        }
    }
}
