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

package org.apache.carbondata.alluxio

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.alluxio.util.AlluxioUtilTest
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

class AlluxioWithCarbonSessionTest extends AlluxioUtilTest with BeforeAndAfterAll {
    var remoteFile = ""
    var allDataTypeRemote = ""
    var storeLocationOriginal = ""
    val carbonAndAlluxio = "/CarbonAndAlluxio"

    override protected def beforeAll(): Unit = {
        super.beforeAll()
        storeLocationOriginal = CarbonProperties.getInstance().
                getProperty(CarbonCommonConstants.STORE_LOCATION)
        val alluxioStoreLocation = localAlluxioCluster.getMasterURI + carbonAndAlluxio
        CarbonProperties.getInstance().
                addProperty(CarbonCommonConstants.STORE_LOCATION, alluxioStoreLocation)
        val rootPath = new File(this.getClass.getResource("/").getPath
                + "../../../..").getCanonicalPath

        val time = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
        val localFile = rootPath + "/hadoop/src/test/resources/data.csv"
        remoteFile = "/carbon_alluxio" + time + ".csv"
        fileSystemShell.run("copyFromLocal", localFile, remoteFile)

        val allDataTypeLocal = resourcesPath + "/alldatatypeforpartition.csv"
        allDataTypeRemote = "/alldatatype" + time + ".csv"
        fileSystemShell.run("copyFromLocal", allDataTypeLocal, allDataTypeRemote)
        fileSystemShell.run("ls", allDataTypeRemote)
    }

    test("CarbonSession test alluxio with all data type and Create/Load/Select/Insert/Update/Delete") {
        val tableNameForAllType = "alluxio_table_all_type"
        try {
            CarbonProperties.getInstance()
                    .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
                        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

            sql("DROP TABLE IF EXISTS " + tableNameForAllType)
            sql(
                s"""create table $tableNameForAllType(
                   | smallIntField SMALLINT,
                   | intField INT,
                   | bigIntField BIGINT,
                   | floatField FLOAT,
                   | doubleField DOUBLE,
                   | decimalField DECIMAL(25, 4),
                   | timestampField TIMESTAMP,
                   | dateField DATE,
                   | stringField STRING,
                   | varcharField VARCHAR(10),
                   | charField CHAR(10),
                   | arrayField ARRAY<string>,
                   | structField STRUCT<col1:STRING, col2:STRING, col3:STRING>,
                   | booleanField BOOLEAN)
                   | stored by 'carbondata'
             """.stripMargin)

            val path = localAlluxioCluster.getMasterURI + allDataTypeRemote

            sql(s"LOAD DATA LOCAL INPATH '$path' INTO TABLE $tableNameForAllType " +
                    "options('COMPLEX_DELIMITER_LEVEL_1'='$','COMPLEX_DELIMITER_LEVEL_2'=':')")

            AlluxioCommonTest.testAllDataType(tableNameForAllType)

            fileSystemShell.run("ls", carbonAndAlluxio + "/default")
        } catch {
            case _: Exception => assert(false)
        } finally {
            sql("DROP TABLE IF EXISTS " + tableNameForAllType)
        }
    }

    test("CarbonSession test alluxio with different TBLPROPERTIES for creating table and segments") {
        val tableName = "alluxio_table"
        try {
            CarbonProperties.getInstance()
                    .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")
            sql(s"DROP TABLE IF EXISTS $tableName")
            sql(
                s"""
                   | CREATE TABLE IF NOT EXISTS $tableName(
                   |       ID INT,
                   |       date DATE,
                   |       country STRING,
                   |       name STRING,
                   |       phonetype STRING,
                   |       serialname STRING,
                   |       salary INT)
                   | STORED BY 'carbondata'
                   | TBLPROPERTIES(
                   |    'SORT_COLUMNS'='salary',
                   |    'AUTO_LOAD_MERGE'='true',
                   |    'LOCAL_DICTIONARY_ENABLE'='true',
                   |    'LOCAL_DICTIONARY_THRESHOLD'='1100',
                   |    'LOCAL_DICTIONARY_INCLUDE'='country',
                   |    'LOAD_MIN_SIZE_INMB'='128',
                   |    'TABLE_BLOCKLET_SIZE'='8')
             """.stripMargin)

            val path = localAlluxioCluster.getMasterURI + remoteFile
            sql(s"""LOAD DATA LOCAL INPATH '$path' INTO TABLE $tableName""")

            AlluxioCommonTest.testCreateTableAndSegment(tableName, path, remoteFile, localAlluxioCluster = localAlluxioCluster)
            fileSystemShell.run("ls", carbonAndAlluxio + "/default")
        } catch {
            case e: Exception =>
                e.printStackTrace()
                assert(false)
        } finally {
            CarbonProperties.getInstance()
                    .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
                        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
            sql("DROP TABLE IF EXISTS " + tableName)
        }
    }

    test("CarbonSession test alluxio with alter table") {
        val tableName = "alluxio_table"
        val tableNameOriginal = "carbon_table"
        try {
            // Specify date format based on raw data
            CarbonProperties.getInstance()
                    .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

            sql(s"DROP TABLE IF EXISTS $tableName")
            sql(s"DROP TABLE IF EXISTS $tableNameOriginal")
            sql(
                s"""
                   | CREATE TABLE IF NOT EXISTS $tableNameOriginal(
                   |       ID INT,
                   |       date DATE,
                   |       country STRING,
                   |       name STRING,
                   |       phonetype STRING,
                   |       serialname STRING,
                   |       salary INT)
                   | STORED BY 'carbondata'
             """.stripMargin)

            val path = localAlluxioCluster.getMasterURI + remoteFile
            sql(s"""LOAD DATA LOCAL INPATH '$path' INTO TABLE $tableNameOriginal""")
            AlluxioCommonTest.testAlterTAble(tableName, tableNameOriginal, remoteFile)
            fileSystemShell.run("ls", carbonAndAlluxio + "/default")
        } catch {
            case e: Exception =>
                e.printStackTrace()
                assert(false)
        } finally {
            // Specify date format based on raw data
            CarbonProperties.getInstance()
                    .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
                        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

            sql("DROP TABLE IF EXISTS " + tableName)
            sql("DROP TABLE IF EXISTS " + tableNameOriginal)
        }
    }

    override protected def afterAll(): Unit = {
        // fileSystemShell.run("rm", remoteFile)
        // fileSystemShell.run("rm", allDataTypeRemote)
        super.afterAll()
        CarbonProperties.getInstance()
                .addProperty(CarbonCommonConstants.STORE_LOCATION, storeLocationOriginal)
                .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
                    CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    }
}
