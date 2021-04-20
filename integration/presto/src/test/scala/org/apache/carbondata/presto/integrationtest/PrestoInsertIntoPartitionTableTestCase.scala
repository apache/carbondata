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

package org.apache.carbondata.presto.integrationtest

import java.io.File
import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.datatype.{DataTypes, StructField}
import org.apache.carbondata.core.metadata.schema.{PartitionInfo, SchemaReader}
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.presto.server.PrestoServer

/**
 * Tests for partition tables transational write in presto
 */
class PrestoInsertIntoPartitionTableTestCase
  extends FunSuiteLike with BeforeAndAfterAll with BeforeAndAfterEach {

  private val logger = LogServiceFactory
    .getLogService(classOf[PrestoAllDataTypeTest].getCanonicalName)

  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"
  private val prestoServer = new PrestoServer

  override def beforeAll: Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME,
      "Presto")
    val map = new util.HashMap[String, String]()
    map.put("hive.metastore", "file")
    map.put("hive.metastore.catalog.dir", s"file://$storePath")
    map.put("hive.allow-drop-table", "true")
    prestoServer.startServer("testdb", map)
    prestoServer.execute("drop schema if exists testdb")
    prestoServer.execute("create schema testdb")
  }

  test("test partition table insert") {
    prestoServer.execute(s"drop table if exists partitiontable")
    val query =
      "create table testdb.partitiontable(ID int, name varchar,country varchar) with " +
      "(partitioned_by = ARRAY['country'], format='CARBON') "
    val tuple = getSchemaBuilder(false)
    PrestoUtil.createTable(prestoServer, query, "testdb", "partitiontable", tuple._1, tuple._2)
    prestoServer.execute("insert into testdb.partitiontable values(10,'joey','India')")
    prestoServer.execute("insert into testdb.partitiontable values(20,'ross','US')")
    prestoServer.execute("insert into testdb.partitiontable values(30,'chandler','china')")
    val partitionDirectories = Seq("country=India/", "country=US/", "country=china/")
    checkPartitionAssertions("partitiontable", "testdb", partitionDirectories, 3)
    val actualResult1: List[Map[String, Any]] = prestoServer
      .executeQuery("select count(*) AS RESULT from testdb.partitiontable WHERE country = 'India'")
    val expectedResult1: List[Map[String, Any]] = List(Map("RESULT" -> 1))
    assert(actualResult1.equals(expectedResult1))
    val actualResult2: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "select count(*) AS RESULT from testdb.partitiontable WHERE country = 'India' or " +
        "country = 'US' or country = 'china'")
    val expectedResult2: List[Map[String, Any]] = List(Map("RESULT" -> 3))
    assert(actualResult2.equals(expectedResult2))
  }

  test("test insert to partition table with multiple partition columns") {
    prestoServer.execute(s"drop table if exists multipartitiontable")
    val query =
      "create table testdb.multipartitiontable(ID int, name varchar,country varchar, state " +
      "varchar, city varchar) with (partitioned_by = ARRAY['country','state','city'], " +
      "format='CARBON') "
    val tuple = getSchemaBuilder(true)
    PrestoUtil.createTable(prestoServer, query, "testdb", "multipartitiontable", tuple._1, tuple._2)
    prestoServer.execute(
      "insert into testdb.multipartitiontable values(10,'joey','India', 'karnataka', 'gadag')")
    prestoServer.execute(
      "insert into testdb.multipartitiontable values(20,'ross','US', 'Texas','Dallas')")
    prestoServer.execute(
      "insert into testdb.multipartitiontable values(30,'chandler','china', 'zejiang','shenzen')")
    prestoServer.execute(
      "insert into testdb.multipartitiontable values(10,'rachel','India', 'karnataka', 'mysuru')")
    val partitionDirectories = Seq("country=India/state=karnataka/city=mysuru/",
      "country=India/state=karnataka/city=gadag/",
      "country=china/state=zejiang/city=shenzen/",
      "country=US/state=Texas/city=Dallas/")
    checkPartitionAssertions("multipartitiontable", "testdb", partitionDirectories, 4)
    val actualResult1: List[Map[String, Any]] = prestoServer.executeQuery(
        "select count(*) AS RESULT from testdb.multipartitiontable WHERE country = 'India'")
    val expectedResult1: List[Map[String, Any]] = List(Map("RESULT" -> 2))
    assert(actualResult1.equals(expectedResult1))
    val actualResult2: List[Map[String, Any]] = prestoServer
      .executeQuery(
        "select count(*) AS RESULT from testdb.multipartitiontable WHERE country = 'US' or " +
        "country = 'china' or city = 'gadag'")
    val expectedResult2: List[Map[String, Any]] = List(Map("RESULT" -> 3))
    assert(actualResult2.equals(expectedResult2))
  }

  private def checkPartitionAssertions(tableName: String,
      dbName: String,
      partitionDirectories: Seq[String], segmentFileCount: Int): Unit = {
    val absoluteTableIdentifier: AbsoluteTableIdentifier = PrestoUtil
      .getAbsoluteIdentifier(dbName, tableName)
    val carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier)
    val tablePath = carbonTable.getTablePath
    val segmentsPath = CarbonTablePath.getSegmentFilesLocation(tablePath)
    // check if all the segment files are present for each segment.
    val segmentFiles = FileFactory.getCarbonFile(segmentsPath).listFiles()
    assert(segmentFiles.length == segmentFileCount)
    segmentFiles.foreach { segmentFile =>
      val segmentFileName = segmentFile.getName
      val segmentFileStore = new SegmentFileStore(carbonTable.getTablePath, segmentFileName)
      val partitionSpecs = segmentFileStore.getPartitionSpecs
      partitionDirectories.contains(partitionSpecs.get(0).getPartitions.get(0))
      // check if the partition directories are not empty
      val dataOrIndexFiles = FileFactory.getCarbonFile(partitionSpecs.get(0).getLocation.toString)
        .listFiles(new CarbonFileFilter {
          override def accept(file: CarbonFile): Boolean = {
            file.getName.endsWith(CarbonCommonConstants.FACT_FILE_EXT) ||
            file.getName.endsWith(CarbonCommonConstants.UPDATE_INDEX_FILE_EXT) ||
            file.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)
          }
        })
      assert(dataOrIndexFiles.nonEmpty)
    }
    // check if the segment file name is present in each load metadatadetail
    val ssm = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)
    ssm.getValidAndInvalidSegments.getValidSegments.asScala.foreach { segment =>
      val loadMetadataDetails = segment.getLoadMetadataDetails
      assert(loadMetadataDetails.getSegmentFile != null)
    }
  }

  def getSchemaBuilder(isMultiplePartionColuns: Boolean): (TableSchemaBuilder, PartitionInfo) = {
    val integer = new AtomicInteger(0)
    val schemaBuilder = new TableSchemaBuilder
    schemaBuilder.addColumn(new StructField("ID", DataTypes.INT), integer, false, false)
    schemaBuilder.addColumn(new StructField("name", DataTypes.STRING), integer, false, false)
    val partitionColumnSchemas: util.List[ColumnSchema] = new util.ArrayList[ColumnSchema]
    partitionColumnSchemas.add(schemaBuilder.addColumn(new StructField("country", DataTypes.STRING),
      integer, false, false))
    if (isMultiplePartionColuns) {
      partitionColumnSchemas.add(schemaBuilder.addColumn(new StructField("state", DataTypes.STRING),
        integer, false, false))
      partitionColumnSchemas.add(schemaBuilder.addColumn(new StructField("city", DataTypes.STRING),
        integer, false, false))
    }
    val partitionInfo = new PartitionInfo(partitionColumnSchemas, PartitionType.NATIVE_HIVE)
    (schemaBuilder, partitionInfo)
  }

  override def afterAll(): Unit = {
    prestoServer.stopServer()
    CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath))
  }

}
