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
import java.util.UUID

import scala.collection.JavaConverters._

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuiteLike}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.SchemaReader
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.presto.server.PrestoServer
import org.apache.carbondata.presto.util.CarbonDataStoreCreator

class PrestoInsertIntoTableTestCase extends FunSuiteLike with BeforeAndAfterAll with BeforeAndAfterEach {

  private val logger = LogServiceFactory
    .getLogService(classOf[PrestoAllDataTypeTest].getCanonicalName)

  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"
  private val systemPath = s"$rootPath/integration/presto/target/system"
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

  override protected def beforeEach(): Unit = {
    val query = "create table testdb.testtable(ID int, date date, country varchar, name varchar, phonetype varchar, serialname varchar,salary decimal(6,1), bonus decimal(8,6), monthlyBonus decimal(5,3), dob timestamp, shortField smallint, iscurrentemployee boolean) with(format='CARBONDATA') "
    createTable(query, "testdb", "testtable")
  }

  private def createTable(query: String, databaseName: String, tableName: String): Unit = {
    prestoServer.execute(s"drop table if exists ${databaseName}.${tableName}")
    prestoServer.execute(query)
    logger.info("Creating The Carbon Store")
    val absoluteTableIdentifier: AbsoluteTableIdentifier = getAbsoluteIdentifier(databaseName, tableName)
    CarbonDataStoreCreator.createTable(absoluteTableIdentifier, true)
    logger.info(s"\nCarbon store is created at location: $storePath")
  }

  private def getAbsoluteIdentifier(dbName: String,
      tableName: String) = {
    val absoluteTableIdentifier = AbsoluteTableIdentifier.from(
      storePath + "/" + dbName + "/" + tableName,
      new CarbonTableIdentifier(dbName,
        tableName,
        UUID.randomUUID().toString))
    absoluteTableIdentifier
  }

  test("test insert with different storage format names") {
    val query1 = "create table testdb.testtable(ID int, date date, country varchar, name varchar, phonetype varchar, serialname varchar,salary decimal(6,1), bonus decimal(8,6), monthlyBonus decimal(5,3), dob timestamp, shortField smallint, iscurrentemployee boolean) with(format='CARBONDATA') "
    val query2 = "create table testdb.testtable(ID int, date date, country varchar, name varchar, phonetype varchar, serialname varchar,salary decimal(6,1), bonus decimal(8,6), monthlyBonus decimal(5,3), dob timestamp, shortField smallint, iscurrentemployee boolean) with(format='CARBON') "
    val query3 = "create table testdb.testtable(ID int, date date, country varchar, name varchar, phonetype varchar, serialname varchar,salary decimal(6,1), bonus decimal(8,6), monthlyBonus decimal(5,3), dob timestamp, shortField smallint, iscurrentemployee boolean) with(format='ORG.APACHE.CARBONDATA.FORMAT') "
    createTable(query1, "testdb", "testtable")
    createTable(query2, "testdb", "testtable")
    createTable(query3, "testdb", "testtable")
    prestoServer.execute("insert into testdb.testtable values(10, current_date, 'INDIA', 'Chandler', 'qwerty', 'usn20392',10000.0,16.234567,25.678,timestamp '1994-06-14 05:00:09',smallint '23', true)")
    val absoluteTableIdentifier: AbsoluteTableIdentifier = getAbsoluteIdentifier("testdb", "testtable")
    val carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier)
    val segmentPath = CarbonTablePath.getSegmentPath(carbonTable.getTablePath, "0")
    assert(FileFactory.getCarbonFile(segmentPath).isFileExist)
  }

  test("test insert into one segment and check folder structure") {
    prestoServer.execute("insert into testdb.testtable values(10, current_date, 'INDIA', 'Chandler', 'qwerty', 'usn20392',10000.0,16.234567,25.678,timestamp '1994-06-14 05:00:09',smallint '23', true)")
    prestoServer.execute("insert into testdb.testtable values(10, current_date, 'INDIA', 'Chandler', 'qwerty', 'usn20392',10000.0,16.234567,25.678,timestamp '1994-06-14 05:00:09',smallint '23', true)")
    val absoluteTableIdentifier: AbsoluteTableIdentifier = getAbsoluteIdentifier("testdb", "testtable")
    val carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier)
    val tablePath = carbonTable.getTablePath
    val segment0Path = CarbonTablePath.getSegmentPath(tablePath, "0")
    val segment1Path = CarbonTablePath.getSegmentPath(tablePath, "1")
    val segment0 = FileFactory.getCarbonFile(segment0Path)
    assert(segment0.isFileExist)
    assert(segment0.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.endsWith(CarbonTablePath.CARBON_DATA_EXT) ||
        file.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)
      }
    }).length == 2)
    val segment1 = FileFactory.getCarbonFile(segment1Path)
    assert(segment1.isFileExist)
    assert(segment1.listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.endsWith(CarbonTablePath.CARBON_DATA_EXT) ||
        file.getName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)
      }
    }).length == 2)
    val segmentsPath = CarbonTablePath.getSegmentFilesLocation(tablePath)
    assert(FileFactory.getCarbonFile(segmentsPath).isFileExist && FileFactory.getCarbonFile(segmentsPath).listFiles(true).size() == 2)
    val metadataFolderPath = CarbonTablePath.getMetadataPath(tablePath)
    FileFactory.getCarbonFile(metadataFolderPath).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = {
        file.getName.endsWith(CarbonTablePath.TABLE_STATUS_FILE)
      }
    })
  }

  test("test insert into many segments and check segment count and data count") {
    prestoServer.execute("insert into testdb.testtable values(10, current_date, 'INDIA', 'Chandler', 'qwerty', 'usn20392',10000.0,16.234567,25.678,timestamp '1994-06-14 05:00:09',smallint '23', true)")
    prestoServer.execute("insert into testdb.testtable values(10, current_date, 'INDIA', 'Chandler', 'qwerty', 'usn20392',10000.0,16.234567,25.678,timestamp '1998-12-16 10:12:09',smallint '23', true)")
    prestoServer.execute("insert into testdb.testtable values(10, current_date, 'INDIA', 'Chandler', 'qwerty', 'usn20392',10000.0,16.234567,25.678,timestamp '1994-06-14 05:00:09',smallint '23', true)")
    prestoServer.execute("insert into testdb.testtable values(10, current_date, 'INDIA', 'Chandler', 'qwerty', 'usn20392',10000.0,16.234567,25.678,timestamp '1998-12-16 10:12:09',smallint '23', true)")
    val absoluteTableIdentifier: AbsoluteTableIdentifier = getAbsoluteIdentifier("testdb", "testtable")
    val carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier)
    val segmentFoldersLocation = CarbonTablePath.getPartitionDir(carbonTable.getTablePath)
    assert(FileFactory.getCarbonFile(segmentFoldersLocation).listFiles(false).size() == 8)
    val actualResult1: List[Map[String, Any]] = prestoServer
      .executeQuery("select count(*) AS RESULT from testdb.testtable")
    val expectedResult1: List[Map[String, Any]] = List(Map("RESULT" -> 4))
    assert(actualResult1.equals(expectedResult1))
    // filter query
    val actualResult2: List[Map[String, Any]] = prestoServer
      .executeQuery("select count(*) AS RESULT from testdb.testtable WHERE dob = timestamp '1998-12-16 10:12:09'")
    val expectedResult2: List[Map[String, Any]] = List(Map("RESULT" -> 2))
    assert(actualResult2.equals(expectedResult2))
  }

  test("test if the table status contains the segment file name for each load") {
    prestoServer.execute("insert into testdb.testtable values(10, current_date, 'INDIA', 'Chandler', 'qwerty', 'usn20392',10000.0,16.234567,25.678,timestamp '1994-06-14 05:00:09',smallint '23', true)")
    val absoluteTableIdentifier: AbsoluteTableIdentifier = getAbsoluteIdentifier("testdb", "testtable")
    val carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier)
    val ssm = new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier)
    ssm.getValidAndInvalidSegments.getValidSegments.asScala.foreach { segment =>
      val loadMetadataDetails = segment.getLoadMetadataDetails
      assert(loadMetadataDetails.getSegmentFile != null)
    }
  }

  override def afterAll(): Unit = {
    prestoServer.stopServer()
    CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath))
  }
}
