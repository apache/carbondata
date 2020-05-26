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
import java.util.Arrays.asList

import io.prestosql.jdbc.PrestoArray
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.presto.server.PrestoServer
import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, BeforeAndAfterEach}

class PrestoReadTableFilesTest extends FunSuiteLike with BeforeAndAfterAll with BeforeAndAfterEach{

  private val rootPath = new File(this.getClass.getResource("/").getPath
    + "../../../..").getCanonicalPath
  private val storePath = s"$rootPath/integration/presto/target/store"
  private val prestoServer = new PrestoServer

  override def beforeAll: Unit = {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME,
      "Presto")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME,
      "Presto")
    val map = new util.HashMap[String, String]()
    map.put("hive.metastore", "file")
    map.put("hive.metastore.catalog.dir", s"file://$storePath")

    prestoServer.startServer("sdk_output", map)
  }

  override def afterAll(): Unit = {
    prestoServer.stopServer()
    CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(storePath))
  }

  private def createComplexTableForSingleLevelArray = {
    prestoServer.execute("drop table if exists sdk_output.files")
    prestoServer.execute("drop schema if exists sdk_output")
    prestoServer.execute("create schema sdk_output")
    prestoServer
      .execute(
        "create table sdk_output.files(stringCol varchar, intCol int, doubleCol double, realCol real, boolCol boolean, arrayStringCol1 array(varchar), arrayStringcol2 array(varchar), arrayIntCol array(int), arrayBigIntCol array(bigint), arrayRealCol array(real), arrayDoubleCol array(double), arrayBooleanCol array(boolean)) with(format='CARBON') ")
  }

  private def createComplexTableFor2LevelArray = {
    prestoServer.execute("drop table if exists sdk_output.files2")
    prestoServer.execute("drop schema if exists sdk_output")
    prestoServer.execute("create schema sdk_output")
        prestoServer
      .execute(
        "create table sdk_output.files2(arrayArrayInt array(array(int)), arrayArrayBigInt array(array(bigint)), arrayArrayReal array(array(real)), arrayArrayDouble array(array(double)), arrayArrayString array(array(varchar)), arrayArrayBoolean array(array(boolean))) with(format='CARBON') ")
  }

  private def createComplexTableFor3LevelArray = {
    prestoServer.execute("drop table if exists sdk_output.files3")
    prestoServer.execute("drop schema if exists sdk_output")
    prestoServer.execute("create schema sdk_output")
    prestoServer
        .execute(
          "create table sdk_output.files3(array3_Int array(array(array(int))), array3_BigInt array(array(array(bigint))), array3_Real array(array(array(real))), array3_Double array(array(array(double))), array3_String array(array(array(varchar))), array3_Boolean array(array(array(boolean))) ) with(format='CARBON') ")
    }


  def cleanTestData(path: String): Unit = {
    FileUtils.deleteDirectory(new File(path))
  }

  test("test single-array complex columns") {
    createComplexTableForSingleLevelArray
    val gen = new GenerateFiles()
    gen.singleLevelArrayFile();
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT arrayStringCol2, arrayIntCol, arrayBigIntCol, arrayDoubleCol, arrayRealCol, arrayBooleanCol FROM files")

    // check number of rows
    assert(actualResult.size == 5)

    // check number of columns in each row
    assert(actualResult(0).size == 6)
    assert(actualResult(1).size == 6)
    assert(actualResult(2).size == 6)
    assert(actualResult(3).size == 6)
    assert(actualResult(4).size == 6)

    for( row <- 0 to actualResult.size - 1){
      var actual1 = (actualResult(row)("arrayStringCol2").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
      var actual2 = (actualResult(row)("arrayIntCol").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
      var actual3 = (actualResult(row)("arrayBigIntCol").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
      var actual4 = (actualResult(row)("arrayDoubleCol").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
      var actual5 = (actualResult(row)("arrayRealCol").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
      var actual6 = (actualResult(row)("arrayBooleanCol").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

      if(row == 0){
        assert(actual1.sameElements(Array("India", "Egypt")))
        assert(actual2.sameElements(Array(1,2,3)))
        assert(actual3.sameElements(Array(70000L, 600000000L)))
        assert(actual4.sameElements(Array(1.1,2.2,3.3)))
        assert(actual5.sameElements(Array(1.111F,2.2F)))
        assert(actual6.sameElements(Array(true, false, true)))
      } else if(row == 1) {
        assert(actual1.sameElements(Array("Japan", "China", "India")))
        assert(actual2.sameElements(Array(1,2,3,4)))
        assert(actual3.sameElements(Array(70000L, 600000000L,8000L)))
        assert(actual4.sameElements(Array(1.1, 2.2, 4.45, 3.3)))
        assert(actual5.sameElements(Array(1.1F,2.2F,3.3f)))
        assert(actual6.sameElements(Array(true, true, true)))
      } else if(row == 2) {
        assert(actual1.sameElements(Array("China", "Brazil", "Paris", "France")))
        assert(actual2.sameElements(Array(1,2,3,4,5)))
        assert(actual3.sameElements(Array(70000L, 600000000L, 8000L, 9111111111L)))
        assert(actual4.sameElements(Array(1.1, 2.2, 4.45, 5.5, 3.3)))
        assert(actual5.sameElements(Array(1.1F, 2.2F, 3.3F, 4.45F)))
        assert(actual6.sameElements(Array(true, false, true)))
      } else if(row == 3) {
        assert(actual1.sameElements(Array("India", "Egypt")))
        assert(actual2.sameElements(Array(1,2,3)))
        assert(actual3.sameElements(Array(70000L, 600000000L)))
        assert(actual4.sameElements(Array(1.1, 2.2, 3.3)))
        assert(actual5.sameElements(Array(1.1F,2.2F)))
        assert(actual6.sameElements(Array(true, false, true)))
      } else if(row == 4) {
        assert(actual1.sameElements(Array("Japan", "China", "India")))
        assert(actual2.sameElements(Array(1,2,3,4)))
        assert(actual3.sameElements(Array(70000L, 600000000L,8000L)))
        assert(actual4.sameElements(Array(4.0, 1.0, 21.222, 15.231)))
        assert(actual5.sameElements(Array(1.1F,2.2F,3.3F)))
        assert(actual6.sameElements(Array(false, false, false)))
      }

    }

    cleanTestData(storePath + "/sdk_output/files")
  }


  /*
  +--------------------------------------------+---------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------+--------------------------+
  |arrayarrayint                               |arrayarraybigint                                                                 |arrayarrayreal                                                 |arrayarraydouble                            |arrayarraystring                  |arrayarrayboolean         |
  +--------------------------------------------+---------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------+--------------------------+
  |[[1, 2, 3], [4, 5]]                         |[[90000, 600000000], [8000], [911111111]]                                        |[[1.111, 2.2], [9.139, 2.98]]                                  |[[1.111, 2.2], [9.139, 2.98989898]]         |[[Japan, China], [India]]         |[[false, false], [false]] |
  |[[1, 2, 3], [0, 5], [1, 2, 3, 4, 5], [4, 5]]|[[40000, 600000000, 8000], [9111111111]]                                         |[[1.111, 2.2], [9.139, 2.98], [9.99]]                          |[[1.111, 2.2], [9.139777, 2.98], [9.99888]] |[[China, Brazil], [Paris, France]]|[[false], [true, false]]  |
  |[[1], [0], [3], [4, 5]]                     |[[5000], [600000000], [8000, 9111111111], [20000], [600000000, 8000, 9111111111]]|[[9.198]]                                                      |[[0.1987979]]                               |[[Japan, China, India]]           |[[false, true, false]]    |
  |[[0, 9, 0, 1, 3, 2, 3, 4, 7]]               |[[5000, 600087000, 8000, 9111111111, 20000, 600000000, 8000, 977777]]            |[[1.111, 2.2], [9.139, 2.98, 4.67], [2.91, 2.2], [9.139, 2.98]]|[[1.111, 2.0, 4.67, 2.91, 2.2, 9.139, 2.98]]|[[Japan], [China], [India]]       |[[false], [true], [false]]|
  +--------------------------------------------+---------------------------------------------------------------------------------+---------------------------------------------------------------+--------------------------------------------+----------------------------------+--------------------------+
  */


  test("test 2-level for array of int, bigInt") {
    createComplexTableFor2LevelArray
    val gen = new GenerateFiles()
    gen.twoLevelArrayFile();
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT arrayArrayint, arrayArrayBigInt FROM files2")

    // check number of rows
    assert(actualResult.size == 4)

    //check number of columns in each row
    assert(actualResult(0).size == 2)
    assert(actualResult(1).size == 2)
    assert(actualResult(2).size == 2)
    assert(actualResult(3).size == 2)

    var row_1 = (actualResult(0)("arrayArrayint").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_2 = (actualResult(1)("arrayArrayint").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_3 = (actualResult(2)("arrayArrayint").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_4 = (actualResult(3)("arrayArrayint").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    // check number of child elements in each 2-level arrayArrayint
    assert(row_1.length == 2);
    assert(row_2.length == 4);
    assert(row_3.length == 4);
    assert(row_4.length == 1);

    // check actual data in each single-level array
    assert(asList(row_1(0), row_1(1)) == asList(asList(1,2,3), asList(4,5)))
    assert(asList(row_2(0), row_2(1), row_2(2), row_2(3)) == asList(asList(1,2,3), asList(0,5), asList(1,2,3,4,5), asList(4,5)))
    assert(asList(row_3(0), row_3(1), row_3(2), row_3(3)) == asList(asList(1), asList(0), asList(3), asList(4,5)))
    assert(asList(row_4(0)) == asList(asList(0,9,0,1,3,2,3,4,7)))

    row_1 = (actualResult(0)("arrayArrayBigInt").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_2 = (actualResult(1)("arrayArrayBigInt").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_3 = (actualResult(2)("arrayArrayBigInt").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_4 = (actualResult(3)("arrayArrayBigInt").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    // check number of child elements in each 2-level arrayArrayBigInt
    assert(row_1.length == 3);
    assert(row_2.length == 2);
    assert(row_3.length == 5);
    assert(row_4.length == 1);

    // check actual data in each single-level array
    assert(asList(row_1(0),row_1(1),row_1(2)) == (asList(asList(90000L, 600000000L), asList(8000L), asList(911111111L))))
    assert(asList(row_2(0),row_2(1)) == asList(asList(40000L, 600000000L, 8000L), asList(9111111111L)))
    assert(asList(row_3(0),row_3(1),row_3(2),row_3(3),row_3(4)) == asList(asList(5000L),asList(600000000L),asList(8000L,9111111111L),asList(20000L),asList(600000000L,8000L,9111111111L)))
    assert(asList(row_4(0)) == asList(asList(5000L, 600087000L, 8000L, 9111111111L, 20000L, 600000000L, 8000L, 977777L)))

    cleanTestData(storePath + "/sdk_output/files2")
  }

  test("test 2-level array for real and double") {
    createComplexTableFor2LevelArray
    val gen = new GenerateFiles()
    gen.twoLevelArrayFile();
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT arrayArrayReal, arrayArrayDouble FROM files2")

    // check number of rows
    assert(actualResult.size == 4)

    //check number of columns in each row
    assert(actualResult(0).size == 2)
    assert(actualResult(1).size == 2)

    var row_1 = (actualResult(0)("arrayArrayReal").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_2 = (actualResult(1)("arrayArrayReal").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_3 = (actualResult(2)("arrayArrayReal").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_4 = (actualResult(3)("arrayArrayReal").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    // check number of child elements in each 2-level arrayArrayDouble
    assert(row_1.length == 2);
    assert(row_2.length == 3);
    assert(row_3.length == 1);
    assert(row_4.length == 4);

    // check actual data in each single-level array
    assert(asList(row_1(0),row_1(1)) == asList(asList(1.111F, 2.2F), asList(9.139F, 2.98F)))
    assert(asList(row_2(0),row_2(1),row_2(2)) == asList(asList(1.111F, 2.2F), asList(9.139F, 2.98F), asList(9.99F)))
    assert(asList(row_3(0)) == asList(asList(9.198F)))
    assert(asList(row_4(0),row_4(1),row_4(2),row_4(3)) == asList(asList(1.111F, 2.2F), asList(9.139F, 2.98F, 4.67F), asList(2.91F, 2.2F), asList(9.139F, 2.98F)))

    row_1 = (actualResult(0)("arrayArrayDouble").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_2 = (actualResult(1)("arrayArrayDouble").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_3 = (actualResult(2)("arrayArrayDouble").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_4 = (actualResult(3)("arrayArrayDouble").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    // check number of child elements in each 2-level arrayArrayDouble
    assert(row_1.length == 2);
    assert(row_2.length == 3);
    assert(row_3.length == 1);
    assert(row_4.length == 1);

    // check actual data in each single-level array
    assert(asList(row_1(0),row_1(1)) == asList(asList(1.111, 2.2),asList(9.139, 2.98989898)))
    assert(asList(row_2(0),row_2(1),row_2(2)) == asList(asList(1.111, 2.2),asList(9.139777, 2.98),asList(9.99888)))
    assert(asList(row_3(0)) == asList(asList(0.1987979)))
    assert(asList(row_4(0)) == asList(asList(1.111, 2.0, 4.67, 2.91, 2.2, 9.139, 2.98)))

    cleanTestData(storePath + "/sdk_output/files2")
  }

  test("test 2-level array for string and boolean") {
    createComplexTableFor2LevelArray
    val gen = new GenerateFiles()
    gen.twoLevelArrayFile();
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT arrayArrayString, arrayArrayBoolean FROM files2")

    // check number of rows
    assert(actualResult.size == 4)

    //check number of columns in each row
    assert(actualResult(0).size == 2)
    assert(actualResult(1).size == 2)

    var row_1 = (actualResult(0)("arrayArrayString").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_2 = (actualResult(1)("arrayArrayString").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_3 = (actualResult(2)("arrayArrayString").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_4 = (actualResult(3)("arrayArrayString").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    // check number of child elements in each 2-level arrayArrayDouble
    assert(row_1.length == 2);
    assert(row_2.length == 2);
    assert(row_3.length == 1);
    assert(row_4.length == 3);

    // check actual data in each single-level array
    assert(asList(row_1(0),row_1(1)) == asList(asList("Japan", "China"), asList("India")))
    assert(asList(row_2(0),row_2(1)) == asList(asList("China", "Brazil"), asList("Paris", "France")))
    assert(asList(row_3(0)) == asList(asList("Japan", "China", "India")))
    assert(asList(row_4(0),row_4(1),row_4(2)) == asList(asList("Japan"), asList("China"), asList("India")))

    row_1 = (actualResult(0)("arrayArrayBoolean").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_2 = (actualResult(1)("arrayArrayBoolean").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_3 = (actualResult(2)("arrayArrayBoolean").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_4 = (actualResult(3)("arrayArrayBoolean").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    // check number of child elements in each 2-level arrayArrayDouble
    assert(row_1.length == 2);
    assert(row_2.length == 2);
    assert(row_3.length == 1);
    assert(row_4.length == 3);

    // check actual data in each single-level array
    assert(asList(row_1(0),row_1(1)) == asList(asList(false, false), asList(false)))
    assert(asList(row_2(0),row_2(1)) == asList(asList(false), asList(true, false)))
    assert(asList(row_3(0)) == asList(asList(false, true, false)))
    assert(asList(row_4(0),row_4(1),row_4(2)) == asList(asList(false), asList(true), asList(false)))

    cleanTestData(storePath + "/sdk_output/files2")
  }

  test("test 3-level array for int, BigInt, Real, Double, String and Boolean columns") {
    createComplexTableFor3LevelArray
    val gen = new GenerateFiles()
    gen.threeLevelArrayFile();
    val actualResult: List[Map[String, Any]] = prestoServer
      .executeQuery("SELECT array3_Int, array3_BigInt, array3_Real, array3_Double, array3_String, array3_Boolean  FROM files3")

    // check total number of 3-level array elements
    assert(actualResult.size == 4)

    // check number of columns in each row
    assert(actualResult(0).size == 6)
    assert(actualResult(1).size == 6)
    assert(actualResult(2).size == 6)
    assert(actualResult(3).size == 6)

    var row_1 = (actualResult(0)("array3_Int").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_2 = (actualResult(1)("array3_Int").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_3 = (actualResult(2)("array3_Int").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    var row_4 = (actualResult(3)("array3_Int").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    // check number of child elements in each 3-level array
    assert(row_1.length == 3);
    assert(row_2.length == 1);
    assert(row_3.length == 2);
    assert(row_4.length == 1);

    assert(asList(row_1(0), row_1(1), row_1(2)) == asList(asList(asList(1,2,3),asList(4,5)),asList(asList(6,7,8),asList(9)),asList(asList(1,2),asList(4,5))))
    assert(asList(row_2(0)) == asList(asList(asList(1,2,3), asList(0,5),asList(1,2,3,4,5), asList(4,5))))
    assert(asList(row_3(0), row_3(1)) == asList(asList(asList(1),asList(0),asList(3)),asList(asList(4,5))))
    assert(asList(row_4(0)) == asList(asList(asList(0,9,0,1,3,2,3,4,7))))


    row_1 = (actualResult(0)("array3_BigInt").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_2 = (actualResult(1)("array3_BigInt").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_3 = (actualResult(2)("array3_BigInt").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_4 = (actualResult(3)("array3_BigInt").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    assert(asList(row_1(0), row_1(1)) == asList(asList(asList(90000L,600000000L), asList(8000L)), asList(asList(911111111L))))
    assert(asList(row_2(0)) == asList(asList(asList(40000L,600000000L,8000L), asList(9111111111L))))
    assert(asList(row_3(0)) == asList(asList(asList(5000L),asList(600000000L),asList(8000L,9111111111L),asList(20000L),asList(600000000L,8000L,9111111111L))))
    assert(asList(row_4(0)) == asList(asList(asList(5000L,600087000L,8000L,9111111111L,20000L,600000000L,8000L,977777L))))

    row_1 = (actualResult(0)("array3_Real").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_2 = (actualResult(1)("array3_Real").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_3 = (actualResult(2)("array3_Real").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_4 = (actualResult(3)("array3_Real").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    assert(asList(row_1(0)) == asList(asList(asList(1.111F,2.2F), asList(9.139F,2.98F))))
    assert(asList(row_2(0), row_2(1)) == asList(asList(asList(1.111F,2.2F), asList(9.139F,2.98F)),asList(asList(9.99F))))
    assert(asList(row_3(0)) == asList(asList(asList(9.198F))))
    assert(asList(row_4(0), row_4(1)) == asList(asList(asList(1.111F,2.2F), asList(9.139F,2.98F,4.67F)), asList(asList(2.91F,2.2F),asList(9.139F,2.98F))))

    row_1 = (actualResult(0)("array3_Double").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_2 = (actualResult(1)("array3_Double").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_3 = (actualResult(2)("array3_Double").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_4 = (actualResult(3)("array3_Double").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    assert(asList(row_1(0),row_1(1)) == asList(asList(asList(1.111,2.2)), asList(asList(9.139,2.98989898))))
    assert(asList(row_2(0), row_2(1)) == asList(asList(asList(1.111,2.2), asList(9.139777,2.98)),asList(asList(9.99888))))
    assert(asList(row_3(0)) == asList(asList(asList(0.1987979))))
    assert(asList(row_4(0)) == asList(asList(asList(1.111,2,4.67, 2.91,2.2, 9.139,2.98))))

    row_1 = (actualResult(0)("array3_String").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_2 = (actualResult(1)("array3_String").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_3 = (actualResult(2)("array3_String").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_4 = (actualResult(3)("array3_String").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    assert(asList(row_1(0),row_1(1)) == asList(asList(asList("Japan", "China"), asList("Brazil", "Paris")),asList(asList("India"))))
    assert(asList(row_2(0)) == asList(asList(asList("China", "Brazil"), asList("Paris", "France"))))
    assert(asList(row_3(0)) == asList(asList(asList("Japan", "China", "India"))))
    assert(asList(row_4(0)) == asList(asList(asList("Japan"), asList("China"), asList("India"))))


    row_1 = (actualResult(0)("array3_Boolean").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_2 = (actualResult(1)("array3_Boolean").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_3 = (actualResult(2)("array3_Boolean").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]
    row_4 = (actualResult(3)("array3_Boolean").asInstanceOf[PrestoArray].getArray()).asInstanceOf[Array[Object]]

    assert(asList(row_1(0),row_1(1)) == asList(asList(asList(false, false), asList(false)), asList(asList(true))))
    assert(asList(row_2(0)) == asList(asList(asList(false), asList(true, false))))
    assert(asList(row_3(0)) == asList(asList(asList(false, true, false))))
    assert(asList(row_4(0)) == asList(asList(asList(false), asList(true), asList(false))))

    cleanTestData(storePath + "/sdk_output/files3")

  }
}
