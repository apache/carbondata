package org.apache.carbondata.hive.integrationtest

import java.io.File

import scala.collection.JavaConversions._

import org.scalatest._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hive.store.CarbonDataStoreCreator
import org.apache.carbondata.server.HiveEmbeddedServer2

class AllDataTypeTest extends FunSuite with BeforeAndAfterAll {
  private val rootPath = new File(this.getClass.getResource("/").getPath
                                  + "../../../..").getCanonicalPath
  private val csvPath = rootPath + "/integration/hive/src/test/resources/alldatatypetest.csv"

  override def beforeAll(): Unit = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)

    CarbonDataStoreCreator.createCarbonStore(rootPath + "/integration/hive/target/store", csvPath)
    HiveEmbeddedServer2.start()

    HiveEmbeddedServer2
      .execute(s"CREATE TABLE ALLDATATYPETEST(ID INT,JOININGDATE DATE,COUNTRY string,NAME STRING," +
               s"PHONETYPE STRING,SERIALNAME STRING,SALARY DOUBLE,BONUS decimal(10,4),DOB " +
               s"timestamp,SHORTFIELD " +
               s"smallint) ROW FORMAT SERDE 'org.apache.carbondata.hive.CarbonHiveSerDe' STORED " +
               s"AS " +
               s"INPUTFORMAT 'org.apache.carbondata.hive.MapredCarbonInputFormat' OUTPUTFORMAT " +
               s"'org.apache.carbondata.hive.MapredCarbonOutputFormat' TBLPROPERTIES ('spark.sql" +
               s".sources.provider'='org.apache.spark.sql.CarbonSource')")


    HiveEmbeddedServer2
      .execute(s"ALTER TABLE ALLDATATYPETEST SET LOCATION " +
               s"'file:///$rootPath/integration/hive/target/store/default/testtable' ")
  }

  override def afterAll(): Unit = {
    HiveEmbeddedServer2.stop()
  }

  test("test the string data type") {
    val actualResult = HiveEmbeddedServer2.execute("SELECT ID FROM ALLDATATYPETEST")
    actualResult.toList.equals(List("1,2,3,4,5,6,7,8,9"))

  }
  test("test greater than expression on shortfield") {
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT SHORTFIELD FROM ALLDATATYPETEST WHERE SHORTFIELD>17")
    actualResult.mkString(",").equals("18")

  }
  test("test less than expression on shortfield") {
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT SHORTFIELD FROM ALLDATATYPETEST WHERE SHORTFIELD<2")
    actualResult.mkString(",").equals("1")
  }
  test("test and operator on shortfield") {
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT PHONETYPE FROM ALLDATATYPETEST WHERE SHORTFIELD>2 AND NAME='sahil' ")
    actualResult.mkString(",").equals("phone610")
  }
  test("test or operator on shortfield") {
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT PHONETYPE FROM ALLDATATYPETEST WHERE SHORTFIELD>12 OR NAME='akash' ")
    actualResult.mkString(",").equals("phone294")
  }

  test("test timestampdatatype with equal to expression") {
    val actualResult = HiveEmbeddedServer2.execute("SELECT DOB FROM ALLDATATYPETEST WHERE ID=8")
    actualResult.toList.equals(List("2008-09-21 11:10:06"))
  }
  test("test timestampdatatype with greater than  expression") {
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT DOB FROM ALLDATATYPETEST WHERE DOB> 2016-01-14")
    assert(
      ("2016-04-14 15:00:09.0,2016-01-14 15:07:09.0,1992-04-14 13:00:09.0,2010-06-19 14:10:06.0," +
       "2013-07-19 12:10:08.0,2008-09-21 11:10:06.0,2009-06-19 15:10:06.0,2001-08-29 13:09:03.0")
        .equals(actualResult.mkString(",")))
  }
  test("test timestampdatatype for and operator") {
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT id FROM ALLDATATYPETEST WHERE DOB< 2016-01-14 OR NAME='anubhav' ")
    assert("1".equals(actualResult.mkString(",")))
  }
  test("test timestampdatatype for or operator") {
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT id FROM ALLDATATYPETEST WHERE DOB< 2016-01-31 OR NAME='sahil' ")
    assert("7".equals(actualResult.mkString(",")))
  }
  test("test decimal for equal to expression") {
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE BONUS=1234.444")
    assert("1".equals(actualResult.mkString(",")))
  }
  test("test decimal for less than  expression") {
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE BONUS<500.88")
    assert("6,9".equals(actualResult.mkString(",")))
  }
  test("test decimal for greater than expression") {
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE BONUS>500.88")
    assert("1,2,3,4,5,7,9".equals(actualResult.mkString(",")))
  }
  test("test double with equal to expression") {
    import org.apache.carbondata.server.HiveEmbeddedServer2
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE SALARY=5000000.00")
    assert("1".equals(actualResult.mkString(",")))
  }
  test("test double with greater than expression") {
    import org.apache.carbondata.server.HiveEmbeddedServer2
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE SALARY<5000000.00")
    assert("2,3,4,5,6,7,8,9,9".equals(actualResult.mkString(",")))
  }
  test("test double with less than expression") {
    import org.apache.carbondata.server.HiveEmbeddedServer2
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE SALARY>15008.00")

    assert("1,2".equals(actualResult.mkString(",")))
  }
  test("test datetype with equal to expression") {
    import org.apache.carbondata.server.HiveEmbeddedServer2
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE JOININGDATE='2015-07-23' ")
    assert("1".equals(actualResult.mkString(",")))
  }
  test("test datetype with greater than expression") {
    import org.apache.carbondata.server.HiveEmbeddedServer2
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE JOININGDATE>'2015-07-21' ")
    assert("1,2,3,4,5,6,7".equals(actualResult.mkString(",")))
  }

  test("test datetype with less than expression") {
    import org.apache.carbondata.server.HiveEmbeddedServer2
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE JOININGDATE<'2015-07-31' AND NAME='sahil' ")
    assert("7".equals(actualResult.mkString(",")))
  }
  test("test datetype with or operator") {
    import org.apache.carbondata.server.HiveEmbeddedServer2
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE JOININGDATE>'2015-07-31' OR NAME='sahil' ")
    assert("7".equals(actualResult.mkString(",")))
  }
  test("test datetype with and operator") {
    import org.apache.carbondata.server.HiveEmbeddedServer2
    val actualResult = HiveEmbeddedServer2
      .execute("SELECT ID FROM ALLDATATYPETEST WHERE JOININGDATE>'2015-07-01' AND NAME='akash' ")
    assert("6".equals(actualResult.mkString(",")))
  }
}

