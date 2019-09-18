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

package org.apache.carbondata.integration.spark.testsuite.preaggregate

import org.apache.spark.sql.test.util.CarbonQueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException
import org.apache.carbondata.spark.exception.ProcessMetaDataException

class TestPreAggregateDrop extends CarbonQueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists maintable")
    sql("create table maintable (a string, b string, c string) stored by 'carbondata'")
  }

  test("create and drop preaggregate table") {
    sql(
      "create datamap preagg1 on table maintable using 'preaggregate' as select" +
      " a,sum(b) from maintable group by a")
    sql("drop datamap if exists preagg1 on table maintable")
    checkExistence(sql("SHOW DATAMAP ON TABLE maintable"), false, "maintable_preagg1")
  }

  test("dropping 1 aggregate table should not drop others") {
    sql(
      "create datamap preagg1 on table maintable using 'preaggregate' as select" +
      " a,sum(b) from maintable group by a")
    sql(
      "create datamap preagg2 on table maintable using 'preaggregate' as select" +
      " a,sum(c) from maintable group by a")
    sql("drop datamap if exists preagg2 on table maintable")
    val showdatamaps =sql("show datamap on table maintable")
    checkExistence(showdatamaps, false, "maintable_preagg2")
    checkExistence(showdatamaps, true, "maintable_preagg1")
  }

  test("drop datamap which is not existed") {
    intercept[NoSuchDataMapException] {
      sql("drop datamap newpreagg on table maintable")
    }
  }

  test("drop datamap with same name on different tables") {
    sql("drop table if exists maintable1")
    sql("create datamap preagg_same on table maintable using 'preaggregate' as select" +
    " a,sum(c) from maintable group by a")
    sql("create table maintable1 (a string, b string, c string) stored by 'carbondata'")
    sql("create datamap preagg_same on table maintable1 using 'preaggregate' as select" +
    " a,sum(c) from maintable1 group by a")

    sql("drop datamap preagg_same on table maintable")
    val showDataMaps = sql("SHOW DATAMAP ON TABLE maintable1")
    checkExistence(showDataMaps, false, "maintable_preagg_same")
    checkExistence(showDataMaps, true, "maintable1_preagg_same")
    sql("drop datamap preagg_same on table maintable1")
    checkExistence(sql("SHOW DATAMAP ON TABLE maintable1"), false, "maintable1_preagg_same")
    sql("drop table if exists maintable1")
  }

  test("drop datamap and create again with same name") {
    sql("create datamap preagg_same1 on table maintable using 'preaggregate' as select" +
    " a,sum(c) from maintable group by a")

    sql("drop datamap preagg_same1 on table maintable")
    checkExistence(sql("SHOW DATAMAP ON TABLE maintable"), false, "maintable_preagg_same1")
    sql("create datamap preagg_same1 on table maintable using 'preaggregate' as select" +
        " a,sum(c) from maintable group by a")
    val showDatamaps =sql("show datamap on table maintable")
    checkExistence(showDatamaps, true, "maintable_preagg_same1")
    sql("drop datamap preagg_same1 on table maintable")
  }

  test("drop main table and check if preaggreagte is deleted") {
    sql(
      "create datamap preagg2 on table maintable using 'preaggregate' as select" +
      " a,sum(c) from maintable group by a")
    sql("drop table if exists maintable")
    checkExistence(sql("show tables"), false, "maintable_preagg1", "maintable", "maintable_preagg2")
  }

  test("drop datamap with 'if exists' when datamap not exists") {
    sql("DROP TABLE IF EXISTS maintable")
    sql("CREATE TABLE maintable (a STRING, b STRING, c STRING) STORED BY 'carbondata'")
    sql("DROP DATAMAP IF EXISTS not_exists_datamap ON TABLE maintable")
    checkExistence(sql("DESCRIBE FORMATTED maintable"), false, "not_exists_datamap")
  }

  test("drop datamap without 'if exists' when datamap not exists") {
    sql("DROP TABLE IF EXISTS maintable")
    sql("CREATE TABLE maintable (a STRING, b STRING, c STRING) STORED BY 'carbondata'")
    sql("DROP DATAMAP IF EXISTS not_exists_datamap ON TABLE maintable")
    val e = intercept[NoSuchDataMapException] {
      sql("DROP DATAMAP not_exists_datamap ON TABLE maintable")
    }
    assert(e.getMessage.equals(
      "Datamap with name not_exists_datamap does not exist"))
  }

  test("drop datamap without 'if exists' when main table not exists") {
    sql("DROP TABLE IF EXISTS maintable")
    val e = intercept[ProcessMetaDataException] {
      sql("DROP DATAMAP preagg3 ON TABLE maintable")
    }
    assert(e.getMessage.contains("Table or view 'maintable' not found in"))
  }

  test("drop datamap with 'if exists' when main table not exists") {
    sql("DROP TABLE IF EXISTS maintable")
    val e = intercept[ProcessMetaDataException] {
      sql("DROP DATAMAP IF EXISTS preagg3 ON TABLE maintable")
    }
    assert(e.getMessage.contains("Table or view 'maintable' not found in"))
  }

  test("drop preaggregate datamap whose main table has other type datamaps") {
    sql("drop table if exists maintable")
    sql("create table maintable (a string, b string, c string) stored by 'carbondata'")
    sql(
      "create datamap bloom1 on table maintable using 'bloomfilter'" +
      " dmproperties('index_columns'='a')")
    sql(
      "create datamap preagg1 on table maintable using 'preaggregate' as select" +
      " a,sum(b) from maintable group by a")
    sql("drop datamap if exists bloom1 on table maintable")
    sql("drop datamap if exists preagg1 on table maintable")
    checkExistence(sql("SHOW DATAMAP ON TABLE maintable"), false, "preagg1", "bloom1")
    sql(
      "create datamap bloom1 on table maintable using 'bloomfilter'" +
      " dmproperties('index_columns'='a')")
    sql(
      "create datamap preagg1 on table maintable using 'preaggregate' as select" +
      " a,sum(b) from maintable group by a")
    sql("drop datamap if exists preagg1 on table maintable")
    sql("drop datamap if exists bloom1 on table maintable")
    checkExistence(sql("SHOW DATAMAP ON TABLE maintable"), false, "preagg1", "bloom1")
    sql(
      "create datamap bloom1 on table maintable using 'bloomfilter'" +
      " dmproperties('index_columns'='a')")
    sql(
      "create datamap preagg1 on table maintable using 'preaggregate' as select" +
      " a,sum(b) from maintable group by a")
    sql("drop table if exists maintable")
    checkExistence(sql("show tables"), false, "maintable_preagg1", "maintable")
    checkExistence(sql("show datamap"), false, "preagg1", "bloom1")
  }

  override def afterAll() {
    sql("drop table if exists maintable")
    sql("drop table if exists maintable1")
  }
  
}
