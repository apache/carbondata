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
package org.apache.carbondata.spark.testsuite.detailquery

import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class ExpressionWithNullTestCase extends QueryTest with BeforeAndAfterAll {

  override def beforeAll() {
    sql("drop table if exists expression_test")
    sql("drop table if exists expression_test_hive")
    sql("drop table if exists expression")
    sql("drop table if exists expression_hive")
    sql("create table expression_test (id int, name string, number int) STORED AS carbondata")
    sql(s"load data local inpath '$resourcesPath/filter/datawithnull.csv' into table expression_test options('FILEHEADER'='id,name,number')")
    sql("""create table expression_test_hive (id int, name string, number int) row format delimited fields terminated by ','""")
    sql(s"load data local inpath '$resourcesPath/filter/datawithnull.csv' into table expression_test_hive")

    sql("""create table expression (id int, name string, number int) row format delimited fields terminated by ','""")
    sql(s"load data local inpath '$resourcesPath/filter/datawithoutnull.csv' into table expression")
    sql("""create table expression_hive (id int, name string, number int) row format delimited fields terminated by ','""")
    sql(s"load data local inpath '$resourcesPath/filter/datawithoutnull.csv' into table expression_hive")
  }

  override def afterAll = {
    sql("drop table if exists expression_test")
    sql("drop table if exists expression_test_hive")
    sql("drop table if exists expression")
    sql("drop table if exists expression_hive")
  }

  test("test to check in expression with null values") {
    checkAnswer(sql("select * from expression_test where id in (1,2,'', NULL, ' ')"), sql("select * from expression_test_hive where id in (1,2,' ', NULL, ' ')"))
    checkAnswer(sql("select * from expression_test where id in (1,2,'')"), sql("select * from expression_test_hive where id in (1,2,'')"))
    checkAnswer(sql("select * from expression_test where id in ('')"), sql("select * from expression_test_hive where id in ('')"))
    checkAnswer(sql("select * from expression_test where number in (NULL)"), sql("select * from expression_test_hive where number in (null)"))
    checkAnswer(sql("select * from expression_test where number in (2)"), sql("select * from expression_test_hive where number in (2)"))
    checkAnswer(sql("select * from expression_test where number in (1,null)"), sql("select * from expression_test_hive where number in (1,null)"))
    checkAnswer(sql("select * from expression where number in (1,null)"), sql("select * from expression_hive where number in (1,null)"))
    checkAnswer(sql("select * from expression where id in (3)"), sql("select * from expression_hive where id in (3)"))
    checkAnswer(sql("select * from expression where id in ('2')"), sql("select * from expression_hive where id in ('2')"))
    checkAnswer(sql("select * from expression where id in (cast('2' as int))"), sql("select * from expression_hive where id in (cast('2' as int))"))
    checkAnswer(sql("select * from expression_test where id in (3)"), sql("select * from expression_test_hive where id in (3)"))
    checkAnswer(sql("select * from expression_test where id in ('2')"), sql("select * from expression_test_hive where id in ('2')"))
    checkAnswer(sql("select * from expression_test where id in (cast('2' as int))"), sql("select * from expression_test_hive where id in (cast('2' as int))"))
    checkAnswer(sql("select * from expression_test where id in (cast('null' as int))"), sql("select * from expression_test_hive where id in (cast('null' as int))"))
    checkAnswer(sql("select * from expression_test where id in (1,2,NULL)"), sql("select * from expression_test_hive where id in (1,2,NULL)"))
    checkAnswer(sql("select * from expression_test where id in (NULL)"), sql("select * from expression_test_hive where id in (NULL)"))
    checkAnswer(sql("select * from expression_test where number in (cast('null' as int))"), sql("select * from expression_test_hive where number in (cast('null' as int))"))
    checkAnswer(sql("select * from expression_test where number in (1,2, cast('NULL' as int), cast('3' as int))"), sql("select * from expression_test_hive where number in (1,2, cast('NULL' as int),cast('3' as int))"))
    checkAnswer(sql("select * from expression_test where cast(number as int) IN(1,null)"), sql("select * from expression_test_hive where cast(number as int) IN(1,null)"))

  }

  test("test to check not in expression with null values") {
    checkAnswer(sql("select * from expression_test where id not in (1,2,'', NULL, ' ')"), sql("select * from expression_test_hive where id not in (1,2,' ', NULL, ' ')"))
    checkAnswer(sql("select * from expression_test where id not in (1,2,'')"), sql("select * from expression_test_hive where id not in (1,2,'')"))
    checkAnswer(sql("select * from expression_test where id not in ('')"), sql("select * from expression_test_hive where id not in ('')"))
    checkAnswer(sql("select * from expression_test where number not in (null)"), sql("select * from expression_test_hive where number not in (null)"))
    checkAnswer(sql("select * from expression_test where number not in (1,null)"), sql("select * from expression_test_hive where number not in (1,null)"))
    checkAnswer(sql("select * from expression where number not in (1,null)"), sql("select * from expression_hive where number not in (1,null)"))
    checkAnswer(sql("select * from expression where id not in (3)"), sql("select * from expression_hive where id not in (3)"))
    checkAnswer(sql("select * from expression where id not in ('2')"), sql("select * from expression_hive where id not in ('2')"))
    checkAnswer(sql("select * from expression where id not in (cast('2' as int))"), sql("select * from expression_hive where id not in (cast('2' as int))"))
    checkAnswer(sql("select * from expression_test where id not in (3)"), sql("select * from expression_test_hive where id not in (3)"))
    checkAnswer(sql("select * from expression_test where id not in ('2')"), sql("select * from expression_test_hive where id not in ('2')"))
    checkAnswer(sql("select * from expression_test where id not in (cast('2' as int))"), sql("select * from expression_test_hive where id not in (cast('2' as int))"))
    checkAnswer(sql("select * from expression_test where id not in (cast('null' as int))"), sql("select * from expression_test_hive where id not in (cast('null' as int))"))
    checkAnswer(sql("select * from expression_test where id not in (1,2,NULL)"), sql("select * from expression_test_hive where id not in (1,2,NULL)"))
    checkAnswer(sql("select * from expression_test where id not in (NULL)"), sql("select * from expression_test_hive where id not in (NULL)"))
    checkAnswer(sql("select * from expression_test where number not in (2, null)"), sql("select * from expression_test_hive where number not in (2, null)"))
    checkAnswer(sql("select * from expression_test where number not in (cast('2' as int), cast('null' as int))"), sql("select * from expression_test_hive where number not in (cast('2' as int), cast('null' as int))"))

  }

  test("test to check equals expression with null values") {
    checkAnswer(sql("select * from expression_test where id=''"), sql("select * from expression_test_hive where id=''"))
    checkAnswer(sql("select * from expression_test where id=' '"), sql("select * from expression_test_hive where id=' '"))
    checkAnswer(sql("select * from expression_test where number=null"), sql("select * from expression_test_hive where number=null"))
    checkAnswer(sql("select * from expression_test where id=3"), sql("select * from expression_test_hive where id=3"))
    checkAnswer(sql("select * from expression where number=null"), sql("select * from expression_hive where number=null"))
    checkAnswer(sql("select * from expression where id=2"), sql("select * from expression_hive where id=2"))
    checkAnswer(sql("select * from expression_test where id='2'"), sql("select * from expression_test_hive where id='2'"))
    checkAnswer(sql("select * from expression_test where cast(id as int)='2'"), sql("select * from expression_test_hive where cast(id as int)='2'"))
    checkAnswer(sql("select * from expression where id='2'"), sql("select * from expression_hive where id='2'"))
    checkAnswer(sql("select * from expression where cast(id as int)='null'"), sql("select * from expression_hive where cast(id as int)='null'"))
  }

  test("test to check not equals expression with null values") {
    checkAnswer(sql("select * from expression_test where name != ''"), sql("select * from expression_test_hive where name != ''"))
    checkAnswer(sql("select * from expression_test where name != ' '"), sql("select * from expression_test_hive where name != ' '"))
    checkAnswer(sql("select * from expression_test where id=3"), sql("select * from expression_test_hive where id=3"))
    checkAnswer(sql("select * from expression_test where number is not null"), sql("select * from expression_test_hive where number is not null"))
    checkAnswer(sql("select * from expression where number is not null"), sql("select * from expression_hive where number is not null"))
    checkAnswer(sql("select * from expression where id!=2"), sql("select * from expression_hive where id!=2"))
    checkAnswer(sql("select * from expression_test where id!='2'"), sql("select * from expression_test_hive where id!='2'"))
    checkAnswer(sql("select * from expression_test where cast(id as int)!='2'"), sql("select * from expression_test_hive where cast(id as int)!='2'"))
    checkAnswer(sql("select * from expression where id!='2'"), sql("select * from expression_hive where id!='2'"))
    checkAnswer(sql("select * from expression where cast(id as int)!='2'"), sql("select * from expression_hive where cast(id as int)!='2'"))
    checkAnswer(sql("select * from expression where cast(id as int)!='null'"), sql("select * from expression_hive where cast(id as int)!='null'"))
  }

  test("test to check greater than equals to expression with null values") {
    checkAnswer(sql("select * from expression_test where id >= ''"), sql("select * from expression_test_hive where id >= ''"))
    checkAnswer(sql("select * from expression_test where id >= ' '"), sql("select * from expression_test_hive where id >= ' '"))
    checkAnswer(sql("select * from expression_test where number >= null"), sql("select * from expression_test_hive where number >= null"))
    checkAnswer(sql("select * from expression where number >= null"), sql("select * from expression_hive where number >= null"))
    checkAnswer(sql("select * from expression where id>=2"), sql("select * from expression_hive where id>=2"))
    checkAnswer(sql("select * from expression_test where id>='2'"), sql("select * from expression_test_hive where id>='2'"))
    checkAnswer(sql("select * from expression_test where cast(id as int)>='2'"), sql("select * from expression_test_hive where cast(id as int)>='2'"))
    checkAnswer(sql("select * from expression where id>='2'"), sql("select * from expression_hive where id>='2'"))
    checkAnswer(sql("select * from expression where cast(id as int)>='2'"), sql("select * from expression_hive where cast(id as int)>='2'"))
    checkAnswer(sql("select * from expression where cast(id as int)>='null'"), sql("select * from expression_hive where cast(id as int)>='null'"))
  }

  test("test to check less than equals to expression with null values") {
    checkAnswer(sql("select * from expression_test where id <= ''"), sql("select * from expression_test_hive where id <= ''"))
    checkAnswer(sql("select * from expression_test where id <= ' '"), sql("select * from expression_test_hive where id <= ' '"))
    checkAnswer(sql("select * from expression_test where number <= null"), sql("select * from expression_test_hive where number <= null"))
    checkAnswer(sql("select * from expression where number <= null"), sql("select * from expression_hive where number <= null"))
    checkAnswer(sql("select * from expression where id<=2"), sql("select * from expression_hive where id<=2"))
    checkAnswer(sql("select * from expression_test where id<='2'"), sql("select * from expression_test_hive where id<='2'"))
    checkAnswer(sql("select * from expression_test where cast(id as int)<='2'"), sql("select * from expression_test_hive where cast(id as int)<='2'"))
    checkAnswer(sql("select * from expression where id<='2'"), sql("select * from expression_hive where id<='2'"))
    checkAnswer(sql("select * from expression where cast(id as int)<='2'"), sql("select * from expression_hive where cast(id as int)<='2'"))
    checkAnswer(sql("select * from expression where cast(id as int)<='null'"), sql("select * from expression_hive where cast(id as int)<='null'"))
  }

  test("test to check greater than expression with null values") {
    checkAnswer(sql("select * from expression_test where id > ''"), sql("select * from expression_test_hive where id > ''"))
    checkAnswer(sql("select * from expression_test where id > ' '"), sql("select * from expression_test_hive where id > ' '"))
    checkAnswer(sql("select * from expression_test where number > null"), sql("select * from expression_test_hive where number > null"))
    checkAnswer(sql("select * from expression where number > null"), sql("select * from expression_hive where number > null"))
    checkAnswer(sql("select * from expression where id>2"), sql("select * from expression_hive where id>2"))
    checkAnswer(sql("select * from expression_test where id>'2'"), sql("select * from expression_test_hive where id>'2'"))
    checkAnswer(sql("select * from expression_test where cast(id as int)>'2'"), sql("select * from expression_test_hive where cast(id as int)>'2'"))
    checkAnswer(sql("select * from expression where id>'2'"), sql("select * from expression_hive where id>'2'"))
    checkAnswer(sql("select * from expression where cast(id as int)>'2'"), sql("select * from expression_hive where cast(id as int)>'2'"))
    checkAnswer(sql("select * from expression where cast(id as int)>'null'"), sql("select * from expression_hive where cast(id as int)>'null'"))
  }

  test("test to check less than expression with null values") {
    checkAnswer(sql("select * from expression_test where id < ''"), sql("select * from expression_test_hive where id < ''"))
    checkAnswer(sql("select * from expression_test where id < ' '"), sql("select * from expression_test_hive where id < ' '"))
    checkAnswer(sql("select * from expression_test where number < null"), sql("select * from expression_test_hive where number < null"))
    checkAnswer(sql("select * from expression where number < null"), sql("select * from expression_hive where number < null"))
    checkAnswer(sql("select * from expression where id<2"), sql("select * from expression_hive where id<2"))
    checkAnswer(sql("select * from expression_test where id<'2'"), sql("select * from expression_test_hive where id<'2'"))
    checkAnswer(sql("select * from expression_test where cast(id as int)<'2'"), sql("select * from expression_test_hive where cast(id as int)<'2'"))
    checkAnswer(sql("select * from expression where id<'2'"), sql("select * from expression_hive where id<'2'"))
    checkAnswer(sql("select * from expression where cast(id as int)<'2'"), sql("select * from expression_hive where cast(id as int)<'2'"))
    checkAnswer(sql("select * from expression where cast(id as int)<'null'"), sql("select * from expression_hive where cast(id as int)<'null'"))
  }

}
