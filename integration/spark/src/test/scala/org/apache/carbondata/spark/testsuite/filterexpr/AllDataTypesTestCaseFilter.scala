/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.filterexpr

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for filter expression query on multiple datatypes
 * @author N00902756
 *
 */

class AllDataTypesTestCaseFilter extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("CREATE TABLE alldatatypestableFilter (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int) STORED BY 'org.apache.carbondata.format'")
    sql("LOAD DATA local inpath './src/test/resources/data.csv' INTO TABLE alldatatypestableFilter OPTIONS('DELIMITER'= ',', 'QUOTECHAR'= '\"')");
    
    sql("CREATE TABLE alldatatypestableFilter_hive (empno int, empname String, designation String, doj Timestamp, workgroupcategory int, workgroupcategoryname String, deptno int, deptname String, projectcode int, projectjoindate Timestamp, projectenddate Timestamp,attendance int,utilization int,salary int)row format delimited fields terminated by ','")
    sql("LOAD DATA local inpath './src/test/resources/datawithoutheader.csv' INTO TABLE alldatatypestableFilter_hive");

    // coalasce load
    sql("CREATE TABLE coalasce_carbon(imei string,age int,task bigint,name string,country string,city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp,enddate timestamp,PointId double,score decimal(10,3))STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('dictionary_include' =  'imei,name,age,productdate,enddate,country ,city ,sale ,num ,PointId,level,score,task,quest','COLUMNPROPERTIES.level.shared_column'= 'share.level')")
    sql("LOAD DATA INPATH './src/test/resources/big_int_withnull.csv'  INTO TABLE coalasce_carbon options ('DELIMITER'=',','fileheader'='imei,age,task,name,country,city,sale,num,level,quest,productdate,enddate,pointid,score')")
    
    sql("CREATE TABLE coalasce_hive(imei string,age int,task bigint,name string,country string,city string,sale int,num double,level decimal(10,3),quest bigint,productdate timestamp,enddate timestamp,PointId double,score decimal(10,3))ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
    sql("LOAD DATA LOCAL INPATH './src/test/resources/big_int_withnull.csv'  INTO TABLE coalasce_hive")
  }

  test("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableFilter where empname in ('arvind','ayushi') group by empno,empname,utilization") {
    checkAnswer(
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableFilter where empname in ('arvind','ayushi') group by empno,empname,utilization"),
      sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypestableFilter_hive where empname in ('arvind','ayushi') group by empno,empname,utilization"))
  }
  
  test("select empno,empname from alldatatypestableFilter where regexp_replace(workgroupcategoryname, 'er', 'ment') NOT IN ('development')") {
    checkAnswer(
      sql("select empno,empname from alldatatypestableFilter where regexp_replace(workgroupcategoryname, 'er', 'ment') NOT IN ('development')"),
      sql("select empno,empname from alldatatypestableFilter_hive where regexp_replace(workgroupcategoryname, 'er', 'ment') NOT IN ('development')"))
  }
  
  test("select empno,empname from alldatatypescubeFilter where regexp_replace(workgroupcategoryname, 'er', 'ment') != 'development'") {
    checkAnswer(
      sql("select empno,empname from alldatatypestableFilter where regexp_replace(workgroupcategoryname, 'er', 'ment') != 'development'"),
      sql("select empno,empname from alldatatypestableFilter_hive where regexp_replace(workgroupcategoryname, 'er', 'ment') != 'development'"))
  }
  
  test("select imei,name,country,city,productdate,enddate,age,task,sale,num,level,quest,pointid,score from big_int_null_hive where coalesce( name ,1) != 'Lily'") {
    checkAnswer(
     sql("select imei,name,country,city,productdate,enddate,age,task,sale,num,level,quest,pointid,score from coalasce_carbon where coalesce( name ,1) != 'Lily'"),
     sql("select imei,name,country,city,productdate,enddate,age,task,sale,num,level,quest,pointid,score from coalasce_carbon where coalesce( name ,1) != 'Lily'")
    )
    
  }
  override def afterAll {
    sql("drop table alldatatypestableFilter")
    sql("drop table alldatatypestableFilter_hive")
  }
}