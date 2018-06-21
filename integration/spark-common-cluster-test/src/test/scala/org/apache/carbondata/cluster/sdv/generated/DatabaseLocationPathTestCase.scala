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

package org.apache.carbondata.cluster.sdv.generated

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util._
import org.apache.spark.sql.test.TestQueryExecutor


class DatabaseLocationPathTestCase  extends QueryTest  {

  private val path = TestQueryExecutor
                       .resourcesPath + "/tmp"
  //test creating external db
  test("External_DB_TC_01", Include) {
  sql(s"CREATE DATABASE EXTERNAL_DB LOCATION '$path' ")
  assert(sqlContext.sparkSession.catalog.listDatabases().filter(database=>database.name.equals("external_db")).count()>0)
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")
  }

  //create the table inside external db
  test("External_DB_TC_02", Include) {
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")
    sql("DROP TABLE IF EXISTS EMPLOYEE")

    sql(s"CREATE DATABASE EXTERNAL_DB LOCATION '$path' ")
    sql("USE EXTERNAL_DB")
    sql("CREATE TABLE EMPLOYEE(ID INT) STORED BY 'CARBONDATA' ")
    assert(sqlContext.sparkSession.catalog.listTables("EXTERNAL_DB").filter(tableName => tableName.name.equals("EMPLOYEE".toLowerCase)).count()>0)
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")

  }
  //load data inside table created in externaldb
  test("External_DB_TC_03", Include) {
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")

    sql(s"CREATE DATABASE EXTERNAL_DB LOCATION '$path' ")
    sql("USE EXTERNAL_DB")
    sql("CREATE TABLE EMPLOYEE(ID INT) STORED BY 'CARBONDATA' ")
    sql("INSERT INTO EMPLOYEE VALUES(1)")
    sql("INSERT INTO EMPLOYEE VALUES(2)")
    sql("INSERT INTO EMPLOYEE VALUES(3)")

    checkAnswer(sql("select * from employee"),Seq(Row(1),Row(2),Row(3)))
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")

  }
  //User must be able to create to create database at same location same as hive does
  test("EXTERNAL_DB_TC_04",Include){
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")
    sql(s"CREATE DATABASE EXTERNAL_DB LOCATION '$path' ")
    sql(s"CREATE DATABASE EXTERNAL_DB1 LOCATION '$path' ")
    assert(sqlContext.sparkSession.catalog.listDatabases().filter(database=>database.name.equals("external_db")).count()>0)
    assert(sqlContext.sparkSession.catalog.listDatabases().filter(database=>database.name.equals("external_db1")).count()>0)
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB1 CASCADE")

  }
  //User must be able to drop database at external location
  test("EXTERNAL_DB_TC_05",Include){
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")
    sql(s"CREATE DATABASE EXTERNAL_DB LOCATION '$path' ")
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")

    assert(sqlContext.sparkSession.catalog.listDatabases().filter(database=>database.name.equals("external_db")).count()==0)

  }
  //User must be able to alter datatype of a table created at external location
  test("EXTERNAL_DB_TC_06",Include){
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")
    sql(s"CREATE DATABASE EXTERNAL_DB LOCATION '$path' ")
    sql("USE EXTERNAL_DB")
    sql("CREATE TABLE EMPLOYEE(ID INT) STORED BY 'CARBONDATA' ")
    sql("ALTER TABLE EMPLOYEE CHANGE id id BIGINT")
    sql("DESCRIBE EMPLOYEE").show()
   assert(sql("DESCRIBE EMPLOYEE").collect()
     .exists(row => row.get(1).asInstanceOf[String].trim.contains("bigint")))
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")
  }
  //User must be able to add a new column in a table created at external location
  test("EXTERNAL_DB_TC_07",Include){
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")
    sql(s"CREATE DATABASE EXTERNAL_DB LOCATION '$path' ")
    sql("USE EXTERNAL_DB")
    sql("CREATE TABLE EMPLOYEE(ID INT) STORED BY 'CARBONDATA' ")
    sql("ALTER TABLE EMPLOYEE ADD COLUMNS(NAME STRING)")
    assert(sql("DESCRIBE EMPLOYEE").collect()
      .exists(row => row.get(1).asInstanceOf[String].trim.contains("string")))
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")


  }
  //User must be able to drop a  column in a table created at external location
  test("EXTERNAL_DB_TC_08",Include){
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")
    sql(s"CREATE DATABASE EXTERNAL_DB LOCATION '$path' ")
    sql("USE EXTERNAL_DB")
    sql("CREATE TABLE EMPLOYEE(ID INT) STORED BY 'CARBONDATA' ")
    sql("ALTER TABLE EMPLOYEE ADD COLUMNS(NAME STRING)")
    sql("ALTER TABLE EMPLOYEE DROP COLUMNS(NAME)")

    assert(!sql("DESCRIBE EMPLOYEE").collect()
      .exists(row => row.get(1).asInstanceOf[String].trim.contains("string")))
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")


  }
  //User must be able to rename a table created at external location
  test("EXTERNAL_DB_TC_09",Include){
    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")
    sql(s"CREATE DATABASE EXTERNAL_DB LOCATION '$path' ")
    sql("USE EXTERNAL_DB")
    sql("CREATE TABLE EMPLOYEE(ID INT) STORED BY 'CARBONDATA' ")
    sql("ALTER TABLE EMPLOYEE RENAME TO NEW_EMPLOYEE")

    assert(sqlContext.sparkSession.catalog.listTables("EXTERNAL_DB").filter(tableName => tableName.name.equals("NEW_EMPLOYEE".toLowerCase)).count()>0)

    sql("DROP DATABASE IF EXISTS EXTERNAL_DB CASCADE")


  }
}
