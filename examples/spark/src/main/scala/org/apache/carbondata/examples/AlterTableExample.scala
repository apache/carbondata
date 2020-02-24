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

package org.apache.carbondata.examples

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.examples.util.ExampleUtils


/**
 * For alter table relative syntax, you can refer to DDL operation
 * document (data-management-on-carbondata.md)
 */
object AlterTableExample {

  def main(args: Array[String]): Unit = {

    val spark = ExampleUtils.createSparkSession("AlterTableExample")
    exampleBody(spark)
    spark.close()
  }

  def exampleBody(spark : SparkSession): Unit = {
    spark.sql("DROP TABLE IF EXISTS alter_table")
    spark.sql("DROP TABLE IF EXISTS new_alter_table")

    spark.sql(
      s"""
         | CREATE TABLE alter_table(
         | shortField SHORT,
         | intField INT,
         | bigintField LONG,
         | doubleField DOUBLE,
         | stringField STRING,
         | timestampField TIMESTAMP,
         | decimalField DECIMAL(18,2),
         | dateField DATE,
         | charField CHAR(5),
         | floatField FLOAT,
         | complexData ARRAY<STRING>
         | )
         | STORED AS carbondata
       """.stripMargin)

    // Alter table change data type
    spark.sql("DESCRIBE FORMATTED alter_table").show()
    spark.sql("ALTER TABLE alter_table CHANGE intField intField BIGINT").show()

    // Alter table add columns
    spark.sql("DESCRIBE FORMATTED alter_table").show()
    spark.sql("ALTER TABLE alter_table ADD COLUMNS (newField STRING) " +
              "TBLPROPERTIES ('DEFAULT.VALUE.newField'='def')").show()

    // Alter table drop columns
    spark.sql("DESCRIBE FORMATTED alter_table").show()
    spark.sql("ALTER TABLE alter_table DROP COLUMNS (newField)").show()
    spark.sql("DESCRIBE FORMATTED alter_table").show()

    // Alter table rename table name
    spark.sql("SHOW TABLES").show()
    spark.sql("ALTER TABLE alter_table RENAME TO new_alter_table").show()
    spark.sql("SHOW TABLES").show()

    spark.sql("DROP TABLE IF EXISTS alter_table")
    spark.sql("DROP TABLE IF EXISTS new_alter_table")
  }
}
