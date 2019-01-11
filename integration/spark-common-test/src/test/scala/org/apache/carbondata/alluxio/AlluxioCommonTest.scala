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

import alluxio.master.LocalAlluxioCluster
import java.sql.{Timestamp, Date => sqlData}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.LongType
import scala.collection.mutable

import org.apache.carbondata.alluxio.util.AlluxioUtilTest
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

object AlluxioCommonTest extends AlluxioUtilTest {

    val carbonAndAlluxio = "/CarbonAndAlluxio"

    def testAllDataType(tableNameForAllType: String) {
        try {
            // Count
            checkAnswer(sql(s"SELECT count(*) FROM $tableNameForAllType"), Seq(Row(3)))

            // Select
            checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField,booleanField from $tableNameForAllType where smallIntField = -32768"),
                Seq(Row(-32768, -2147483648, -9223372036854775808L, -2147483648.1, -9223372036854775808.1, BigDecimal("-9223372036854775808.1234"), Timestamp.valueOf("2017-06-11 00:00:01"), sqlData.valueOf("2017-06-11"), "abc1", "abcd1", "abcde1", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "1")), Row("a", "b", "1"), true)))

            checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField,booleanField from $tableNameForAllType where smallIntField = 128"),
                Seq(Row(128, 32768, 2147483648L, 2147483647.1, 9223372036854775807.1, BigDecimal("9223372036854775807.1234"), Timestamp.valueOf("2017-06-12 23:59:59"), sqlData.valueOf("2017-06-12"), "abc2", "abcd2", "abcde2", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "2")), Row("a", "b", "2"), false)))

            checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField,booleanField from $tableNameForAllType where smallIntField = 32767"),
                Seq(Row(32767, 2147483647, 9223372036854775807L, 2147483648.1, 9223372036854775808.1, BigDecimal("9223372036854775808.1234"), Timestamp.valueOf("2017-06-13 23:59:59"), sqlData.valueOf("2017-06-13"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"), true)))

            // Insert of IUD
            sql(s"insert into $tableNameForAllType values(123,2147483640,9223372036854775800,2147483648.0,9223372036854775808.0,'9223372036854775808.1230','2017-06-13 23:59:50','2017-06-10'," +
                    "'abc3','abcd3','abcde3','a\001b\001c\0013','a\001b\0013',false)")

            checkAnswer(sql(s"SELECT count(*) FROM $tableNameForAllType"), Seq(Row(4)))
            checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField,booleanField from $tableNameForAllType where smallIntField = 123"),
                Seq(Row(123, 2147483640, 9223372036854775800L, 2147483648.0, 9223372036854775808.0, BigDecimal("9223372036854775808.1230"), Timestamp.valueOf("2017-06-13 23:59:50"), sqlData.valueOf("2017-06-10"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"), false)))

            // Update of IUD
            sql(s"UPDATE $tableNameForAllType SET (booleanfield)=(true) WHERE smallIntField=123").collect()
            checkAnswer(sql(s"select smallIntField,intField,bigIntField,floatField,doubleField,decimalField,timestampField,dateField,stringField,varcharField,charField,arrayField,structField,booleanField from $tableNameForAllType where smallIntField = 123"),
                Seq(Row(123, 2147483640, 9223372036854775800L, 2147483648.0, 9223372036854775808.0, BigDecimal("9223372036854775808.1230"), Timestamp.valueOf("2017-06-13 23:59:50"), sqlData.valueOf("2017-06-10"), "abc3", "abcd3", "abcde3", new mutable.WrappedArray.ofRef[String](Array("a", "b", "c", "3")), Row("a", "b", "3"), true)))

            // Delete of IUD
            checkAnswer(sql(s"SELECT count(*) FROM $tableNameForAllType"), Seq(Row(4)))
            sql(s"DELETE FROM $tableNameForAllType WHERE smallIntField=123")
            checkAnswer(sql(s"SELECT count(*) FROM $tableNameForAllType"), Seq(Row(3)))

            // Order by
            val result = sql(s"SELECT intfield,stringfield FROM $tableNameForAllType order by intfield")
                    .collect()
                    .map { each => each.get(0) }
            var temp: Int = Integer.MIN_VALUE
            for (i <- 0 until (result.length)) {
                assert(Integer.compare(Integer.valueOf(result(i).toString), temp) >= 0)
                temp = Integer.valueOf(result(i).toString)
            }
        } catch {
            case e: Exception =>
                e.printStackTrace()
                assert(false)
        } finally {
            sql("DROP TABLE IF EXISTS " + tableNameForAllType)
        }
    }

    def testCreateTableAndSegment(tableName: String, path: String, remoteFile: String,
                                  localAlluxioCluster: LocalAlluxioCluster) {
        try {
            // Check table properties
            val descLoc = sql(s"describe formatted $tableName").collect
            descLoc.find(_.get(0).toString.contains("Local Dictionary Threshold")) match {
                case Some(row) => assert(row.get(1).toString.contains("1100"))
                case None => assert(false)
            }
            descLoc.find(_.get(0).toString.contains("Local Dictionary Enabled")) match {
                case Some(row) => assert(row.get(1).toString.contains("true"))
                case None => assert(false)
            }
            descLoc.find(_.get(0).toString.contains("Local Dictionary Include")) match {
                case Some(row) => assert(row.get(1).toString.contains("country"))
                case None => assert(false)
            }
            descLoc.find(_.get(0).toString.contains("AUTO_LOAD_MERGE")) match {
                case Some(row) => assert(row.get(1).toString.contains("true"))
                case None => assert(false)
            }
            descLoc.find(_.get(0).toString.contains("Sort Columns")) match {
                case Some(row) => assert(row.get(1).toString.contains("salary"))
                case None => assert(false)
            }

            // Specify date format based on raw data
            CarbonProperties.getInstance()
                    .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")


            // Check segment
            val segments = sql(s"SHOW SEGMENTS FOR TABLE $tableName")
            val SegmentSequenceIds = segments.collect().map { each => (each.toSeq) (0) }
            assert(SegmentSequenceIds.contains("0"))
            assert(!SegmentSequenceIds.contains("1"))
            assert(!SegmentSequenceIds.contains("0.1"))
            assert(SegmentSequenceIds.length == 1)

            val Status = segments.collect().map { each => (each.toSeq) (1) }
            assert(!Status.contains("Compacted"))

            val df = sql(
                s"""
                   | SELECT country, count(salary) AS amount
                   | FROM $tableName
                   | WHERE country IN ('china','france')
                   | GROUP BY country
             """.stripMargin)
            checkAnswer(df, Seq(Row("france", 101), Row("china", 849)))
            checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(1000)))

            // Add segments
            sql(s"""LOAD DATA LOCAL INPATH '$path' INTO TABLE $tableName""")
            val segmentsAdded = sql(s"SHOW SEGMENTS FOR TABLE $tableName")
            val SegmentSequenceIdsAdded = segmentsAdded.collect().map { each => (each.toSeq) (0) }
            assert(SegmentSequenceIdsAdded.contains("0"))
            assert(SegmentSequenceIdsAdded.contains("1"))
            assert(SegmentSequenceIdsAdded.length == 2)
            checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(2000)))

            // Delete segment
            sql(s"DELETE FROM TABLE $tableName WHERE SEGMENT.ID IN (0)")
            sql(s"CLEAN FILES FOR TABLE $tableName").collect()
            val segmentsDeleted = sql(s"SHOW SEGMENTS FOR TABLE $tableName")
            val SegmentSequenceIdsDeleted = segmentsDeleted.collect().map { each => (each.toSeq) (0) }
            assert(!SegmentSequenceIdsDeleted.contains("0"))
            assert(SegmentSequenceIdsDeleted.contains("1"))
            assert(SegmentSequenceIdsDeleted.length == 1)
            checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(1000)))

            // QUERY DATA WITH SPECIFIED SEGMENTS
            sql(s"""LOAD DATA LOCAL INPATH '$path' INTO TABLE $tableName""")
            val SegmentSequenceIdsQuery = sql(s"SHOW SEGMENTS FOR TABLE $tableName")
                    .collect()
                    .map { each => (each.toSeq) (0) }
            assert(SegmentSequenceIdsQuery.contains("2"))
            assert(SegmentSequenceIdsQuery.contains("1"))
            assert(SegmentSequenceIdsQuery.length == 2)
            checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(2000)))
            sql(s"SET carbon.input.segments.default.$tableName=1")
            checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(1000)))
            sql(s"SET carbon.input.segments.default.$tableName=*")
            checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(2000)))

            // Auto merge
            sql(s"""LOAD DATA LOCAL INPATH '$path' INTO TABLE $tableName""")
            sql(s"""LOAD DATA LOCAL INPATH '$path' INTO TABLE $tableName""")

            val segmentsMerged = sql(s"SHOW SEGMENTS FOR TABLE $tableName")
            val SegmentSequenceIdsMerged = segmentsMerged.collect().map { each => (each.toSeq) (0) }
            assert(!SegmentSequenceIdsMerged.contains("0"))
            assert(SegmentSequenceIdsMerged.contains("1"))
            assert(SegmentSequenceIdsMerged.contains("1.1"))
            assert(SegmentSequenceIdsMerged.length == 5)
            val StatusMerged = segmentsMerged.collect().map { each => (each.toSeq) (1) }
            assert(StatusMerged.contains("Compacted"))
            assert(StatusMerged.contains("Success"))

            // Clean files
            sql(s"CLEAN FILES FOR TABLE $tableName")
            val segmentClean = sql(s"SHOW SEGMENTS FOR TABLE $tableName")
            val StatusClean = segmentClean.collect().map { each => (each.toSeq) (1) }

            assert(!StatusClean.contains("Compacted"))
            assert(StatusClean.length == 1)
            checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(4000)))
        } catch {
            case e: Exception =>
                e.printStackTrace()
                assert(false)
        } finally {
            sql(s"DROP TABLE IF EXISTS $tableName")
        }
    }

    def testAlterTAble(tableName: String, tableNameOriginal: String, remoteFile: String) {

        try {
            // Alter table name
            val tableNameList = sql("show tables").collect().map { each => (each.toSeq) (1) }
            assert(!tableNameList.contains(s"$tableName"))
            assert(tableNameList.contains(s"$tableNameOriginal"))
            sql(s"ALTER TABLE $tableNameOriginal RENAME TO $tableName")
            val tableNameRenamed = sql("show tables").collect().map { each => (each.toSeq) (1) }
            assert(tableNameRenamed.contains(s"$tableName"))
            assert(!tableNameRenamed.contains(s"$tableNameOriginal"))

            // Add columns
            sql(s"ALTER TABLE $tableName ADD COLUMNS (a1 INT, b1 STRING)")
            checkAnswer(sql(s"SELECT count(*) FROM $tableName"), Seq(Row(1000)))
            assert(10 == sql(s"SELECT * FROM $tableName limit 10").collect().length)
            val columnName = sql(s"SELECT * FROM $tableName").schema.map { each => (each.name) }
            assert(columnName.length == 9)
            assert(columnName.contains("a1"))
            assert(columnName.contains("b1"))

            // Drop columns
            sql(s"ALTER TABLE $tableName DROP COLUMNS (b1)")
            val columnNameDropped = sql(s"SELECT * FROM $tableName").schema.map { each => (each.name) }
            assert(columnNameDropped.length == 8)
            assert(columnNameDropped.contains("a1"))
            assert(!columnNameDropped.contains("b1"))

            // Change data type
            sql(s"ALTER TABLE $tableName CHANGE a1 a2 LONG")
            val columnChanged = sql(s"SELECT * FROM $tableName").schema
            assert(columnChanged.length == 8)
            assert(!columnChanged.map { each => (each.name) }.contains("a1"))
            assert(columnChanged.map { each => (each.name) }.contains("a2"))
            columnChanged.foreach { each =>
                assert(!each.name.contains("a1"))
                if (each.name.contains("a2")) {
                    assert(each.dataType == LongType)
                }
            }
        } catch {
            case e: Exception =>
                e.printStackTrace()
                assert(false)
        } finally {
            sql(s"DROP TABLE IF EXISTS $tableName")
            sql(s"DROP TABLE IF EXISTS $tableNameOriginal")
        }
    }


}
