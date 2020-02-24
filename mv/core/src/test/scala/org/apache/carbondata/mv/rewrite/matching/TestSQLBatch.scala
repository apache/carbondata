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

package org.apache.carbondata.mv.rewrite.matching

object TestSQLBatch {

  // seq of (summaryDataset(MV), testUserSQL(Q), correctRewrittenSQL)
  val sampleTestCases = Seq(
    ("case_1",
     s"""
        |SELECT i_item_id, i_item_sk
        |FROM Item
        |WHERE i_item_sk = 2
     """.stripMargin.trim,
     s"""
        |SELECT i_item_id, i_item_sk
        |FROM Item
        |WHERE i_item_sk = 1 and i_item_id > 0
     """.stripMargin.trim,
     s"""
        |SELECT item.`i_item_id`, item.`i_item_sk` 
        |FROM
        |  item
        |WHERE
        |  (item.`i_item_sk` = 1) AND (item.`i_item_id` > 0)
     """.stripMargin.trim),
    ("case_2",
     s"""
        |SELECT i_item_id
        |FROM Item
        |WHERE i_item_sk = 1
     """.stripMargin.trim,
     s"""
        |SELECT i_item_id, i_item_sk
        |FROM Item
        |WHERE i_item_sk = 1 or i_item_id > 0
     """.stripMargin.trim,
     s"""
        |SELECT item.`i_item_id`, item.`i_item_sk` 
        |FROM
        |  item
        |WHERE
        |  ((item.`i_item_sk` = 1) OR (item.`i_item_id` > 0))
     """.stripMargin.trim),
    ("case_3",
     s"""
        |SELECT faid, flid, date
        |FROM Fact
     """.stripMargin.trim,
     s"""
        |SELECT faid, flid, year(date) as year
        |FROM Fact
     """.stripMargin.trim,
     s"""
        |SELECT gen_subsumer_0.`faid`, gen_subsumer_0.`flid`, year(CAST(gen_subsumer_0.`date` AS DATE)) AS `year` 
        |FROM
        |  (SELECT fact.`faid`, fact.`flid`, fact.`date` 
        |  FROM
        |    fact) gen_subsumer_0
     """.stripMargin.trim),
    ("case_4",
     s"""
        |SELECT faid, flid, date
        |FROM Fact
     """.stripMargin.trim,
     s"""
        |SELECT faid, flid
        |FROM Fact
        |WHERE year(date) = 2000
     """.stripMargin.trim,
     s"""
        |SELECT gen_subsumer_0.`faid`, gen_subsumer_0.`flid` 
        |FROM
        |  (SELECT fact.`faid`, fact.`flid`, fact.`date` 
        |  FROM
        |    fact) gen_subsumer_0 
        |WHERE
        |  (year(CAST(gen_subsumer_0.`date` AS DATE)) = 2000)
     """.stripMargin.trim),
    ("case_5",
     s"""
        |SELECT faid, flid, date
        |FROM Fact
        |WHERE year(date) = 2000
     """.stripMargin.trim,
     s"""
        |SELECT faid, flid
        |FROM Fact
        |WHERE year(date) = 2000
     """.stripMargin.trim,
     s"""
        |SELECT gen_subsumer_0.`faid`, gen_subsumer_0.`flid` 
        |FROM
        |  (SELECT fact.`faid`, fact.`flid`, fact.`date` 
        |  FROM
        |    fact
        |  WHERE
        |    (year(CAST(fact.`date` AS DATE)) = 2000)) gen_subsumer_0 
        |WHERE
        |  (year(CAST(gen_subsumer_0.`date` AS DATE)) = 2000)
     """.stripMargin.trim),
    ("case_6",
     s"""
        |SELECT faid, flid, date
        |FROM Fact
        |WHERE year(date) in (2000,2001)
     """.stripMargin.trim,
     s"""
        |SELECT faid, flid
        |FROM Fact
        |WHERE year(date) = 2000
     """.stripMargin.trim,
     s"""
        |SELECT fact.`faid`, fact.`flid` 
        |FROM
        |  fact
        |WHERE
        |  (year(CAST(fact.`date` AS DATE)) = 2000)
     """.stripMargin.trim),
    ("case_7",
     s"""
        |SELECT faid, flid, year(date) as year, count(*) as cnt
        |FROM Fact
        |GROUP BY faid, flid, year(date)
     """.stripMargin.trim,
     s"""
        |SELECT faid, year(date) as year, count(*) as cnt
        |FROM Fact
        |GROUP BY Fact.faid,year(Fact.date)
        |HAVING count(*) > 2
     """.stripMargin.trim,
     s"""
        |SELECT gen_subsumer_0.`faid`, gen_subsumer_0.`year` AS `year`, sum(gen_subsumer_0.`cnt`) AS `cnt` 
        |FROM
        |  (SELECT fact.`faid`, fact.`flid`, year(CAST(fact.`date` AS DATE)) AS `year`, count(1) AS `cnt` 
        |  FROM
        |    fact
        |  GROUP BY fact.`faid`, fact.`flid`, year(CAST(fact.`date` AS DATE))) gen_subsumer_0 
        |GROUP BY gen_subsumer_0.`faid`, gen_subsumer_0.`year`
        |HAVING (sum(gen_subsumer_0.`cnt`) > 2L)
     """.stripMargin.trim),
    ("case_8",
     s"""
        |SELECT date
        |FROM Fact
     """.stripMargin.trim,
     s"""
        |SELECT year(date)
        |FROM Fact
     """.stripMargin.trim,
     s"""
        |SELECT year(CAST(gen_subsumer_0.`date` AS DATE)) AS `year(CAST(date AS DATE))` 
        |FROM
        |  (SELECT fact.`date` 
        |  FROM
        |    fact) gen_subsumer_0
     """.stripMargin.trim),
    ("case_9",
     s"""
        |SELECT faid, flid
        |FROM Fact
        |WHERE faid > 0
     """.stripMargin.trim,
     s"""
        |SELECT faid
        |FROM Fact
        |WHERE faid > 0 AND flid > 0
     """.stripMargin.trim,
     s"""
        |SELECT gen_subsumer_0.`faid` 
        |FROM
        |  (SELECT fact.`faid`, fact.`flid` 
        |  FROM
        |    fact
        |  WHERE
        |    (fact.`faid` > 0)) gen_subsumer_0 
        |WHERE
        |  (gen_subsumer_0.`faid` > 0) AND (gen_subsumer_0.`flid` > 0)
     """.stripMargin.trim),
    ("case_10",
     s"""
        |SELECT faid, flid
        |FROM Fact
        |WHERE faid > 0
     """.stripMargin.trim,
     s"""
        |SELECT faid
        |FROM Fact
        |WHERE faid > 0 OR flid > 0
     """.stripMargin.trim,
     s"""
        |SELECT fact.`faid` 
        |FROM
        |  fact
        |WHERE
        |  ((fact.`faid` > 0) OR (fact.`flid` > 0))
     """.stripMargin.trim),
    ("case_11",
     s"""
        |SELECT faid, count(flid)
        |FROM Fact
        |GROUP BY faid
     """.stripMargin.trim,
     s"""
        |SELECT faid, count(flid)
        |FROM Fact
        |WHERE faid = 3
        |GROUP BY faid
     """.stripMargin.trim,
     s"""
        |SELECT gen_subsumer_0.`faid`, gen_subsumer_0.`count(flid)` AS `count(flid)` 
        |FROM
        |  (SELECT fact.`faid`, count(fact.`flid`) AS `count(flid)` 
        |  FROM
        |    fact
        |  GROUP BY fact.`faid`) gen_subsumer_0 
        |WHERE
        |  (gen_subsumer_0.`faid` = 3)
     """.stripMargin.trim))
}