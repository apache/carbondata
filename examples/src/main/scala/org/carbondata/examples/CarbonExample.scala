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

package org.carbondata.examples

import org.apache.spark.sql.CarbonContext
import org.apache.spark.{SparkConf, SparkContext}

object CarbonExample {

  def main(args: Array[String]) {

    //please replace with your local machine path
    val hdfsCarbonBasePath = "/home/root1/carbon/metadata"
    val sc = new SparkContext(new SparkConf()
      .setAppName("CarbonSpark")
      .setMaster("local[2]"))

    val oc = new CarbonContext(sc, hdfsCarbonBasePath)

    //please replace with your local machine path
    oc.setConf("carbon.kettle.home", "/home/root1/carbon/carbondata/processing/carbonplugins/")
     oc.setConf("hive.metastore.warehouse.dir", "/home/root1/carbon/hivemetadata")

    //When you excute the second time, need to enable it
    //oc.sql("drop cube alldatatypescube")

    oc.sql("CREATE CUBE alldatatypescube DIMENSIONS (empno Integer, empname String, " +
      "designation String, doj Timestamp, workgroupcategory Integer, workgroupcategoryname String, " +
      "deptno Integer, deptname String, projectcode Integer, projectjoindate Timestamp, " +
      "projectenddate Timestamp) MEASURES (attendance Integer,utilization Integer,salary Integer) " +
      "OPTIONS (PARTITIONER [PARTITION_COUNT=1])")

    //please replace with your local machine path
    oc.sql("LOAD DATA fact from '/home/root1/carbon/carbondata/integration/spark/src/test/resources/data.csv' INTO CUBE alldatatypescube PARTITIONDATA(DELIMITER ',', QUOTECHAR '\"')");

    oc.sql("select empno,empname,utilization,count(salary),sum(empno) from alldatatypescube where empname in ('arvind','ayushi') group by empno,empname,utilization").show()

  }

}
