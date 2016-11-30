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

import org.apache.carbondata.examples.util.ExampleUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}

case class People(name: String, occupation: String, id: Int)

object CaseClassDataFrameAPIExample {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CaseClassDataFrameAPIExample")
    import cc.implicits._

    val people = List(People("sangeeta", "engineer", 1), People("pallavi", "consultant", 2))
    val peopleRDD: RDD[People] = cc.sc.parallelize(people)
    val peopleDF: DataFrame = peopleRDD.toDF("name", "occupation", "id")

    // writing data to carbon table
    peopleDF.write
      .format("carbondata")
      .option("tableName", "carbon2")
      .option("compress", "true")
      .mode(SaveMode.Overwrite)
      .save()

    cc.sql("SELECT * FROM carbon2").show()

    cc.sql("DROP TABLE IF EXISTS carbon2")
  }
}
