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

import java.io.File

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.CarbonContext

import org.carbondata.core.util.CarbonProperties

/**
 * Carbon supports the complex types ARRAY and STRUCT.
 * The complex type columns can be used with all SQL clauses.
 */
object ComplexTypeExample {

  def main(args: Array[String]) {

    val (cc: CarbonContext, currentDirectory: String) = initCarbonContext
    val dataPath = currentDirectory + "/src/main/resources/complexdata.csv"
    val tableName = "complexTypeTable"

    cc.sql(s"drop table if exists $tableName")
    cc.sql(s"""create table $tableName (
                 deviceInformationId int,
                 channelsId string,
                 ROMSize string,
                 purchasedate string,
                 mobile struct<imei:string,
                              imsi:string>,
                 MAC array<string>,
                 locationinfo array<struct<ActiveAreaId:int,
                                           ActiveCountry:string,
                                           ActiveProvince:string,
                                           Activecity:string,
                                           ActiveDistrict:string,
                                           ActiveStreet:string>>,
                  proddate struct<productionDate: string,
                                 activeDeactivedate: array<string>>,
                  gamePointId double,
                  contractNumber double)
                row format delimited fields terminated by ','
                collection items terminated by '$$' map keys terminated by ':' """)
    cc.sql(s"load data local inpath '$dataPath' into table $tableName")

    // filter on complex ARRAY type with index filter
    cc.sql(s"select mobile, proddate.activeDeactivedate, MAC[0] from $tableName " +
      "where MAC[0] like 'MAC1%'").show

    // filter on complex STRUCT type
    cc.sql(s"select mobile, proddate.activeDeactivedate  from $tableName " +
      "where mobile.imei = '1AA1' or mobile.imsi = ''").show

    // filter on complex STRUCT<ARRAY>
    cc.sql(s"select mobile, proddate.activeDeactivedate[0] from $tableName " +
      "where proddate.activeDeactivedate[0] = '29-11-2015'").show

    // filter on complex ARRAY<STRUCT>
    cc.sql(s"select mobile, locationinfo[0] from $tableName " +
      "where locationinfo[0].ActiveCountry = 'Chinese'").show
    // complex type aggregation and group by complex type
    cc.sql(s"select mobile, count(proddate) from $tableName group by mobile").show

    cc.sql(s"drop table if exists $tableName")
  }

  private[examples] def initCarbonContext: (CarbonContext, String) = {
    // get current work directory:/examples
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath

    // specify parameters
    val storeLocation = currentDirectory + "/target/store"
    val kettleHome = new File(currentDirectory + "/../processing/carbonplugins").getCanonicalPath
    val hiveMetaPath = currentDirectory + "/target/hivemetadata"

    val sc = new SparkContext(new SparkConf()
      .setAppName("CarbonExample")
      .setMaster("local[2]"))

    val cc = new CarbonContext(sc, storeLocation)

    // As Carbon using kettle, so need to set kettle configuration
    cc.setConf("carbon.kettle.home", kettleHome)
    cc.setConf("hive.metastore.warehouse.dir", hiveMetaPath)

    // whether use table split partition
    // true -> use table split partition, support multiple partition loading
    // false -> use node split partition, support data load by host partition
    CarbonProperties.getInstance().addProperty("carbon.table.split.partition.enable", "false")

    (cc, currentDirectory)
  }
}
