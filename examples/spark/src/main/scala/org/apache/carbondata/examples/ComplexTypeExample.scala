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

/**
 * Carbon supports the complex types ARRAY and STRUCT.
 * The complex type columns can be used with all SQL clauses.
 */
object ComplexTypeExample {

  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("ComplexTypeExample")
    val dataPath = ExampleUtils.currentPath + "/src/main/resources/complexdata.csv"
    val tableName = "complexTypeTable"

    cc.sql(s"DROP TABLE IF EXISTS $tableName")
    cc.sql(s"""CREATE TABLE $tableName (
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
              STORED BY 'org.apache.carbondata.format' """)

    cc.sql(s"load data local inpath '$dataPath' into table $tableName " +
      "options ('COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')")

    // filter on complex ARRAY type with index filter
    cc.sql(s"SELECT mobile, proddate.activeDeactivedate, MAC[0] FROM $tableName " +
      "WHERE MAC[0] LIKE 'MAC1%'").show

    // filter on complex STRUCT type
    cc.sql(s"SELECT mobile, proddate.activeDeactivedate FROM $tableName " +
      "WHERE mobile.imei = '1AA1' or mobile.imsi = ''").show

    // filter on complex STRUCT<ARRAY>
    cc.sql(s"SELECT mobile, proddate.activeDeactivedate[0] FROM $tableName " +
      "WHERE proddate.activeDeactivedate[0] = '29-11-2015'").show

    // filter on complex ARRAY<STRUCT>
    cc.sql(s"SELECT mobile, locationinfo[0] FROM $tableName " +
      "WHERE locationinfo[0].ActiveCountry = 'Chinese'").show

    // complex type aggregation and group by complex type
    cc.sql(s"SELECT mobile, count(proddate) FROM $tableName GROUP BY mobile").show

    cc.sql(s"DROP TABLE IF EXISTS $tableName")
  }
}
