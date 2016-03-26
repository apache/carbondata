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

package org.apache.spark.sql.common.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.OlapContext
import org.apache.spark.{SparkConf, SparkContext}
import org.carbondata.core.constants.MolapCommonConstants
import org.carbondata.core.util.MolapProperties
import org.carbondata.integration.spark.load.MolapLoaderUtil

class LocalSQLContext(hdfsCarbonBasePath: String)
  extends OlapContext(new SparkContext(new SparkConf()
    .setAppName("CarbonSpark")
    .setMaster("local[2]")
    .set("carbon.storelocation", hdfsCarbonBasePath)
    .set("molap.kettle.home", "../../Molap/Molap-Data-Processor/molapplugins/molapplugins")
    .set("molap.is.columnar.storage", "true")
    .set("spark.sql.bigdata.register.dialect", "org.apache.spark.sql.MolapSqlDDLParser")
    .set("spark.sql.bigdata.register.strategyRule", "org.apache.spark.sql.hive.CarbonStrategy")
    .set("spark.sql.bigdata.initFunction", "org.apache.spark.sql.CarbonEnv")
    .set("spark.sql.bigdata.acl.enable", "false")
    .set("molap.tempstore.location", System.getProperty("java.io.tmpdir"))
    .set("spark.sql.bigdata.register.strategy.useFunction", "true")
    .set("hive.security.authorization.enabled", "false")
    .set("spark.sql.bigdata.register.analyseRule", "org.apache.spark.sql.QueryStatsRule")
    .set("spark.sql.dialect", "hiveql")), hdfsCarbonBasePath) {

}

object CarbonHiveContext extends LocalSQLContext(
{
  val hadoopConf = new Configuration();
  hadoopConf.addResource(new Path("../core-default.xml"));
  hadoopConf.addResource(new Path("core-site.xml"));
  val hdfsCarbonPath = hadoopConf.get("fs.defaultFS", "./") + "/opt/carbon/test/";
  hdfsCarbonPath
}) {

  {
    MolapProperties.getInstance().addProperty("molap.kettle.home", "../../Molap/Molap-Data-Processor/molapplugins/molapplugins")
    MolapProperties.getInstance().addProperty(MolapCommonConstants.MOLAP_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    MolapProperties.getInstance().addProperty(MolapCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"))

    val hadoopConf = new Configuration();
    hadoopConf.addResource(new Path("../core-default.xml"));
    hadoopConf.addResource(new Path("core-site.xml"));
    val hdfsCarbonPath = hadoopConf.get("fs.defaultFS", "./") + "/opt/carbon/test/";

    MolapLoaderUtil.deleteStorePath(hdfsCarbonPath)
    //	    //		sql("drop cube timestamptypecube");
  }
}


