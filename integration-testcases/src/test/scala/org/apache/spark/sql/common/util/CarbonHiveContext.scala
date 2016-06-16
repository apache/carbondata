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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.CarbonContext
import org.apache.spark.{SparkConf, SparkContext}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.spark.load.CarbonLoaderUtil

class LocalSQLContext(val hdfsCarbonBasePath: String)
  extends CarbonContext(new SparkContext(new SparkConf()
    .setAppName("CarbonSpark")
    .setMaster("local[2]").set("spark.sql.shuffle.partitions", "20")), hdfsCarbonBasePath) {

}

object CarbonHiveContext extends LocalSQLContext(
{
  val hadoopConf = new Configuration();
  hadoopConf.addResource(new Path("../core-default.xml"));
  hadoopConf.addResource(new Path("core-site.xml"));
  val hdfsCarbonPath = new File("./target/test/").getCanonicalPath;
  hdfsCarbonPath
}) {
    sparkContext.setLogLevel("ERROR")
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    CarbonProperties.getInstance().addProperty("carbon.kettle.home", currentDirectory+"/../processing/carbonplugins")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH, System.getProperty("java.io.tmpdir"))

    val hadoopConf = new Configuration();
    hadoopConf.addResource(new Path("../core-default.xml"));
    hadoopConf.addResource(new Path("core-site.xml"));
    val hdfsCarbonPath = new File("./target/test/").getCanonicalPath;
    setConf("hive.metastore.warehouse.dir", hdfsCarbonPath +"/hivemetadata")
    val hiveMetaStoreDB = hdfsCarbonPath+"/metastore_db"
    setConf("javax.jdo.option.ConnectionURL","jdbc:derby:;databaseName="+hiveMetaStoreDB+";create=true")

    CarbonLoaderUtil.deleteStorePath(hdfsCarbonPath)
    //	    //		sql("drop cube timestamptypecube");
}


