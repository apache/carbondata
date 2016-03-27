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

package org.apache.spark.sql

import org.apache.spark.Logging
import org.apache.spark.sql.hive.{HiveContext, CarbonMetastoreCatalog}
import org.carbondata.core.metadata.CarbonMetadata
import org.carbondata.core.util.CarbonUtil
import org.carbondata.integration.spark.load.CarbonLoaderUtil

/**
  * Carbon Environment for unified context
  */
class CarbonEnv extends Logging {
  var carbonContext: HiveContext = _
  var carbonCatalog: CarbonMetastoreCatalog = _
  val FS_DEFAULT_FS = "fs.defaultFS"
  val HDFSURL_PREFIX = "hdfs://"
}

object CarbonEnv {
  val className = classOf[CarbonEnv].getCanonicalName
  var carbonEnv: CarbonEnv = _

  def getInstance(sqlContext: SQLContext): CarbonEnv = {
    if(carbonEnv == null)
    {
      carbonEnv = new CarbonEnv
      carbonEnv.carbonContext = sqlContext.asInstanceOf[CarbonContext]
      carbonEnv.carbonCatalog = sqlContext.asInstanceOf[CarbonContext].catalog
    }
    carbonEnv
  }

  var isloaded = false

  def loadCarbonCubes(sqlContext: SQLContext, carbonCatalog: CarbonMetastoreCatalog): Unit = {
    val cubes = carbonCatalog.getAllCubes()(sqlContext)
    if (null != cubes && isloaded == false) {
      isloaded = true
      cubes.foreach { cube =>
        val schemaName = cube._1
        val cubeName = cube._2
        val cubeInstance = CarbonMetadata.getInstance().getCube(
          schemaName + '_' + cubeName)
        val filePath = cubeInstance.getMetaDataFilepath();
        val details = CarbonUtil
          .readLoadMetadata(filePath)
        if (null != details) {
          var listOfLoadFolders = CarbonLoaderUtil.getListOfValidSlices(details)
          if (null != listOfLoadFolders && listOfLoadFolders.size() > 0 ) {
            var hc: HiveContext = sqlContext.asInstanceOf[HiveContext]
            hc.sql(" select count(*) from " + schemaName + "." + cubeName).collect()
          }
        }
      }
    }
  }
}


