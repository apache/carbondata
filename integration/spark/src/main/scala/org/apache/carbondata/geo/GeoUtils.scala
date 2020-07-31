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

package org.apache.carbondata.geo

import scala.collection.mutable

import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CustomIndex

object GeoUtils {
  def getGeoHashHandler(tableProperties: mutable.Map[String, String])
                        : (String, CustomIndex[_]) = {
    val indexProperty = tableProperties.get(CarbonCommonConstants.SPATIAL_INDEX)
    if (indexProperty.isEmpty || indexProperty.get.trim.isEmpty) {
      CarbonException.analysisException(
        s"Table do not have ${CarbonCommonConstants.SPATIAL_INDEX} property " +
        s"with ${GeoConstants.GEOHASH} type")
    }
    val handler = indexProperty.get.split(",").map(_.trim).filter(indexName =>
      GeoConstants.GEOHASH.equalsIgnoreCase(
        tableProperties.getOrElse(s"${CarbonCommonConstants.SPATIAL_INDEX}.$indexName.type", "")))
      .map(indexName => (indexName,
        tableProperties.get(s"${CarbonCommonConstants.SPATIAL_INDEX}.$indexName.instance")))
    if (handler.isEmpty || handler.length != 1 || handler(0)._1.isEmpty
      || handler(0)._2.isEmpty) {
      CarbonException.analysisException(
        s"Table do not have ${CarbonCommonConstants.SPATIAL_INDEX} property " +
        s"with ${GeoConstants.GEOHASH} type index")
    }
    (handler(0)._1, CustomIndex.getCustomInstance(handler(0)._2.get))
  }
}
