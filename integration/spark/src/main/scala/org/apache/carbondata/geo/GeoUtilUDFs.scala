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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.SparkSession

object GeoUtilUDFs {

  def registerUDFs(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("GeoIdToGridXy", new GeoIdToGridXyUDF)
    sparkSession.udf.register("GeoIdToLatLng", new GeoIdToLatLngUDF)
    sparkSession.udf.register("LatLngToGeoId", new LatLngToGeoIdUDF)
    sparkSession.udf.register("ToUpperLayerGeoId", new ToUpperLayerGeoIdUDF)
    sparkSession.udf.register("ToRangeList", new ToRangeListUDF)
  }
}

class GeoIdToGridXyUDF extends (java.lang.Long => Array[Int]) with Serializable {
  override def apply(geoId: java.lang.Long): Array[Int] = {
    GeoHashUtils.validateUDFInputValue(geoId, "geoId", "Long")
    GeoHashUtils.geoID2ColRow(geoId)
  }
}

class GeoIdToLatLngUDF
  extends ((java.lang.Long, java.lang.Double, java.lang.Integer) => Array[Double]) with
    Serializable {
  override def apply(geoId: java.lang.Long, oriLatitude: java.lang.Double,
      gridSize: java.lang.Integer): Array[Double] = {
    GeoHashUtils.validateUDFInputValue(geoId, "geoId", "Long")
    GeoHashUtils.validateUDFInputValue(oriLatitude, "oriLatitude", "Double")
    GeoHashUtils.validateUDFInputValue(gridSize, "gridSize", "Integer")
    GeoHashUtils.geoID2LatLng(geoId, oriLatitude, gridSize)
  }
}

class LatLngToGeoIdUDF extends ((java.lang.Long, java.lang.Long,
  java.lang.Double, java.lang.Integer) => Long) with Serializable {
  override def apply(latitude: java.lang.Long, longitude: java.lang.Long,
      oriLatitude: java.lang.Double, gridSize: java.lang.Integer): Long = {
    GeoHashUtils.validateUDFInputValue(latitude, "latitude", "Long")
    GeoHashUtils.validateUDFInputValue(longitude, "longitude", "Long")
    GeoHashUtils.validateUDFInputValue(oriLatitude, "oriLatitude", "Double")
    GeoHashUtils.validateUDFInputValue(gridSize, "gridSize", "Integer")
    GeoHashUtils.lonLat2GeoID(longitude, latitude, oriLatitude, gridSize)
  }
}

class ToUpperLayerGeoIdUDF extends (java.lang.Long => Long) with Serializable {
  override def apply(geoId: java.lang.Long): Long = {
    GeoHashUtils.validateUDFInputValue(geoId, "geoId", "Long")
    GeoHashUtils.convertToUpperLayerGeoId(geoId)
  }
}

class ToRangeListUDF extends ((java.lang.String, java.lang.Double, java.lang.Integer) =>
  mutable.Buffer[Array[Long]]) with Serializable {
  override def apply(polygon: java.lang.String, oriLatitude: java.lang.Double,
      gridSize: java.lang.Integer): mutable.Buffer[Array[Long]] = {
    GeoHashUtils.validateUDFInputValue(polygon, "polygon", "String")
    GeoHashUtils.validateUDFInputValue(oriLatitude, "oriLatitude", "Double")
    GeoHashUtils.validateUDFInputValue(gridSize, "gridSize", "Integer")
    GeoHashUtils.getRangeList(polygon, oriLatitude, gridSize).asScala.map(_.map(Long2long))
  }
}
