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

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.annotations.InterfaceAudience

object GeoFilterUDFs {

  def registerUDFs(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("in_polygon", new InPolygonUDF)
    sparkSession.udf.register("in_polygon_list", new InPolygonListUDF)
    sparkSession.udf.register("in_polyline_list", new InPolylineListUDF)
    sparkSession.udf.register("in_polygon_range_list", new InPolygonRangeListUDF)
  }
}

@InterfaceAudience.Internal
class InPolygonUDF extends (String => Boolean) with Serializable {
  override def apply(v1: String): Boolean = {
    true // Carbon applies the filter. So, Spark do not have to apply filter.
  }
}

@InterfaceAudience.Internal
class InPolygonListUDF extends ((String, String) => Boolean) with Serializable {
  override def apply(v1: String, v2: String): Boolean = {
    true // Carbon applies the filter. So, Spark do not have to apply filter.
  }
}

@InterfaceAudience.Internal
class InPolylineListUDF extends ((String, Float) => Boolean) with Serializable {
  override def apply(v1: String, v2: Float): Boolean = {
    true // Carbon applies the filter. So, Spark do not have to apply filter.
  }
}

@InterfaceAudience.Internal
class InPolygonRangeListUDF extends ((String, String) => Boolean) with Serializable {
  override def apply(v1: String, v2: String): Boolean = {
    true // Carbon applies the filter. So, Spark do not have to apply filter.
  }
}
