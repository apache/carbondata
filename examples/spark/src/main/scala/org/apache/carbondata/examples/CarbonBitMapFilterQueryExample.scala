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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonBitMapFilterQueryExample {
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")
    cc.sql("""
           SELECT date
           FROM t3
           limit 100
           """).show(1)

    var start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT country, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country <> 'china'
           """).show(10)
    }
    print("country <> 'china': " + (System.currentTimeMillis() - start))

    start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT country, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country = 'china'
           """).show(10)
    }
    print("country = 'china': " + (System.currentTimeMillis() - start))

    start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT country, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country <> 'france'
           """).show(10)
    }
    print("country <> 'france': " + (System.currentTimeMillis() - start))

    start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT country, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country = 'france'
           """).show(10)
    }
    print("country = 'france': " + (System.currentTimeMillis() - start))
    start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT country, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country IN ('france')
           """).show(10)
    }
    print("country IN ('france'): " + (System.currentTimeMillis() - start))


    start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT country, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country <> 'china' and country <> 'canada' and country <> 'indian'
           and country <> 'uk'
           """).show(10)
    }
    print("country <> 'china' and country <> 'canada' and country <> 'indian' and country <> 'uk': "
        + (System.currentTimeMillis() - start))
    start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT country, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country not in ('china','canada','indian','usa','uk')
           """).show(10)
    }
    print("country not in ('china','canada','indian','usa','uk'): "
        + (System.currentTimeMillis() - start))

    start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT country, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country IN ('china','usa','uk')
           """).show(10)
    }
    print("country IN ('china','usa','uk'): " + (System.currentTimeMillis() - start))

    start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT country, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country = 'china' or country = 'indian' or country = 'usa'
           """).show(10)
    }
    print("country = 'china' or country = 'indian' or country = 'usa': "
        + (System.currentTimeMillis() - start))

    start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT count(country)
           FROM t3
           WHERE country = 'china' or country = 'indian' or country = 'uk'
           """).show(10)
    }
    print("country = 'china' or country = 'indian' or country = 'uk' count: "
        + (System.currentTimeMillis() - start))

    start = System.currentTimeMillis()
    for (index <- 1 to 1) {
      cc.sql("""
           SELECT count(country)
           FROM t3
           WHERE country <> 'china' and country <> 'indian'
           """).show(10)
    }
    print("country <> 'china' and country <> 'indian' count: "
        + (System.currentTimeMillis() - start))
  }
}
