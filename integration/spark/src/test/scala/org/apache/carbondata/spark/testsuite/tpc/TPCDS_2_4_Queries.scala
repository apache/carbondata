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

package org.apache.carbondata.spark.testsuite.tpc

import org.apache.commons.io.IOUtils

/**
 * This implements the official TPCDS v2.4 queries with only cosmetic modifications.
 */
trait Tpcds_2_4_Queries {

  val tpcdsQueryNames = Seq(
    "q1", "q3", "q7", "q9", "q12", "q13", "q15", "q16", "q19", "q20", "q21",
    "q23a", "q24a", "q26", "q28", "q30", "q31", "q32", "q34", "q35", "q36",
    "q37", "q38", "q41", "q42", "q43", "q45", "q46", "q48", "q49", "q50",
    "q52", "q55", "q59", "q61", "q62", "q63", "q68", "q71", "q73", "q79", "q81",
    "q82", "q87", "q88", "q89", "q90", "q91", "q92", "q94", "q95", "q96", "q97",
    "q98"
  )

  val tpcds2_4Queries = tpcdsQueryNames.map { query =>
    IOUtils.toString(getClass()
      .getClassLoader().getResourceAsStream(s"tpcds/queries/$query.sql"))
  }
}
