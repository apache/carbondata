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

package org.apache.carbondata.datamap

import org.apache.spark.sql.sources.Filter

import org.apache.carbondata.common.annotations.InterfaceAudience

@InterfaceAudience.Internal
class TextMatchUDF extends ((String) => Boolean) with Serializable {
  override def apply(v1: String): Boolean = {
    v1.length > 0
  }
}

@InterfaceAudience.Internal
class TextMatchMaxDocUDF extends ((String, Int) => Boolean) with Serializable {
  override def apply(v1: String, v2: Int): Boolean = {
    v1.length > 0
  }
}

@InterfaceAudience.Internal
case class TextMatch(queryString: String) extends Filter {
  override def references: Array[String] = null
}

@InterfaceAudience.Internal
case class TextMatchLimit(queryString: String, maxDoc: String) extends Filter {
  override def references: Array[String] = null
}
