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

package org.apache.carbondata.view

case class MaterializedViewTimeGranularity(name: String, seconds: Long)

object MaterializedViewTimeGranularity {

  private val GRANULARITY_MAP = Map(
      ("YEAR", MaterializedViewTimeGranularity("YEAR", 365 * 24 * 60 * 60)),
      ("MONTH", MaterializedViewTimeGranularity("MONTH", 30 * 24 * 60 * 60)),
      ("WEEK", MaterializedViewTimeGranularity("WEEK", 7 * 24 * 60 * 60)),
      ("DAY", MaterializedViewTimeGranularity("DAY", 24 * 60 * 60)),
      ("HOUR", MaterializedViewTimeGranularity("HOUR", 60 * 60)),
      ("THIRTY_MINUTE", MaterializedViewTimeGranularity("THIRTY_MINUTE", 30 * 60)),
      ("FIFTEEN_MINUTE", MaterializedViewTimeGranularity("FIFTEEN_MINUTE", 15 * 60)),
      ("TEN_MINUTE", MaterializedViewTimeGranularity("TEN_MINUTE", 10 * 60)),
      ("FIVE_MINUTE", MaterializedViewTimeGranularity("FIVE_MINUTE", 5 * 60)),
      ("MINUTE", MaterializedViewTimeGranularity("MINUTE", 60)),
      ("SECOND", MaterializedViewTimeGranularity("SECOND", 1))
    )

  def get(name: String): MaterializedViewTimeGranularity = {
    GRANULARITY_MAP(name)
  }

  def getAll(): Iterable[MaterializedViewTimeGranularity] = {
    GRANULARITY_MAP.values
  }

}
