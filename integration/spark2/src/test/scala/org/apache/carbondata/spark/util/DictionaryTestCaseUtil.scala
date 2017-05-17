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

package org.apache.carbondata.spark.util

import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.test.TestQueryExecutor

import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.spark.load.CarbonLoaderUtil

/**
 * Utility for global dictionary test cases
 */
object DictionaryTestCaseUtil {

  /**
   *  check whether the dictionary of specified column generated
   * @param relation  carbon table relation
   * @param columnName  name of specified column
   * @param value  a value of column
   */
  def checkDictionary(relation: CarbonRelation, columnName: String, value: String) {
    val table = relation.tableMeta.carbonTable
    val dimension = table.getDimensionByName(table.getFactTableName, columnName)
    val tableIdentifier = new CarbonTableIdentifier(table.getDatabaseName, table.getFactTableName, "uniqueid")
    val columnIdentifier = new DictionaryColumnUniqueIdentifier(tableIdentifier,
      dimension.getColumnIdentifier, dimension.getDataType
    )
    val dict = CarbonLoaderUtil.getDictionary(columnIdentifier, TestQueryExecutor.storeLocation)
    assert(dict.getSurrogateKey(value) != CarbonCommonConstants.INVALID_SURROGATE_KEY)
  }
}
