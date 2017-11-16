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
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.processing.util.CarbonLoaderUtil

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
    val table = relation.carbonTable
    val dimension = table.getDimensionByName(table.getTableName, columnName)
    val tableIdentifier = new CarbonTableIdentifier(table.getDatabaseName, table.getTableName, "uniqueid")
    val  absoluteTableIdentifier = new AbsoluteTableIdentifier(table.getTablePath, tableIdentifier)
    val columnIdentifier = new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier,
      dimension.getColumnIdentifier, dimension.getDataType,
      CarbonStorePath.getCarbonTablePath(table.getAbsoluteTableIdentifier)
    )
    val dict = CarbonLoaderUtil.getDictionary(columnIdentifier)
    assert(dict.getSurrogateKey(value) != CarbonCommonConstants.INVALID_SURROGATE_KEY)
  }
}
