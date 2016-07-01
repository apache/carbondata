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
package org.carbondata.spark

import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.carbon.ColumnIdentifier
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema
import org.carbondata.core.carbon.path.CarbonTablePath
import org.carbondata.spark.exception.MalformedCarbonCommandException

 /**
  * Column validator
  */
trait ColumnValidator {
  def validateColumns(columns: Seq[ColumnSchema])
}
/**
 * Dictionary related helper service
 */
trait DictionaryDetailService {
  def getDictionaryDetail(dictFolderPath: String, primDimensions: Array[CarbonDimension],
      table: CarbonTableIdentifier, hdfsLocation: String): DictionaryDetail
}

/**
 * Dictionary related detail
 */
case class DictionaryDetail(columnIdentifiers: Array[ColumnIdentifier],
    dictFilePaths: Array[String], dictFileExists: Array[Boolean])

/**
 * Factory class
 */
object CarbonSparkFactory {
   /**
    * @return column validator
    */
  def getCarbonColumnValidator(): ColumnValidator = {
    new CarbonColumnValidator
  }

  /**
   * @return dictionary helper
   */
  def getDictionaryDetailService(): DictionaryDetailService = {
    new DictionaryDetailHelper
  }
}
