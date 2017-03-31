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

package org.apache.spark.sql.optimizer

import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.spark.sql.CarbonRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Filter

import scala.collection.mutable.ArrayBuffer

/**
  * Optimize query with Limit condition
  * @param limit_num
  * @param groupingExpressions
  * @param originFilters
  * @param relations
  */
class LimitQueryOptimizer(limit_num: Int, groupingExpressions: Seq[Expression],
                          originFilters: Filter, relations: Seq[CarbonDecoderRelation]) {
    def getNewFilters(): Filter = {
        val cols = groupingExpressions.reverse.map(_.asInstanceOf[AttributeReference])
        val count_arr = ArrayBuffer[Int]()
        val expr_arr = ArrayBuffer[Expression]()
        var total_count : Int = 0

        for( i <- 0 until cols.size ) {
            val col = cols(i)
            val value_arr = ArrayBuffer[Literal]()
            count_arr += 0
            val relation = getTableRelation(col)
            val dictExist = relation.metaData.dictionaryMap.get(col.name).get
            if (total_count < limit_num && dictExist) {
                val dict = LimitQueryOptimizer.getDictionaryValue(col, relation).get
                /** Get distinct value list of current grouping column **/
                var index: Int = 2
                var distinctValue = dict.getDictionaryValueForKey(index)
                value_arr += (Literal(distinctValue))
                count_arr(i) += 1
                total_count = count_arr.product
                while (total_count < limit_num && distinctValue != null) {
                    index += 1
                    distinctValue = dict.getDictionaryValueForKey(index)
                    value_arr += (Literal(distinctValue))
                    count_arr(i) += 1
                    total_count = count_arr.product
                }
                if (value_arr.size > 1) {
                    val inExpression = In(col, value_arr)
                    expr_arr += inExpression
                } else if (value_arr.size == 1) {
                    val equalExpression = EqualTo(col, value_arr.head)
                    expr_arr += equalExpression
                }
            }
        }
        val origin_expr = originFilters.condition
        var new_expr : And = null
        var index = 0
        for(expression <- expr_arr) {
            if (index == 0) {
                new_expr = new And(origin_expr, expression)
            } else {
                new_expr = new And(new_expr, expression)
            }
            index += 1
        }
       new Filter(new_expr, originFilters.child)
    }
    def getTableRelation(col: AttributeReference): CarbonRelation = {
        val tableName = col.qualifiers.head
        val relation = relations.filter(_.carbonRelation.getTable() == tableName).head
        val carbonRelation = relation.carbonRelation.carbonRelation
        carbonRelation
    }
}

object LimitQueryOptimizer {
    def apply(limit_num: Int, groupingExpressions: Seq[Expression], originFilters: Filter,
              relations: Seq[CarbonDecoderRelation]): LimitQueryOptimizer
      = new LimitQueryOptimizer(limit_num: Int, groupingExpressions: Seq[Expression],
        originFilters: Filter, relations: Seq[CarbonDecoderRelation])

    def getDictionaryValue(col: AttributeReference,
                           relation: CarbonRelation): Option[Dictionary] = {
        val tableName = col.qualifiers.head
        val carbonTable = relation.tableMeta.carbonTable
        val dimension = carbonTable.getDimensionByName(tableName.toLowerCase(), col.name)
        val carbonTableIdentifier = carbonTable.getCarbonTableIdentifier
        val columnIdentifier = new DictionaryColumnUniqueIdentifier(carbonTableIdentifier,
            dimension.getColumnIdentifier, dimension.getDataType)
        val path = relation.metaData.carbonTable.getStorePath
        val dict = CarbonLoaderUtil.getDictionary(columnIdentifier, path)
        Some(dict)
    }
}
