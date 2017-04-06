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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.CarbonRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}

import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.spark.load.CarbonLoaderUtil

 /**
  * Optimize query with Limit condition and group by but no order by and where clause
  * For example:
  *    Select A, B, C, Sum(D)
  *    from t3
  *    group by A, B, C
  *    limit N
  * Step1: get the column reverse list (C, B, A)
  * Step2: traverse through the columns in step1 and get the dictionary distinct value list
  * Step3: get the top N value from the list(N is limit number), if the total value count is less
  *        than N, then get from next column
  * Step4: Exit when value list number equals to N (if every column's distinct count is less than N
  *        then no filters will be added.)
  * For A, B, C  three columns, if A has i distinct values, B has j, C has k. then the max
  * combination of them is i*j*k, the min combination is max(i,j,k)
  */
object LimitQueryOptimizer {
    def getFilters(limit_num: Int, groupingExpressions: Seq[Expression],
                      relations: Seq[CarbonDecoderRelation], child: LogicalPlan): Option[Filter] = {
        val cols = groupingExpressions.reverse.map(_.asInstanceOf[AttributeReference])
        val count_arr = ArrayBuffer[Int]()
        val expr_arr = ArrayBuffer[Expression]()
        for( i <- 0 until cols.size ) {
            val col = cols(i)
            val value_arr = ArrayBuffer[Literal]()
            count_arr += 0
            val relation = getTableRelation(col)
            val dictExist = relation.metaData.dictionaryMap.get(col.name).getOrElse(false)
            val count_max = count_arr.max
            /**
             * Once a column get the N values then skip the other columns
             */
            if (count_max < limit_num && dictExist) {
                val dict = LimitQueryOptimizer.getDictionaryValue(col, relation).get
                // Get distinct value list of current grouping column
                var index: Int = 2
                var distinctValue = dict.getDictionaryValueForKey(index)
                value_arr += (Literal(distinctValue))
                count_arr(i) += 1
                while (count_arr(i) < limit_num && distinctValue != null) {
                    index += 1
                    distinctValue = dict.getDictionaryValueForKey(index)
                    if (distinctValue != null) {
                        value_arr += (Literal(distinctValue))
                        count_arr(i) += 1
                    }
                }
                if (count_arr(i)  == limit_num) {
                    if (value_arr.size > 1) {
                        val inExpression = In(col, value_arr)
                        expr_arr += inExpression
                    } else if (value_arr.size == 1) {
                        val equalExpression = EqualTo(col, value_arr.head)
                        expr_arr += equalExpression
                    }
                }
            }
        }
        def getTableRelation(col: AttributeReference): CarbonRelation = {
            val tableName = col.qualifiers.head
            val relation = relations.filter(_.carbonRelation.getTable() == tableName).head
            val carbonRelation = relation.carbonRelation.carbonRelation
            carbonRelation
        }
        /**
         * Construct Filter with generated dictionary value list
         */
        var new_expr : Expression = null
        if (!expr_arr.isEmpty) {
            for (i <- 0 until expr_arr.size) {
                if (i == 0) {
                    new_expr = expr_arr(i)
                } else {
                    new_expr = new And(new_expr, expr_arr(i))
                }
            }
            Some(new Filter(new_expr, child))
        } else {
            None
        }

    }
     /**
      * get dictionary from column name and table info
      */
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
