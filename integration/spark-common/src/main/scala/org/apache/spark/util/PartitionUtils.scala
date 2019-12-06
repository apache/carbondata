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

package org.apache.spark.util

import scala.collection.mutable

import org.apache.spark.sql.execution.command.{DataMapField, Field, PartitionerField}

object PartitionUtils {

  /**
   * Used to extract PartitionerFields for aggregation datamaps.
   * This method will keep generating partitionerFields until the sequence of
   * partition column is broken.
   *
   * For example: if x,y,z are partition columns in main table then child tables will be
   * partitioned only if the child table has List("x,y,z", "x,y", "x") as the projection columns.
   *
   *
   */
  def getPartitionerFields(allPartitionColumn: Seq[String],
      fieldRelations: mutable.LinkedHashMap[Field, DataMapField]): Seq[PartitionerField] = {

    def generatePartitionerField(partitionColumn: List[String],
        partitionerFields: Seq[PartitionerField]): Seq[PartitionerField] = {
      partitionColumn match {
        case head :: tail =>
          // Collect the first relation which matched the condition
          val validRelation = fieldRelations.zipWithIndex.collectFirst {
            case ((field, dataMapField), index) if
            dataMapField.columnTableRelationList.getOrElse(Seq()).nonEmpty &&
            head.equals(dataMapField.columnTableRelationList.get.head.parentColumnName) &&
            dataMapField.aggregateFunction.isEmpty =>
              (PartitionerField(field.name.get,
                field.dataType,
                field.columnComment), allPartitionColumn.indexOf(head))
          }
          if (validRelation.isDefined) {
            val (partitionerField, index) = validRelation.get
            // if relation is found then check if the partitionerFields already found are equal
            // to the index of this element.
            // If x with index 1 is found then there should be exactly 1 element already found.
            // If z with index 2 comes directly after x then this check will be false are 1
            // element is skipped in between and index would be 2 and number of elements found
            // would be 1. In that case return empty sequence so that the aggregate table is not
            // partitioned on any column.
            if (index == partitionerFields.length) {
              generatePartitionerField(tail, partitionerFields :+ partitionerField)
            } else {
              Seq.empty
            }
          } else {
            // if not found then countinue search for the rest of the elements. Because the rest
            // of the elements can also decide if the table has to be partitioned or not.
            generatePartitionerField(tail, partitionerFields)
          }
        case Nil =>
          // if end of list then return fields.
          partitionerFields
      }
    }

    generatePartitionerField(allPartitionColumn.toList, Seq.empty)
  }


}
