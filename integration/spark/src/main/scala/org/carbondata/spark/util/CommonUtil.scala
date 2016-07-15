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
package org.carbondata.spark.util

import java.util
import java.util.UUID

import org.apache.spark.sql.execution.command.{ColumnProperty, Field}

import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.spark.exception.MalformedCarbonCommandException

object CommonUtil {
  def validateColumnGroup(colGroup: String, noDictionaryDims: Seq[String],
      msrs: Seq[Field], retrievedColGrps: Seq[String], dims: Seq[Field]) {
    val colGrpCols = colGroup.split(',').map(_.trim)
    colGrpCols.foreach { x =>
      // if column is no dictionary
      if (noDictionaryDims.contains(x)) {
        throw new MalformedCarbonCommandException(
          "Column group is not supported for no dictionary columns:" + x)
      } else if (msrs.filter { msr => msr.column.equals(x) }.size > 0) {
        // if column is measure
        throw new MalformedCarbonCommandException("Column group is not supported for measures:" + x)
      } else if (foundIndExistingColGrp(x)) {
        throw new MalformedCarbonCommandException("Column is available in other column group:" + x)
      } else if (isComplex(x, dims)) {
        throw new MalformedCarbonCommandException(
          "Column group doesn't support Complex column:" + x)
      } else if (isTimeStampColumn(x, dims)) {
        throw new MalformedCarbonCommandException(
          "Column group doesn't support Timestamp datatype:" + x)
      }
      // if invalid column is present
      else if (dims.filter { dim => dim.column.equalsIgnoreCase(x) }.size == 0) {
        throw new MalformedCarbonCommandException(
          "column in column group is not a valid column :" + x
        )
      }
    }
    // check if given column is present in other groups
    def foundIndExistingColGrp(colName: String): Boolean = {
      retrievedColGrps.foreach { colGrp =>
        if (colGrp.split(",").contains(colName)) {
          return true
        }
      }
      false
    }

  }


  def isTimeStampColumn(colName: String, dims: Seq[Field]): Boolean = {
    dims.foreach { dim =>
      if (dim.column.equalsIgnoreCase(colName)) {
        if (None != dim.dataType && null != dim.dataType.get &&
            "timestamp".equalsIgnoreCase(dim.dataType.get)) {
          return true
        }
      }
    }
    false
  }

  def isComplex(colName: String, dims: Seq[Field]): Boolean = {
    dims.foreach { x =>
      if (None != x.children && null != x.children.get && x.children.get.size > 0) {
        val children = x.children.get
        if (x.column.equals(colName)) {
          return true
        } else {
          children.foreach { child =>
            val fieldName = x.column + "." + child.column
            if (fieldName.equalsIgnoreCase(colName)) {
              return true
            }
          }
        }
      }
    }
    false
  }

  def getColumnProperties(column: String,
      tableProperties: Map[String, String]): Option[util.List[ColumnProperty]] = {
    val fieldProps = new util.ArrayList[ColumnProperty]()
    val columnPropertiesStartKey = CarbonCommonConstants.COLUMN_PROPERTIES + "." + column + "."
    tableProperties.foreach {
      case (key, value) =>
        if (key.startsWith(columnPropertiesStartKey)) {
          fieldProps.add(ColumnProperty(key.substring(columnPropertiesStartKey.length(),
            key.length()), value))
        }
    }
    if (fieldProps.isEmpty()) {
      None
    } else {
      Some(fieldProps)
    }
  }

  def validateTblProperties(tableProperties: Map[String, String], fields: Seq[Field]): Boolean = {
    val itr = tableProperties.keys
    var isValid: Boolean = true
    tableProperties.foreach {
      case (key, value) =>
        if (!validateFields(key, fields)) {
          isValid = false
          throw new MalformedCarbonCommandException(s"Invalid table properties ${ key }")
        }
    }
    isValid
  }

  def validateFields(key: String, fields: Seq[Field]): Boolean = {
    var isValid: Boolean = false
    fields.foreach { field =>
      if (field.children.isDefined && field.children.get != null) {
        field.children.foreach(fields => {
          fields.foreach(complexfield => {
            val column = if ("val" == complexfield.column) {
              field.column
            } else {
              field.column + "." + complexfield.column
            }
            if (validateColumnProperty(key, column)) {
              isValid = true
            }
          }
          )
        }
        )
      } else {
        if (validateColumnProperty(key, field.column)) {
          isValid = true
        }
      }

    }
    isValid
  }

  def validateColumnProperty(key: String, column: String): Boolean = {
    if (!key.startsWith(CarbonCommonConstants.COLUMN_PROPERTIES)) {
      return true
    }
    val columnPropertyKey = CarbonCommonConstants.COLUMN_PROPERTIES + "." + column + "."
    if (key.startsWith(columnPropertyKey)) {
      true
    } else {
      false
    }
  }
}
