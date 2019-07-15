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
package org.apache.spark.sql.carbondata.execution.datasources

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.{ArrayType => CarbonArrayType, DataType => CarbonDataType, DataTypes => CarbonDataTypes, DecimalType => CarbonDecimalType, MapType => CarbonMapType, StructField => CarbonStructField, StructType => CarbonStructType}
import org.apache.carbondata.core.scan.expression.{ColumnExpression => CarbonColumnExpression, Expression => CarbonExpression, LiteralExpression => CarbonLiteralExpression}
import org.apache.carbondata.core.scan.expression.conditional._
import org.apache.carbondata.core.scan.expression.logical.{AndExpression, FalseExpression, OrExpression}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.sdk.file.{CarbonWriterBuilder, Field, Schema}

object CarbonSparkDataSourceUtil {

  /**
   * Convert from carbon datatype to sparks datatype
   */
  def convertCarbonToSparkDataType(dataType: CarbonDataType): types.DataType = {
    if (CarbonDataTypes.isDecimal(dataType)) {
      DecimalType(dataType.asInstanceOf[CarbonDecimalType].getPrecision,
        dataType.asInstanceOf[CarbonDecimalType].getScale)
    } else {
      if (CarbonDataTypes.isStructType(dataType)) {
        val struct = dataType.asInstanceOf[CarbonStructType]
        return StructType(struct.getFields.asScala.map(x =>
          StructField(x.getFieldName, convertCarbonToSparkDataType(x.getDataType)))
        )
      } else if (CarbonDataTypes.isArrayType(dataType)) {
        val array = dataType.asInstanceOf[CarbonArrayType]
        return ArrayType(convertCarbonToSparkDataType(array.getElementType))
      } else if (CarbonDataTypes.isMapType(dataType)) {
        val map = dataType.asInstanceOf[CarbonMapType]
        return MapType(
          convertCarbonToSparkDataType(map.getKeyType),
          convertCarbonToSparkDataType(map.getValueType))
      }
      dataType match {
        case CarbonDataTypes.STRING => StringType
        case CarbonDataTypes.SHORT => ShortType
        case CarbonDataTypes.INT => IntegerType
        case CarbonDataTypes.LONG => LongType
        case CarbonDataTypes.BYTE => ByteType
        case CarbonDataTypes.DOUBLE => DoubleType
        case CarbonDataTypes.FLOAT => FloatType
        case CarbonDataTypes.BOOLEAN => BooleanType
        case CarbonDataTypes.TIMESTAMP => TimestampType
        case CarbonDataTypes.BINARY => BinaryType
        case CarbonDataTypes.DATE => DateType
        case CarbonDataTypes.VARCHAR => StringType
      }
    }
  }

  /**
   * Convert from sparks datatype to carbon datatype
   */
  def convertSparkToCarbonDataType(dataType: DataType): CarbonDataType = {
    dataType match {
      case StringType => CarbonDataTypes.STRING
      case ShortType => CarbonDataTypes.SHORT
      case IntegerType => CarbonDataTypes.INT
      case LongType => CarbonDataTypes.LONG
      case DoubleType => CarbonDataTypes.DOUBLE
      case FloatType => CarbonDataTypes.FLOAT
      case ByteType => CarbonDataTypes.BYTE
      case DateType => CarbonDataTypes.DATE
      case BooleanType => CarbonDataTypes.BOOLEAN
      case TimestampType => CarbonDataTypes.TIMESTAMP
      case BinaryType => CarbonDataTypes.BINARY
      case ArrayType(elementType, _) =>
        CarbonDataTypes.createArrayType(convertSparkToCarbonDataType(elementType))
      case StructType(fields) =>
        val carbonFields = new java.util.ArrayList[CarbonStructField]
        fields.map { field =>
          carbonFields.add(
            new CarbonStructField(
              field.name,
              convertSparkToCarbonDataType(field.dataType)))
        }
        CarbonDataTypes.createStructType(carbonFields)
      case MapType(keyType, valueType, _) =>
        val keyDataType: CarbonDataType = convertSparkToCarbonDataType(keyType)
        val valueDataType: CarbonDataType = convertSparkToCarbonDataType(valueType)
        CarbonDataTypes.createMapType(keyDataType, valueDataType)
      case NullType => CarbonDataTypes.NULL
      case decimal: DecimalType =>
        CarbonDataTypes.createDecimalType(decimal.precision, decimal.scale)
      case _ => throw new UnsupportedOperationException("getting " + dataType + " from spark")
    }
  }

  /**
   * Converts data sources filters to carbon filter predicates.
   */
  def createCarbonFilter(schema: StructType,
      predicate: sources.Filter): Option[CarbonExpression] = {
    val dataTypeOf = schema.map(f => f.name -> f.dataType).toMap

    def createFilter(predicate: sources.Filter): Option[CarbonExpression] = {
      predicate match {

        case sources.EqualTo(name, value) =>
          Some(new EqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.Not(sources.EqualTo(name, value)) =>
          Some(new NotEqualsExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.EqualNullSafe(name, value) =>
          Some(new EqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.Not(sources.EqualNullSafe(name, value)) =>
          Some(new NotEqualsExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.GreaterThan(name, value) =>
          Some(new GreaterThanExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.LessThan(name, value) =>
          Some(new LessThanExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.GreaterThanOrEqual(name, value) =>
          Some(new GreaterThanEqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.LessThanOrEqual(name, value) =>
          Some(new LessThanEqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case sources.In(name, values) =>
          if (values.length == 1 && values(0) == null) {
            Some(new FalseExpression(getCarbonExpression(name)))
          } else {
            Some(new InExpression(getCarbonExpression(name),
              new ListExpression(
                convertToJavaList(values.filterNot(_ == null)
                  .map(filterValues => getCarbonLiteralExpression(name, filterValues)).toList))))
          }
        case sources.Not(sources.In(name, values)) =>
          if (values.contains(null)) {
            Some(new FalseExpression(getCarbonExpression(name)))
          } else {
            Some(new NotInExpression(getCarbonExpression(name),
              new ListExpression(
                convertToJavaList(values.map(f => getCarbonLiteralExpression(name, f)).toList))))
          }
        case sources.IsNull(name) =>
          Some(new EqualToExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, null), true))
        case sources.IsNotNull(name) =>
          Some(new NotEqualsExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, null), true))
        case sources.And(lhs, rhs) =>
          (createFilter(lhs) ++ createFilter(rhs)).reduceOption(new AndExpression(_, _))
        case sources.Or(lhs, rhs) =>
          for {
            lhsFilter <- createFilter(lhs)
            rhsFilter <- createFilter(rhs)
          } yield {
            new OrExpression(lhsFilter, rhsFilter)
          }
        case sources.StringStartsWith(name, value) if value.length > 0 =>
          Some(new StartsWithExpression(getCarbonExpression(name),
            getCarbonLiteralExpression(name, value)))
        case _ => None
      }
    }

    def getCarbonExpression(name: String) = {
      new CarbonColumnExpression(name,
        CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataTypeOf(name)))
    }

    def getCarbonLiteralExpression(name: String, value: Any): CarbonExpression = {
      val dataTypeOfAttribute =
        CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataTypeOf(name))
      val dataType =
        if (Option(value).isDefined &&
            dataTypeOfAttribute == CarbonDataTypes.STRING &&
            value.isInstanceOf[Double]) {
        CarbonDataTypes.DOUBLE
      } else {
        dataTypeOfAttribute
      }
      val dataValue = if (dataTypeOfAttribute.equals(CarbonDataTypes.BINARY)
              && Option(value).isDefined) {
        new String(value.asInstanceOf[Array[Byte]])
      } else if (dataTypeOfAttribute.equals(CarbonDataTypes.DATE) &&
                 value.isInstanceOf[java.sql.Date]) {
        // In case of Date object , convert back to int.
        DateTimeUtils.fromJavaDate(value.asInstanceOf[java.sql.Date])
      } else {
        value
      }
      new CarbonLiteralExpression(dataValue, dataType)
    }

    createFilter(predicate)
  }

  // Convert scala list to java list, Cannot use scalaList.asJava as while deserializing it is
  // not able find the classes inside scala list and gives ClassNotFoundException.
  def convertToJavaList(
      scalaList: Seq[CarbonExpression]): java.util.List[CarbonExpression] = {
    val javaList = new java.util.ArrayList[CarbonExpression]()
    scalaList.foreach(javaList.add)
    javaList
  }

  /**
   * Create load model for carbon
   */
  def prepareLoadModel(options: Map[String, String],
      dataSchema: StructType): CarbonLoadModel = {
    val schema = new Schema(dataSchema.fields.map { field =>
      val dataType = convertSparkToCarbonDataType(field.dataType)
      new Field(field.name, dataType)
    })
    val builder = new CarbonWriterBuilder
    builder.outputPath(options.getOrElse("path", ""))
    val blockSize = options.get(CarbonCommonConstants.TABLE_BLOCKSIZE).map(_.toInt)
    if (blockSize.isDefined) {
      builder.withBlockSize(blockSize.get)
    }
    val blockletSize = options.get("table_blockletsize").map(_.toInt)
    if (blockletSize.isDefined) {
      builder.withBlockletSize(blockletSize.get)
    }
    val pageSizeInMb = options.get("table_page_size_inmb").map(_.toInt)
    if (pageSizeInMb.isDefined) {
      builder.withPageSizeInMb(pageSizeInMb.get)
    }
    builder.enableLocalDictionary(options.getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
      CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT).toBoolean)
    builder.localDictionaryThreshold(
      options.getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
        CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT).toInt)
    val sortCols = options.get(CarbonCommonConstants.SORT_COLUMNS) match {
      case Some(cols) =>
        if (cols.trim.isEmpty) {
          Array[String]()
        } else {
          cols.split(",").map(_.trim)
        }
      case _ => Array[String]()
    }
    builder.sortBy(sortCols)
    val invertedIdxCols = options.get(CarbonCommonConstants.INVERTED_INDEX) match {
      case Some(cols) =>
        if (cols.trim.isEmpty) {
          Array[String]()
        } else {
          cols.split(",").map(_.trim)
        }
      case _ => null
    }
    builder.invertedIndexFor(invertedIdxCols)
    val longStringColumns: String = options
      .getOrElse(CarbonCommonConstants.LONG_STRING_COLUMNS, null)
    if (longStringColumns != null) {
      val loadOptions = Map(CarbonCommonConstants.LONG_STRING_COLUMNS -> longStringColumns).asJava
      builder.withTableProperties(loadOptions)
    }
    builder.uniqueIdentifier(System.currentTimeMillis())
    val model = builder.buildLoadModel(schema)
    val binary_decoder = options.get("binary_decoder").map(_.toString.toLowerCase())
    if (binary_decoder.isDefined) {
      model.setBinaryDecoder(String.valueOf(binary_decoder.get))
    }
    val tableInfo = model.getCarbonDataLoadSchema.getCarbonTable.getTableInfo
    val properties =
      tableInfo.getFactTable.getTableProperties
    // Add the meta cache level
    options.map{ case (key, value) =>
      if (key.equalsIgnoreCase(CarbonCommonConstants.COLUMN_META_CACHE)) {
        val columnsToBeCached = value.split(",").map(x => x.trim.toLowerCase).toSeq
        // make the columns in create table order and then add it to table properties
        val createOrder =
          tableInfo.getFactTable.getListOfColumns.asScala.map(_.getColumnName).filter(
            col => columnsToBeCached.contains(col))
        properties.put(CarbonCommonConstants.COLUMN_META_CACHE, createOrder.mkString(","))
      }
    }
    model
  }
}
