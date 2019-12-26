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

package org.apache.spark.sql

import org.apache.spark.sql.execution.command.management.CarbonInsertIntoWithDf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.{DataTypes => CarbonType}
import org.apache.carbondata.spark.CarbonOption

class CarbonDataFrameWriter(sqlContext: SQLContext, val dataFrame: DataFrame) {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def saveAsCarbonFile(parameters: Map[String, String] = Map()): Unit = {
    // create a new table using dataframe's schema and write its content into the table
    sqlContext.sparkSession.sql(
      makeCreateTableString(dataFrame.schema, new CarbonOption(parameters)))
    writeToCarbonFile(parameters)
  }

  def appendToCarbonFile(parameters: Map[String, String] = Map()): Unit = {
    writeToCarbonFile(parameters)
  }

  private def writeToCarbonFile(parameters: Map[String, String] = Map()): Unit = {
    val options = new CarbonOption(parameters)
    loadDataFrame(options)
  }
  /**
   * Loading DataFrame directly without saving DataFrame to CSV files.
   * @param options
   */
  private def loadDataFrame(options: CarbonOption): Unit = {
    val header = dataFrame.columns.mkString(",")
    CarbonInsertIntoWithDf(
      databaseNameOp = Some(CarbonEnv.getDatabaseName(options.dbName)(sqlContext.sparkSession)),
      tableName = options.tableName,
      options = Map("fileheader" -> header) ++ options.toMap,
      isOverwriteTable = options.overwriteEnabled,
      dataFrame = dataFrame
      ).process(sqlContext.sparkSession)
  }

  private def convertToCarbonType(sparkType: DataType): String = {
    sparkType match {
      case StringType => CarbonType.STRING.getName
      case IntegerType => CarbonType.INT.getName
      case ShortType => CarbonType.SHORT.getName
      case LongType => CarbonType.LONG.getName
      case FloatType => CarbonType.FLOAT.getName
      case DoubleType => CarbonType.DOUBLE.getName
      case TimestampType => CarbonType.TIMESTAMP.getName
      case DateType => CarbonType.DATE.getName
      case decimal: DecimalType => s"decimal(${decimal.precision}, ${decimal.scale})"
      case BooleanType => CarbonType.BOOLEAN.getName
      case BinaryType => CarbonType.BINARY.getName
      case other => CarbonException.analysisException(s"unsupported type: $other")
    }
  }

  private def makeCreateTableString(schema: StructType, options: CarbonOption): String = {
    val property = Map(
      "SORT_COLUMNS" -> options.sortColumns,
      "SORT_SCOPE" -> options.sortScope,
      "LONG_STRING_COLUMNS" -> options.longStringColumns,
      "TABLE_BLOCKSIZE" -> options.tableBlockSize,
      "TABLE_BLOCKLET_SIZE" -> options.tableBlockletSize,
      "TABLE_PAGE_SIZE_INMB" -> options.tablePageSizeInMb,
      "STREAMING" -> Option(options.isStreaming.toString)
    ).filter(_._2.isDefined)
      .map(property => s"'${property._1}' = '${property._2.get}'").mkString(",")

    val partition: Seq[String] = if (options.partitionColumns.isDefined) {
      if (options.partitionColumns.get.toSet.size != options.partitionColumns.get.length) {
        throw new MalformedCarbonCommandException(s"repeated partition column")
      }
      options.partitionColumns.get.map { column =>
        val field = schema.fields.find(_.name.equalsIgnoreCase(column))
        if (field.isEmpty) {
          throw new MalformedCarbonCommandException(s"invalid partition column: $column")
        }
        s"$column ${field.get.dataType.typeName}"
      }
    } else {
      Seq()
    }

    val schemaWithoutPartition = if (options.partitionColumns.isDefined) {
      val partitionCols = options.partitionColumns.get
      val fields = schema.filterNot {
        field => partitionCols.exists(_.equalsIgnoreCase(field.name))
      }
      StructType(fields)
    } else {
      schema
    }

    val carbonSchema = schemaWithoutPartition.map { field =>
      s"${ field.name } ${ convertToCarbonType(field.dataType) }"
    }

    val dbName = CarbonEnv.getDatabaseName(options.dbName)(sqlContext.sparkSession)
    val tablePath = options.tablePath.map(FileFactory.getUpdatedFilePath(_))

    s"""
       | CREATE TABLE IF NOT EXISTS $dbName.${options.tableName}
       | (${ carbonSchema.mkString(", ") })
       | ${ if (partition.nonEmpty) s"PARTITIONED BY (${partition.mkString(", ")})" else ""}
       | STORED AS carbondata
       | ${ if (tablePath.nonEmpty) s"LOCATION '${tablePath.get}'" else ""}
       | ${ if (property.nonEmpty) "TBLPROPERTIES (" + property + ")" else "" }
       |
     """.stripMargin
  }

}
