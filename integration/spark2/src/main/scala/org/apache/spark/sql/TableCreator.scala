package org.apache.spark.sql

import java.util.regex.{Matcher, Pattern}

import org.apache.carbondata.core.carbon.metadata.datatype.DataType
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.DataTypeUtil
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CommonUtil
import org.apache.spark.sql.execution.command.{ColumnProperty, Field, PartitionerField, TableModel}

import scala.collection.mutable.{LinkedHashSet, Map}

/**
  * Created by david on 16-11-25.
  */
object TableCreator {

  /**
    * detects whether complex dimension is part of dictionary_exclude
    */
  def isComplexDimDictionaryExclude(dimensionDataType: String): Boolean = {
    val dimensionType = Array("array", "struct")
    dimensionType.exists(x => x.equalsIgnoreCase(dimensionDataType))
  }

  /**
    * detects whether double or decimal column is part of dictionary_exclude
    */
  def isStringAndTimestampColDictionaryExclude(columnDataType: String): Boolean = {
    val dataTypes = Array("string", "timestamp")
    dataTypes.exists(x => x.equalsIgnoreCase(columnDataType))
  }

  /**
    * detect dimention data type
    *
    * @param dimensionDatatype
    */
  def isDetectAsDimentionDatatype(dimensionDatatype: String): Boolean = {
    val dimensionType =
      Array("string", "stringtype", "array", "arraytype", "struct", "structtype", "timestamp", "timestamptype")
    dimensionType.exists(x => x.equalsIgnoreCase(dimensionDatatype))
  }

  protected def extractDimColsAndNoDictionaryFields(fields: Seq[Field],
                                                    tableProperties: Map[String, String]):
  (Seq[Field], Seq[String]) = {
    var dimFields: LinkedHashSet[Field] = LinkedHashSet[Field]()
    var dictExcludeCols: Array[String] = Array[String]()
    var noDictionaryDims: Seq[String] = Seq[String]()
    var dictIncludeCols: Seq[String] = Seq[String]()

    // All excluded cols should be there in create table cols
    if (tableProperties.get(CarbonCommonConstants.DICTIONARY_EXCLUDE).isDefined) {
      dictExcludeCols =
          tableProperties.get(CarbonCommonConstants.DICTIONARY_EXCLUDE).get.split(',').map(_.trim)
      dictExcludeCols
          .map { dictExcludeCol =>
            if (!fields.exists(x => x.column.equalsIgnoreCase(dictExcludeCol))) {
              val errormsg = "DICTIONARY_EXCLUDE column: " + dictExcludeCol +
                  " does not exist in table. Please check create table statement."
              throw new MalformedCarbonCommandException(errormsg)
            } else {
              val dataType = fields.find(x =>
                x.column.equalsIgnoreCase(dictExcludeCol)).get.dataType.get
              if (isComplexDimDictionaryExclude(dataType)) {
                val errormsg = "DICTIONARY_EXCLUDE is unsupported for complex datatype column: " +
                    dictExcludeCol
                throw new MalformedCarbonCommandException(errormsg)
              } else if (!isStringAndTimestampColDictionaryExclude(dataType)) {
                val errorMsg = "DICTIONARY_EXCLUDE is unsupported for " + dataType.toLowerCase() +
                    " data type column: " + dictExcludeCol
                throw new MalformedCarbonCommandException(errorMsg)
              }
            }
          }
    }
    // All included cols should be there in create table cols
    if (tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE).isDefined) {
      dictIncludeCols =
          tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE).get.split(",").map(_.trim)
      dictIncludeCols.map { distIncludeCol =>
        if (!fields.exists(x => x.column.equalsIgnoreCase(distIncludeCol.trim))) {
          val errormsg = "DICTIONARY_INCLUDE column: " + distIncludeCol.trim +
              " does not exist in table. Please check create table statement."
          throw new MalformedCarbonCommandException(errormsg)
        }
      }
    }

    // include cols should contain exclude cols
    dictExcludeCols.foreach { dicExcludeCol =>
      if (dictIncludeCols.exists(x => x.equalsIgnoreCase(dicExcludeCol))) {
        val errormsg = "DICTIONARY_EXCLUDE can not contain the same column: " + dicExcludeCol +
            " with DICTIONARY_INCLUDE. Please check create table statement."
        throw new MalformedCarbonCommandException(errormsg)
      }
    }

    // by default consider all String cols as dims and if any dictionary exclude is present then
    // add it to noDictionaryDims list. consider all dictionary excludes/include cols as dims
    fields.foreach(field => {

      if (dictExcludeCols.toSeq.exists(x => x.equalsIgnoreCase(field.column))) {
        if (DataTypeUtil.getDataType(field.dataType.get.toUpperCase()) != DataType.TIMESTAMP) {
          noDictionaryDims :+= field.column
        }
        dimFields += field
      } else if (dictIncludeCols.exists(x => x.equalsIgnoreCase(field.column))) {
        dimFields += (field)
      } else if (isDetectAsDimentionDatatype(field.dataType.get)) {
        dimFields += (field)
      }
    }
    )

    (dimFields.toSeq, noDictionaryDims)
  }

  /**
    * Extract the Measure Cols fields. By default all non string cols will be measures.
    *
    * @param fields
    * @param tableProperties
    * @return
    */
  protected def extractMsrColsFromFields(fields: Seq[Field],
                                         tableProperties: Map[String, String]): Seq[Field] = {
    var msrFields: Seq[Field] = Seq[Field]()
    var dictIncludedCols: Array[String] = Array[String]()
    var dictExcludedCols: Array[String] = Array[String]()

    // get all included cols
    if (tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE).isDefined) {
      dictIncludedCols =
          tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE).get.split(',').map(_.trim)
    }

    // get all excluded cols
    if (tableProperties.get(CarbonCommonConstants.DICTIONARY_EXCLUDE).isDefined) {
      dictExcludedCols =
          tableProperties.get(CarbonCommonConstants.DICTIONARY_EXCLUDE).get.split(',').map(_.trim)
    }

    // by default consider all non string cols as msrs. consider all include/ exclude cols as dims
    fields.foreach(field => {
      if (!isDetectAsDimentionDatatype(field.dataType.get)) {
        if (!dictIncludedCols.exists(x => x.equalsIgnoreCase(field.column)) &&
            !dictExcludedCols.exists(x => x.equalsIgnoreCase(field.column))) {
          msrFields :+= field
        }
      }
    })

    msrFields
  }

  def getKey(parentColumnName: Option[String],
             columnName: String): (String, String) = {
    if (parentColumnName.isDefined) {
      if (columnName == "val") {
        (parentColumnName.get, parentColumnName.get + "." + columnName)
      } else {
        (parentColumnName.get + "." + columnName, parentColumnName.get + "." + columnName)
      }
    } else {
      (columnName, columnName)
    }
  }

  protected def fillColumnProperty(parentColumnName: Option[String],
                                   columnName: String,
                                   tableProperties: Map[String, String],
                                   colPropMap: java.util.HashMap[String, java.util.List[ColumnProperty]]) {
    val (tblPropKey, colProKey) = getKey(parentColumnName, columnName)
    val colProps = CommonUtil.getColumnProperties(tblPropKey, tableProperties)
    if (colProps.isDefined) {
      colPropMap.put(colProKey, colProps.get)
    }
  }

  protected def fillAllChildrenColumnProperty(
      parent: String,
      fieldChildren: Option[List[Field]],
      tableProperties: Map[String, String],
      colPropMap: java.util.HashMap[String, java.util.List[ColumnProperty]]) {
    fieldChildren.foreach { fields =>
      fields.foreach { field =>
        fillColumnProperty(Some(parent), field.column, tableProperties, colPropMap)
      }
    }
  }

  protected def extractColumnProperties(fields: Seq[Field], tableProperties: Map[String, String]):
  java.util.Map[String, java.util.List[ColumnProperty]] = {
    val colPropMap = new java.util.HashMap[String, java.util.List[ColumnProperty]]()
    fields.foreach { field =>
      if (field.children.isDefined && field.children.get != null) {
        fillAllChildrenColumnProperty(field.column, field.children, tableProperties, colPropMap)
      } else {
        fillColumnProperty(None, field.column, tableProperties, colPropMap)
      }
    }
    colPropMap
  }

  def rearrangedColumnGroup(colGroup: String, dims: Seq[Field]): String = {
    // if columns in column group is not in schema order than arrange it in schema order
    var colGrpFieldIndx: Seq[Int] = Seq[Int]()
    colGroup.split(',').map(_.trim).foreach { x =>
      dims.zipWithIndex.foreach { dim =>
        if (dim._1.column.equalsIgnoreCase(x)) {
          colGrpFieldIndx :+= dim._2
        }
      }
    }
    // sort it
    colGrpFieldIndx = colGrpFieldIndx.sorted
    // check if columns in column group is in schema order
    if (!checkIfInSequence(colGrpFieldIndx)) {
      throw new MalformedCarbonCommandException("Invalid column group:" + colGroup)
    }
    def checkIfInSequence(colGrpFieldIndx: Seq[Int]): Boolean = {
      for (i <- 0 until (colGrpFieldIndx.length - 1)) {
        if ((colGrpFieldIndx(i + 1) - colGrpFieldIndx(i)) != 1) {
          throw new MalformedCarbonCommandException(
            "Invalid column group,column in group should be contiguous as per schema.")
        }
      }
      true
    }
    val colGrpNames: StringBuilder = StringBuilder.newBuilder
    for (i <- colGrpFieldIndx.indices) {
      colGrpNames.append(dims(colGrpFieldIndx(i)).column)
      if (i < (colGrpFieldIndx.length - 1)) {
        colGrpNames.append(",")
      }
    }
    colGrpNames.toString()
  }

  /**
    * Extract the column groups configuration from table properties.
    * Based on this Row groups of fields will be determined.
    *
    * @param tableProperties
    * @return
    */
  protected def updateColumnGroupsInField(tableProperties: Map[String, String],
                                          noDictionaryDims: Seq[String],
                                          msrs: Seq[Field],
                                          dims: Seq[Field]): Seq[String] = {
    if (tableProperties.get(CarbonCommonConstants.COLUMN_GROUPS).isDefined) {

      var splittedColGrps: Seq[String] = Seq[String]()
      val nonSplitCols: String = tableProperties.get(CarbonCommonConstants.COLUMN_GROUPS).get

      // row groups will be specified in table properties like -> "(col1,col2),(col3,col4)"
      // here first splitting the value by () . so that the above will be splitted into 2 strings.
      // [col1,col2] [col3,col4]
      val m: Matcher = Pattern.compile("\\(([^)]+)\\)").matcher(nonSplitCols)
      while (m.find()) {
        val oneGroup: String = m.group(1)
        CommonUtil.validateColumnGroup(oneGroup, noDictionaryDims, msrs, splittedColGrps, dims)
        val arrangedColGrp = rearrangedColumnGroup(oneGroup, dims)
        splittedColGrps :+= arrangedColGrp
      }
      // This will  be furthur handled.
      CommonUtil.arrangeColGrpsInSchemaOrder(splittedColGrps, dims)
    } else {
      null
    }
  }

  private def reorderDimensions(dims: Seq[Field]): Seq[Field] = {
    var complexDimensions: Seq[Field] = Seq()
    var dimensions: Seq[Field] = Seq()
    dims.foreach { dimension =>
      dimension.dataType.getOrElse("NIL") match {
        case "Array" => complexDimensions = complexDimensions :+ dimension
        case "Struct" => complexDimensions = complexDimensions :+ dimension
        case _ => dimensions = dimensions :+ dimension
      }
    }
    dimensions ++ complexDimensions
  }

  /**
    * This will extract the no inverted columns fields.
    * By default all dimensions use inverted index.
    *
    * @param fields
    * @param tableProperties
    * @return
    */
  protected def extractNoInvertedIndexColumns(fields: Seq[Field],
                                              tableProperties: Map[String, String]):
  Seq[String] = {
    // check whether the column name is in fields
    var noInvertedIdxColsProps: Array[String] = Array[String]()
    var noInvertedIdxCols: Seq[String] = Seq[String]()

    if (tableProperties.get("NO_INVERTED_INDEX").isDefined) {
      noInvertedIdxColsProps =
          tableProperties.get("NO_INVERTED_INDEX").get.split(',').map(_.trim)
      noInvertedIdxColsProps
          .map { noInvertedIdxColProp =>
            if (!fields.exists(x => x.column.equalsIgnoreCase(noInvertedIdxColProp))) {
              val errormsg = "NO_INVERTED_INDEX column: " + noInvertedIdxColProp +
                  " does not exist in table. Please check create table statement."
              throw new MalformedCarbonCommandException(errormsg)
            }
          }
    }
    // check duplicate columns and only 1 col left
    val distinctCols = noInvertedIdxColsProps.toSet
    // extract the no inverted index columns
    fields.foreach(field => {
      if (distinctCols.exists(x => x.equalsIgnoreCase(field.column))) {
        noInvertedIdxCols :+= field.column
      }
    }
    )
    noInvertedIdxCols
  }

  private def normalizeType(field: Field): Field = {
    val dataType = field.dataType.getOrElse("NIL")
    dataType match {
      case "string" => Field(field.column, Some("String"), field.name, Some(null), field.parent,
        field.storeType
      )
      case "integer" | "int" => Field(field.column, Some("Integer"), field.name, Some(null),
        field.parent, field.storeType
      )
      case "long" => Field(field.column, Some("Long"), field.name, Some(null), field.parent,
        field.storeType
      )
      case "double" => Field(field.column, Some("Double"), field.name, Some(null), field.parent,
        field.storeType
      )
      case "timestamp" => Field(field.column, Some("Timestamp"), field.name, Some(null),
        field.parent, field.storeType
      )
      case "numeric" => Field(field.column, Some("Numeric"), field.name, Some(null), field.parent,
        field.storeType
      )
      case "array" => Field(field.column, Some("Array"), field.name,
        field.children.map(f => f.map(normalizeType(_))),
        field.parent, field.storeType
      )
      case "struct" => Field(field.column, Some("Struct"), field.name,
        field.children.map(f => f.map(normalizeType(_))),
        field.parent, field.storeType
      )
      case "bigint" => Field(field.column, Some("BigInt"), field.name, Some(null), field.parent,
        field.storeType
      )
      case "decimal" => Field(field.column, Some("Decimal"), field.name, Some(null), field.parent,
        field.storeType, field.precision, field.scale
      )
      // checking if the nested data type contains the child type as decimal(10,0),
      // if it is present then extracting the precision and scale. resetting the data type
      // with Decimal.
      case _ if (dataType.startsWith("decimal")) =>
        val (precision, scale) = getScaleAndPrecision(dataType)
        Field(field.column,
          Some("Decimal"),
          field.name,
          Some(null),
          field.parent,
          field.storeType, precision,
          scale
        )
      case _ =>
        field
    }
  }

  private def appendParentForEachChild(field: Field, parentName: String): Field = {
    field.dataType.getOrElse("NIL") match {
      case "String" => Field(parentName + "." + field.column, Some("String"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Integer" => Field(parentName + "." + field.column, Some("Integer"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Long" => Field(parentName + "." + field.column, Some("Long"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Double" => Field(parentName + "." + field.column, Some("Double"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Timestamp" => Field(parentName + "." + field.column, Some("Timestamp"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Numeric" => Field(parentName + "." + field.column, Some("Numeric"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Array" => Field(parentName + "." + field.column, Some("Array"),
        Some(parentName + "." + field.name.getOrElse(None)),
        field.children
            .map(f => f.map(appendParentForEachChild(_, parentName + "." + field.column))),
        parentName)
      case "Struct" => Field(parentName + "." + field.column, Some("Struct"),
        Some(parentName + "." + field.name.getOrElse(None)),
        field.children
            .map(f => f.map(appendParentForEachChild(_, parentName + "." + field.column))),
        parentName)
      case "BigInt" => Field(parentName + "." + field.column, Some("BigInt"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Decimal" => Field(parentName + "." + field.column, Some("Decimal"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName,
        field.storeType, field.precision, field.scale)
      case _ => field
    }
  }

  private def addParent(field: Field): Field = {
    field.dataType.getOrElse("NIL") match {
      case "Array" => Field(field.column, Some("Array"), field.name,
        field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent,
        field.storeType)
      case "Struct" => Field(field.column, Some("Struct"), field.name,
        field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent,
        field.storeType)
      case _ => field
    }
  }

  def getScaleAndPrecision(dataType: String): (Int, Int) = {
    val m: Matcher = Pattern.compile("^decimal\\(([^)]+)\\)").matcher(dataType)
    m.find()
    val matchedString: String = m.group(1)
    val scaleAndPrecision = matchedString.split(",")
    (Integer.parseInt(scaleAndPrecision(0).trim), Integer.parseInt(scaleAndPrecision(1).trim))
  }

  def prepareTableModel(ifNotExistPresent: Boolean, dbName: Option[String]
                        , tableName: String, fields: Seq[Field],
                        partitionCols: Seq[PartitionerField],
                        tableProperties: Map[String, String]): TableModel
  = {

    val (dims: Seq[Field], noDictionaryDims: Seq[String]) = extractDimColsAndNoDictionaryFields(
      fields, tableProperties)
    if (dims.isEmpty) {
      throw new MalformedCarbonCommandException(s"Table ${
        dbName.getOrElse(
          CarbonCommonConstants.DATABASE_DEFAULT_NAME)
      }.$tableName"
          +
          " can not be created without key columns. Please " +
          "use DICTIONARY_INCLUDE or " +
          "DICTIONARY_EXCLUDE to set at least one key " +
          "column " +
          "if all specified columns are numeric types")
    }
    val msrs: Seq[Field] = extractMsrColsFromFields(fields, tableProperties)

    // column properties
    val colProps = extractColumnProperties(fields, tableProperties)
    // get column groups configuration from table properties.
    val groupCols: Seq[String] = updateColumnGroupsInField(tableProperties,
      noDictionaryDims, msrs, dims)

    // get no inverted index columns from table properties.
    val noInvertedIdxCols = extractNoInvertedIndexColumns(fields, tableProperties)

    // validate the tableBlockSize from table properties
    CommonUtil.validateTableBlockSize(tableProperties)

    TableModel(ifNotExistPresent,
      dbName.getOrElse(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      dbName,
      tableName,
      tableProperties,
      reorderDimensions(dims.map(f => normalizeType(f)).map(f => addParent(f))),
      msrs.map(f => normalizeType(f)),
      Option(noDictionaryDims),
      Option(noInvertedIdxCols),
      groupCols,
      Some(colProps))
  }

}
