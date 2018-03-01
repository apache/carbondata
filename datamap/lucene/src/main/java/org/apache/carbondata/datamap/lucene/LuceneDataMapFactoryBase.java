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

package org.apache.carbondata.datamap.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * Base implementation for CG and FG lucene DataMapFactory.
 */
@InterfaceAudience.Internal
abstract class LuceneDataMapFactoryBase<T extends DataMap> implements DataMapFactory<T> {

  static final String TEXT_COLUMNS = "text_columns";

  /**
   * Logger
   */
  final LogService LOGGER = LogServiceFactory.getLogService(this.getClass().getName());

  /**
   * table's index columns
   */
  DataMapMeta dataMapMeta = null;

  /**
   * analyzer for lucene
   */
  Analyzer analyzer = null;

  /**
   * index name
   */
  String dataMapName = null;

  /**
   * table identifier
   */
  AbsoluteTableIdentifier tableIdentifier = null;

  @Override
  public void init(AbsoluteTableIdentifier identifier, DataMapSchema dataMapSchema)
      throws IOException, MalformedDataMapCommandException {
    Objects.requireNonNull(identifier);
    Objects.requireNonNull(dataMapSchema);

    this.tableIdentifier = identifier;
    this.dataMapName = dataMapSchema.getDataMapName();

    // get carbonmetadata from carbonmetadata instance
    CarbonMetadata carbonMetadata = CarbonMetadata.getInstance();

    String tableUniqueName = identifier.getCarbonTableIdentifier().getTableUniqueName();

    // get carbon table
    CarbonTable carbonTable = carbonMetadata.getCarbonTable(tableUniqueName);
    if (carbonTable == null) {
      String errorMessage =
          String.format("failed to get carbon table with name %s", tableUniqueName);
      LOGGER.error(errorMessage);
      throw new IOException(errorMessage);
    }

    // validate DataMapSchema and get index columns
    List<String> indexedColumns =  validateAndGetIndexedColumns(dataMapSchema, carbonTable);

    // add optimizedOperations
    List<ExpressionType> optimizedOperations = new ArrayList<ExpressionType>();
    // optimizedOperations.add(ExpressionType.EQUALS);
    // optimizedOperations.add(ExpressionType.GREATERTHAN);
    // optimizedOperations.add(ExpressionType.GREATERTHAN_EQUALTO);
    // optimizedOperations.add(ExpressionType.LESSTHAN);
    // optimizedOperations.add(ExpressionType.LESSTHAN_EQUALTO);
    // optimizedOperations.add(ExpressionType.NOT);
    optimizedOperations.add(ExpressionType.TEXT_MATCH);
    this.dataMapMeta = new DataMapMeta(indexedColumns, optimizedOperations);

    // get analyzer
    // TODO: how to get analyzer ?
    analyzer = new StandardAnalyzer();
  }

  private List<String> validateAndGetIndexedColumns(DataMapSchema dataMapSchema,
      CarbonTable carbonTable) throws MalformedDataMapCommandException {
    String textColumnsStr = dataMapSchema.getProperties().get(TEXT_COLUMNS);
    if (textColumnsStr == null || StringUtils.isBlank(textColumnsStr)) {
      throw new MalformedDataMapCommandException(
          "Lucene DataMap require proper TEXT_COLUMNS property.");
    }
    String[] textColumns = textColumnsStr.split(",", -1);
    for (int i = 0; i < textColumns.length; i++) {
      textColumns[i] = textColumns[i].trim().toLowerCase();
    }
    for (int i = 0; i < textColumns.length; i++) {
      if (textColumns[i].isEmpty()) {
        throw new MalformedDataMapCommandException("TEXT_COLUMNS contains illegal argumnet.");
      }
      for (int j = i + 1; j < textColumns.length; j++) {
        if (textColumns[i].equals(textColumns[j])) {
          throw new MalformedDataMapCommandException(
              "TEXT_COLUMNS has duplicate columns :" + textColumns[i]);
        }
      }
    }
    List<String> textColumnList = new ArrayList<String>(textColumns.length);
    for (int i = 0; i < textColumns.length; i++) {
      CarbonColumn column = carbonTable.getColumnByName(carbonTable.getTableName(), textColumns[i]);
      if (null == column) {
        throw new MalformedDataMapCommandException("TEXT_COLUMNS: " + textColumns[i]
            + " does not exist in table. Please check create DataMap statement.");
      } else if (column.getDataType() != DataTypes.STRING) {
        throw new MalformedDataMapCommandException(
            "TEXT_COLUMNS only supports String column. " + "Unsupported column: " + textColumns[i]
                + ", DataType: " + column.getDataType());
      }
      textColumnList.add(column.getColName());
    }
    return textColumnList;
  }

  /**
   * Return a new write for this datamap
   */
  public DataMapWriter createWriter(String segmentId, String writeDirectoryPath) {
    LOGGER.info("lucene data write to " + writeDirectoryPath);
    return new LuceneDataMapWriter(
        tableIdentifier, dataMapName, segmentId, writeDirectoryPath, true);
  }

  /**
   * Get all distributable objects of a segmentid
   */
  public List<DataMapDistributable> toDistributable(String segmentId) {
    List<DataMapDistributable> lstDataMapDistribute = new ArrayList<DataMapDistributable>();
    DataMapDistributable luceneDataMapDistributable = new LuceneDataMapDistributable(
        CarbonTablePath.getSegmentPath(tableIdentifier.getTablePath(), segmentId));
    lstDataMapDistribute.add(luceneDataMapDistributable);
    return lstDataMapDistribute;
  }

  public void fireEvent(Event event) {

  }

  /**
   * Clears datamap of the segment
   */
  public void clear(String segmentId) {

  }

  /**
   * Clear all datamaps from memory
   */
  public void clear() {

  }

  /**
   * Return metadata of this datamap
   */
  public DataMapMeta getMeta() {
    return dataMapMeta;
  }
}
