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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.dev.DataMapModel;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMap;
import org.apache.carbondata.core.datamap.dev.cgdatamap.CoarseGrainDataMapFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.events.Event;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

@InterfaceAudience.Internal
public class LuceneCoarseGrainDataMapFactory extends CoarseGrainDataMapFactory {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(LuceneCoarseGrainDataMapFactory.class.getName());

  /**
   * table's index columns
   */
  private DataMapMeta dataMapMeta = null;

  /**
   * analyzer for lucene
   */
  private Analyzer analyzer = null;

  /**
   * index name
   */
  private String dataMapName = null;

  /**
   * table identifier
   */
  private AbsoluteTableIdentifier tableIdentifier = null;

  /**
   * Initialization of Datamap factory with the identifier and datamap name
   */
  public void init(AbsoluteTableIdentifier identifier, String dataMapName) throws IOException {
    this.tableIdentifier = identifier;
    this.dataMapName = dataMapName;

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

    TableInfo tableInfo = carbonTable.getTableInfo();
    List<ColumnSchema> lstCoumnSchemas = tableInfo.getFactTable().getListOfColumns();

    // add all columns into lucene indexer
    // TODO:only add index columns
    List<String> indexedColumns = new ArrayList<String>();
    for (ColumnSchema columnSchema : lstCoumnSchemas) {
      if (!columnSchema.isInvisible()) {
        indexedColumns.add(columnSchema.getColumnName());
      }
    }

    // get dataMapSchema by name
    DataMapSchema dataMapSchema = null;
    List<DataMapSchema> lstDataMapSchema = tableInfo.getDataMapSchemaList();
    for (DataMapSchema dataMapSchemaTemp : lstDataMapSchema) {
      if (dataMapSchemaTemp.getDataMapName().equals(dataMapName)) {
        dataMapSchema = dataMapSchemaTemp;
        break;
      }
    }

    // get indexed columns
    if (dataMapSchema != null) {
      Map<String, String> properties = dataMapSchema.getProperties();
      String columns = properties.get(CarbonCommonConstants.TEXT_COLUMNS);
      if (columns != null) {
        String[] columnArray = columns.split(CarbonCommonConstants.COMMA, -1);
        Collections.addAll(indexedColumns, columnArray);
      }
    }

    // add optimizedOperations
    List<ExpressionType> optimizedOperations = new ArrayList<ExpressionType>();
    optimizedOperations.add(ExpressionType.EQUALS);
    optimizedOperations.add(ExpressionType.GREATERTHAN);
    optimizedOperations.add(ExpressionType.GREATERTHAN_EQUALTO);
    optimizedOperations.add(ExpressionType.LESSTHAN);
    optimizedOperations.add(ExpressionType.LESSTHAN_EQUALTO);
    optimizedOperations.add(ExpressionType.NOT);
    this.dataMapMeta = new DataMapMeta(indexedColumns, optimizedOperations);

    // get analyzer
    // TODO: how to get analyzer ?
    analyzer = new StandardAnalyzer();
  }

  public void init(AbsoluteTableIdentifier identifier, DataMapSchema dataMapSchema)
      throws IOException {

  }

  /**
   * Return a new write for this datamap
   */
  public DataMapWriter createWriter(String segmentId, String writeDirectoryPath) {
    LOGGER.info("lucene data write to " + writeDirectoryPath);
    return new LuceneDataMapWriter(tableIdentifier, dataMapName, segmentId, writeDirectoryPath,
        dataMapMeta, false);
  }

  /**
   * Get the datamap for segmentid
   */
  public List<CoarseGrainDataMap> getDataMaps(String segmentId) throws IOException {
    List<CoarseGrainDataMap> lstDataMap = new ArrayList<>();
    CoarseGrainDataMap dataMap =
        new LuceneCoarseGrainDataMap(tableIdentifier, dataMapName, segmentId, analyzer);
    try {
      dataMap.init(new DataMapModel(
          tableIdentifier.getTablePath() + "/Fact/Part0/Segment_" + segmentId + File.separator
              + dataMapName));
    } catch (MemoryException e) {
      LOGGER.error("failed to get lucene datamap , detail is {}" + e.getMessage());
      return lstDataMap;
    }
    lstDataMap.add(dataMap);
    return lstDataMap;
  }

  /**
   * Get datamaps for distributable object.
   */
  public List<CoarseGrainDataMap> getDataMaps(DataMapDistributable distributable)
      throws IOException {
    return getDataMaps(distributable.getSegmentId());
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
