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
package org.apache.carbondata.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.CarbonQueryPlan;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MapredCarbonInputFormat extends CarbonTableInputFormat<ArrayWritable>
    implements InputFormat<Void, ArrayWritable>, CombineHiveInputFormat.AvoidSplitCombination {
  private static final String TABLE_INFO = "mapreduce.input.carboninputformat.tableinfo";
  private static final String TABLE_PATH = "tablePath";

  @Override public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    populateCarbonTable(jobConf);
    String tableInfoStr = jobConf.get(TABLE_INFO);
    Job jobContext = Job.getInstance(jobConf);
    List<org.apache.hadoop.mapreduce.InputSplit> splitList = super.getSplits(jobContext);
    InputSplit[] splits = new InputSplit[splitList.size()];
    CarbonInputSplit split;
    for (int i = 0; i < splitList.size(); i++) {
      split = (CarbonInputSplit) splitList.get(i);
      splits[i] = new CarbonHiveInputSplit(split.getSegmentId(), split.getPath(), split.getStart(),
          split.getLength(), split.getLocations(), split.getNumberOfBlocklets(),
          split.getVersion(), split.getBlockStorageIdMap(),
          split.getDeleteDeltaFiles(), split.getDetailInfo(), tableInfoStr);
    }
    return splits;
  }

  @Override
  public RecordReader<Void, ArrayWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf,
      Reporter reporter) throws IOException {
    CarbonHiveInputSplit hiveInputSplit = (CarbonHiveInputSplit)inputSplit;
    jobConf.set(TABLE_INFO, hiveInputSplit.getTableInfo());
    jobConf.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
    QueryModel queryModel = getQueryModel(jobConf);
    CarbonReadSupport<ArrayWritable> readSupport = new CarbonDictionaryDecodeReadSupport<>();
    return new CarbonHiveRecordReader(queryModel, readSupport, inputSplit, jobConf);
  }

  /**
   * this method will read the schema from the physical file and populate into CARBON_TABLE
   *
   * @param configuration
   * @throws IOException
   */
  private CarbonTable populateCarbonTable(Configuration configuration)
      throws IOException {
    TableInfo tableInfo = getTableInfo(configuration);
    CarbonMetadata metadata = CarbonMetadata.getInstance();
    CarbonTable carbonTable = null;
    if (tableInfo != null) {
      carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
      metadata.addCarbonTable(carbonTable);
      return carbonTable;
    }
    String[] paths = configuration.get(FileInputFormat.INPUT_DIR).split(",");
    Arrays.sort(paths);
    String tablePath = paths[0];
    AbsoluteTableIdentifier absoluteTableIdentifier = null;
    try {
      absoluteTableIdentifier = AbsoluteTableIdentifier
        .from(tablePath, getDatabaseName(configuration), getTableName(configuration));
    } catch (InvalidConfigurationException e) {
      throw new IOException(e);
    }
    // read the schema file to get the absoluteTableIdentifier having the correct table id
    // persisted in the schema
    CarbonTableIdentifier tableIdentifier = absoluteTableIdentifier.getCarbonTableIdentifier();
    carbonTable = metadata.getCarbonTable(tableIdentifier.getTableUniqueName());
    if (carbonTable == null) {
      if (configuration.get("carbonSchemaPartsNo") != null) {
        carbonTable = metadata.getCarbonTable(tableIdentifier.getTableUniqueName());
        if (carbonTable == null) {
          Map<String, String> properties = configuration.getValByRegex("carbon.+");
          String partsNo = properties.get("carbonSchemaPartsNo");
          int numParts = Integer.parseInt(partsNo);
          String schemaPart = null;
          for (int i = 0; i < numParts; i++) {
            schemaPart = properties.get("carbonSchema" + i);
            schemaPart = schemaPart.replaceAll("\\\\", "");
            properties.put("carbonSchema" + i, schemaPart);
          }
          tableInfo = CarbonUtil.convertGsonToTableInfo(properties);
          metadata.loadTableMetadata(tableInfo);
          carbonTable = metadata.getCarbonTable(tableIdentifier.getTableUniqueName());
        }
      } else {
        carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier);
        metadata.addCarbonTable(carbonTable);
      }
    }
    setTableInfo(configuration, carbonTable.getTableInfo());

    return carbonTable;
  }

  private QueryModel getQueryModel(Configuration configuration) throws IOException {
    CarbonTable carbonTable = populateCarbonTable(configuration);
    TableProvider tableProvider = new SingleTableProvider(carbonTable);
    // getting the table absoluteTableIdentifier from the carbonTable
    // to avoid unnecessary deserialization

    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
    String projection = getProjection(configuration, carbonTable,
        identifier.getCarbonTableIdentifier().getTableName());
    CarbonQueryPlan queryPlan = CarbonInputFormatUtil.createQueryPlan(carbonTable, projection);
    QueryModel queryModel =
        QueryModel.createModel(identifier, queryPlan, carbonTable, new DataTypeConverterImpl());
    // set the filter to the query model in order to filter blocklet before scan
    Expression filter = getFilterPredicates(configuration);
    CarbonInputFormatUtil.processFilterExpression(filter, carbonTable, null, null);
    FilterResolverIntf filterIntf =
        CarbonInputFormatUtil.resolveFilter(filter, identifier, tableProvider);
    queryModel.setFilterExpressionResolverTree(filterIntf);

    return queryModel;
  }

  /**
   * Return the Projection for the CarbonQuery.
   *
   * @param configuration
   * @param carbonTable
   * @param tableName
   * @return
   */
  private String getProjection(Configuration configuration, CarbonTable carbonTable,
      String tableName) {
    // query plan includes projection column
    String projection = getColumnProjection(configuration);
    if (projection == null) {
      projection = configuration.get("hive.io.file.readcolumn.names");
    }
    List<CarbonColumn> carbonColumns = carbonTable.getCreateOrderColumn(tableName);
    List<String> carbonColumnNames = new ArrayList<>();
    StringBuilder allColumns = new StringBuilder();
    StringBuilder projectionColumns = new StringBuilder();
    for (CarbonColumn column : carbonColumns) {
      carbonColumnNames.add(column.getColName().toLowerCase());
      allColumns.append(column.getColName() + ",");
    }

    if (!projection.equals("")) {
      String[] columnNames = projection.split(",");
      //verify that the columns parsed by Hive exist in the table
      for (String col : columnNames) {
        //show columns command will return these data
        if (carbonColumnNames.contains(col.toLowerCase())) {
          projectionColumns.append(col + ",");
        }
      }
      return projectionColumns.substring(0, projectionColumns.lastIndexOf(","));
    } else {
      return allColumns.substring(0, allColumns.lastIndexOf(","));
    }
  }

  @Override public boolean shouldSkipCombine(Path path, Configuration conf) throws IOException {
    return true;
  }
}
