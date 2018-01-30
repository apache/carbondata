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
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.apache.carbondata.hadoop.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.util.SchemaReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;

public class MapredCarbonInputFormat extends CarbonTableInputFormat<ArrayWritable>
    implements InputFormat<Void, ArrayWritable>, CombineHiveInputFormat.AvoidSplitCombination {
  private static final String CARBON_TABLE = "mapreduce.input.carboninputformat.table";

  private LogService LOGGER = LogServiceFactory.getLogService(this.getClass().getCanonicalName());

  /**
   * this method will read the schema from the physical file and populate into CARBON_TABLE
   *
   * @param configuration
   * @throws IOException
   */
  private static void populateCarbonTable(Configuration configuration, String paths)
      throws IOException, InvalidConfigurationException {
    String dirs = configuration.get(INPUT_DIR, "");
    String[] inputPaths = StringUtils.split(dirs);
    String validInputPath = null;
    if (inputPaths.length == 0) {
      throw new InvalidPathException("No input paths specified in job");
    } else {
      if (paths != null) {
        for (String inputPath : inputPaths) {
          if (paths.startsWith(inputPath.replace("file:", ""))) {
            validInputPath = inputPath;
            break;
          }
        }
      }
    }
    AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier
        .from(validInputPath, getDatabaseName(configuration), getTableName(configuration));
    // read the schema file to get the absoluteTableIdentifier having the correct table id
    // persisted in the schema
    CarbonTable carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier);
    configuration.set(CARBON_TABLE, ObjectSerializationUtil.convertObjectToString(carbonTable));
    setTableInfo(configuration, carbonTable.getTableInfo());
  }

  private static CarbonTable getCarbonTable(Configuration configuration, String path)
      throws IOException, InvalidConfigurationException {
    populateCarbonTable(configuration, path);
    // read it from schema file in the store
    String carbonTableStr = configuration.get(CARBON_TABLE);
    return (CarbonTable) ObjectSerializationUtil.convertStringToObject(carbonTableStr);
  }

  @Override public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    org.apache.hadoop.mapreduce.JobContext jobContext = Job.getInstance(jobConf);
    List<org.apache.hadoop.mapreduce.InputSplit> splitList = super.getSplits(jobContext);
    InputSplit[] splits = new InputSplit[splitList.size()];
    CarbonInputSplit split;
    for (int i = 0; i < splitList.size(); i++) {
      split = (CarbonInputSplit) splitList.get(i);
      splits[i] = new CarbonHiveInputSplit(split.getSegmentId(), split.getPath(), split.getStart(),
          split.getLength(), split.getLocations(), split.getNumberOfBlocklets(), split.getVersion(),
          split.getBlockStorageIdMap());
    }
    return splits;
  }

  @Override
  public RecordReader<Void, ArrayWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf,
      Reporter reporter) throws IOException {
    String path = null;
    if (inputSplit instanceof CarbonHiveInputSplit) {
      path = ((CarbonHiveInputSplit) inputSplit).getPath().toString();
    }
    QueryModel queryModel = null;
    try {
      queryModel = getQueryModel(jobConf, path);
    } catch (InvalidConfigurationException e) {
      LOGGER.error("Failed to create record reader: " + e.getMessage());
      return null;
    }
    CarbonReadSupport<ArrayWritable> readSupport = new CarbonDictionaryDecodeReadSupport<>();
    return new CarbonHiveRecordReader(queryModel, readSupport, inputSplit, jobConf);
  }

  private QueryModel getQueryModel(Configuration configuration, String path)
      throws IOException, InvalidConfigurationException {
    CarbonTable carbonTable = getCarbonTable(configuration, path);
    TableProvider tableProvider = new SingleTableProvider(carbonTable);
    // getting the table absoluteTableIdentifier from the carbonTable
    // to avoid unnecessary deserialization

    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();

    String projectionString = getProjection(configuration, carbonTable,
        identifier.getCarbonTableIdentifier().getTableName());
    String[] projectionColumns = projectionString.split(",");
    QueryModel queryModel = carbonTable.createQueryWithProjection(
        projectionColumns, new DataTypeConverterImpl());
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
