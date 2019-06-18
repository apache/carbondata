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
import java.util.UUID;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.exception.InvalidConfigurationException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

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
import org.apache.log4j.Logger;

public class MapredCarbonInputFormat extends CarbonTableInputFormat<ArrayWritable>
    implements InputFormat<Void, ArrayWritable>, CombineHiveInputFormat.AvoidSplitCombination {
  private static final String CARBON_TABLE = "mapreduce.input.carboninputformat.table";

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(MapredCarbonInputFormat.class.getCanonicalName());

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
          inputPath = inputPath.replace("file:", "");
          if (FileFactory.isFileExist(inputPath)) {
            validInputPath = inputPath;
            break;
          }
        }
      }
    }
    if (null != validInputPath) {
      AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier
          .from(validInputPath, getDatabaseName(configuration), getTableName(configuration));
      // read the schema file to get the absoluteTableIdentifier having the correct table id
      // persisted in the schema
      CarbonTable carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier);
      configuration.set(CARBON_TABLE, ObjectSerializationUtil.convertObjectToString(carbonTable));
      setTableInfo(configuration, carbonTable.getTableInfo());
    } else {
      throw new InvalidPathException("No input paths specified in job");
    }
  }

  private static CarbonTable getCarbonTable(Configuration configuration, String path)
      throws IOException, InvalidConfigurationException {
    populateCarbonTable(configuration, path);
    // read it from schema file in the store
    String carbonTableStr = configuration.get(CARBON_TABLE);
    return (CarbonTable) ObjectSerializationUtil.convertStringToObject(carbonTableStr);
  }

  @Override public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    jobConf.set(DATABASE_NAME, "_dummyDb_" + UUID.randomUUID().toString());
    jobConf.set(TABLE_NAME, "_dummyTable_" + UUID.randomUUID().toString());
    org.apache.hadoop.mapreduce.JobContext jobContext = Job.getInstance(jobConf);
    CarbonTableInputFormat carbonTableInputFormat = new CarbonTableInputFormat();
    List<org.apache.hadoop.mapreduce.InputSplit> splitList =
        carbonTableInputFormat.getSplits(jobContext);
    InputSplit[] splits = new InputSplit[splitList.size()];
    CarbonInputSplit split;
    for (int i = 0; i < splitList.size(); i++) {
      split = (CarbonInputSplit) splitList.get(i);
      CarbonHiveInputSplit inputSplit = new CarbonHiveInputSplit(split.getSegmentId(),
              split.getPath(), split.getStart(), split.getLength(),
              split.getLocations(), split.getNumberOfBlocklets(),
              split.getVersion(), split.getBlockStorageIdMap(), split.getDetailInfo());
      splits[i] = inputSplit;
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
      jobConf.set(DATABASE_NAME, "_dummyDb_" + UUID.randomUUID().toString());
      jobConf.set(TABLE_NAME, "_dummyTable_" + UUID.randomUUID().toString());
      queryModel = getQueryModel(jobConf, path);
    } catch (InvalidConfigurationException e) {
      LOGGER.error("Failed to create record reader: " + e.getMessage(), e);
      return null;
    }
    CarbonReadSupport<ArrayWritable> readSupport = new CarbonDictionaryDecodeReadSupport<>();
    return new CarbonHiveRecordReader(queryModel, readSupport, inputSplit, jobConf);
  }

  private QueryModel getQueryModel(Configuration configuration, String path)
      throws IOException, InvalidConfigurationException {
    CarbonTable carbonTable = getCarbonTable(configuration, path);
    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
    String projectionString = getProjection(configuration, carbonTable,
        identifier.getCarbonTableIdentifier().getTableName());
    String[] projectionColumns = projectionString.split(",");
    QueryModel queryModel =
        new QueryModelBuilder(carbonTable)
            .projectColumns(projectionColumns)
            .filterExpression(getFilterPredicates(configuration))
            .dataConverter(new DataTypeConverterImpl())
            .build();

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

    if (null != projection && !projection.equals("")) {
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
