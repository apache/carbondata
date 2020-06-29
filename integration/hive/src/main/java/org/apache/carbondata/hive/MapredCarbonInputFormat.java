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
import org.apache.carbondata.core.index.IndexFilter;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonFileInputFormat;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.InvalidPathException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

public class MapredCarbonInputFormat extends CarbonTableInputFormat<ArrayWritable>
    implements InputFormat<Void, ArrayWritable>, CombineHiveInputFormat.AvoidSplitCombination {
  private static final String CARBON_TABLE = "mapreduce.input.carboninputformat.table";

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(MapredCarbonInputFormat.class.getCanonicalName());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3024

  /**
   * This method will read the schema from the physical file and populate into CARBON_TABLE
   */
  private static void populateCarbonTable(Configuration configuration, String paths)
      throws IOException, InvalidConfigurationException {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3687
    if (null != paths) {
      // read the schema file to get the absoluteTableIdentifier having the correct table id
      // persisted in the schema
      CarbonTable carbonTable;
      AbsoluteTableIdentifier absoluteTableIdentifier = AbsoluteTableIdentifier
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3771
          .from(configuration.get(hive_metastoreConstants.META_TABLE_LOCATION),
              getDatabaseName(configuration), getTableName(configuration));
      String schemaPath =
          CarbonTablePath.getSchemaFilePath(absoluteTableIdentifier.getTablePath(), configuration);
      if (FileFactory.getCarbonFile(schemaPath).exists()) {
        // read the schema file to get the absoluteTableIdentifier having the correct table id
        // persisted in the schema
        carbonTable = SchemaReader.readCarbonTableFromStore(absoluteTableIdentifier);
      } else {
        // InferSchema from data file
        carbonTable = CarbonTable.buildFromTableInfo(SchemaReader
            .inferSchema(absoluteTableIdentifier, false));
        carbonTable.setTransactionalTable(false);
      }
      configuration.set(CARBON_TABLE, ObjectSerializationUtil.convertObjectToString(carbonTable));
      setTableInfo(configuration, carbonTable.getTableInfo());
    } else {
      throw new InvalidPathException("No input paths specified in job");
    }
  }

  private static CarbonTable getCarbonTable(Configuration configuration, String path)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1994
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1995
      throws IOException, InvalidConfigurationException {
    populateCarbonTable(configuration, path);
    // read it from schema file in the store
    String carbonTableStr = configuration.get(CARBON_TABLE);
    return (CarbonTable) ObjectSerializationUtil.convertStringToObject(carbonTableStr);
  }

  @Override
  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    jobConf.set(DATABASE_NAME, "_dummyDb_" + UUID.randomUUID().toString());
    jobConf.set(TABLE_NAME, "_dummyTable_" + UUID.randomUUID().toString());
    org.apache.hadoop.mapreduce.JobContext jobContext = Job.getInstance(jobConf);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3687
    CarbonTable carbonTable;
    try {
      carbonTable = getCarbonTable(jobContext.getConfiguration(),
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3771
          jobContext.getConfiguration().get(hive_metastoreConstants.META_TABLE_LOCATION));
    } catch (Exception e) {
      throw new IOException("Unable read Carbon Schema: ", e);
    }
    List<String> partitionNames = new ArrayList<>();
    if (carbonTable.isHivePartitionTable()) {
      String partitionPath =
          FileFactory.getCarbonFile(jobContext.getConfiguration().get(FileInputFormat.INPUT_DIR))
              .getAbsolutePath();
      partitionNames.add(partitionPath.substring(carbonTable.getTablePath().length()));
      List<PartitionSpec> partitionSpec = new ArrayList<>();
      partitionSpec.add(new PartitionSpec(partitionNames, partitionPath));
      setPartitionsToPrune(jobContext.getConfiguration(), partitionSpec);
    }
    try {
      setFilterPredicates(jobContext.getConfiguration(), carbonTable);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    CarbonInputFormat<Void> carbonInputFormat;
    if (carbonTable.isTransactionalTable()) {
      carbonInputFormat = new CarbonTableInputFormat<>();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3771
      jobContext.getConfiguration().set(CARBON_TRANSACTIONAL_TABLE, "true");
    } else {
      carbonInputFormat = new CarbonFileInputFormat<>();
    }
    List<org.apache.hadoop.mapreduce.InputSplit> splitList =
        carbonInputFormat.getSplits(jobContext);
    InputSplit[] splits = new InputSplit[splitList.size()];
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1158
    CarbonInputSplit split;
    for (int i = 0; i < splitList.size(); i++) {
      split = (CarbonInputSplit) splitList.get(i);
      CarbonHiveInputSplit inputSplit = new CarbonHiveInputSplit(split.getSegmentId(),
          split.getPath(), split.getStart(), split.getLength(),
          split.getLocations(), split.getNumberOfBlocklets(),
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3364
          split.getVersion(), split.getBlockStorageIdMap(), split.getDetailInfo());
      splits[i] = inputSplit;
    }
    return splits;
  }

  protected void setFilterPredicates(Configuration configuration, CarbonTable carbonTable) {
    try {
      String expr = configuration.get(TableScanDesc.FILTER_EXPR_CONF_STR);
      if (expr == null) {
        return;
      }
      ExprNodeGenericFuncDesc exprNodeGenericFuncDesc =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3719
          SerializationUtilities.deserializeObject(expr, ExprNodeGenericFuncDesc.class);
      LOGGER.debug("hive expression:" + exprNodeGenericFuncDesc.getGenericUDF());
      LOGGER.debug("hive expression string:" + exprNodeGenericFuncDesc.getExprString());
      Expression expression = Hive2CarbonExpression.convertExprHive2Carbon(exprNodeGenericFuncDesc);
      if (expression == null) {
        return;
      }
      LOGGER.debug("carbon expression:" + expression.getString());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3704
      IndexFilter filter = new IndexFilter(carbonTable, expression, true);
      CarbonInputFormat.setFilterPredicates(configuration, filter);
    } catch (Exception e) {
      throw new RuntimeException("Error while reading filter expression", e);
    }
  }

  @Override
  public RecordReader<Void, ArrayWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf,
      Reporter reporter) throws IOException {
    String path = null;
    if (inputSplit instanceof CarbonHiveInputSplit) {
      path = ((CarbonHiveInputSplit) inputSplit).getPath().toString();
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3687
    QueryModel queryModel;
    try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3441
      jobConf.set(DATABASE_NAME, "_dummyDb_" + UUID.randomUUID().toString());
      jobConf.set(TABLE_NAME, "_dummyTable_" + UUID.randomUUID().toString());
      queryModel = getQueryModel(jobConf, path);
    } catch (InvalidConfigurationException e) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3107
      LOGGER.error("Failed to create record reader: " + e.getMessage(), e);
      return null;
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3605
    CarbonReadSupport<ArrayWritable> readSupport = new WritableReadSupport<>();
    return new CarbonHiveRecordReader(queryModel, readSupport, inputSplit, jobConf);
  }

  private QueryModel getQueryModel(Configuration configuration, String path)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1994
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1995
      throws IOException, InvalidConfigurationException {
    CarbonTable carbonTable = getCarbonTable(configuration, path);
    String projectionString = getProjection(configuration, carbonTable);
    String[] projectionColumns = projectionString.split(",");
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3687
    return new QueryModelBuilder(carbonTable)
        .projectColumns(projectionColumns)
        .filterExpression(getFilterPredicates(configuration))
        .dataConverter(new DataTypeConverterImpl())
        .build();
  }

  /**
   * Return the Projection for the CarbonQuery.
   */
  private String getProjection(Configuration configuration, CarbonTable carbonTable) {
    // query plan includes projection column
    String projection = getColumnProjection(configuration);
    if (projection == null) {
      projection = configuration.get("hive.io.file.readcolumn.names");
    }
    List<CarbonColumn> carbonColumns = carbonTable.getCreateOrderColumn();
    List<String> carbonColumnNames = new ArrayList<>();
    StringBuilder allColumns = new StringBuilder();
    StringBuilder projectionColumns = new StringBuilder();
    for (CarbonColumn column : carbonColumns) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1465
      carbonColumnNames.add(column.getColName().toLowerCase());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3687
      allColumns.append(column.getColName()).append(",");
    }

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3441
    if (null != projection && !projection.equals("")) {
      String[] columnNames = projection.split(",");
      //verify that the columns parsed by Hive exist in the table
      for (String col : columnNames) {
        //show columns command will return these data
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1465
        if (carbonColumnNames.contains(col.toLowerCase())) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3687
          projectionColumns.append(col).append(",");
        }
      }
      return projectionColumns.substring(0, projectionColumns.lastIndexOf(","));
    } else {
      return allColumns.substring(0, allColumns.lastIndexOf(","));
    }
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf) {
    return true;
  }
}
