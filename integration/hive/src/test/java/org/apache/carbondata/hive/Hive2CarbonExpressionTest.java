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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapFilter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.hadoop.api.CarbonFileInputFormat;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.testutil.StoreCreator;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;

public class Hive2CarbonExpressionTest {
  private static StoreCreator creator;
  private static CarbonLoadModel loadModel;
  private static CarbonTable table;
  static {
    CarbonProperties.getInstance().
        addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "/tmp/carbon/badrecords");
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION, "/tmp/carbon/");
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "Hive2CarbonExpressionTest");
    try {
      creator = new StoreCreator(new File("target/store").getAbsolutePath(),
          new File("../../hadoop/src/test/resources/data.csv").getCanonicalPath());
      loadModel = creator.createCarbonStore();
      table=loadModel.getCarbonDataLoadSchema().getCarbonTable();
      table.setTransactionalTable(false);
    } catch (Exception e) {
      Assert.fail("create table failed: " + e.getMessage());
    }
  }
  @Test
  public void testEqualHiveFilter() throws IOException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "1001");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqual(), children);
    Configuration configuration=new Configuration();
    configuration.set("mapreduce.input.carboninputformat.filter.predicate",
        SerializationUtilities.serializeExpression(node));
    CarbonInputFormat.setFilterPredicates(configuration,
        new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node)));

    final Job job = new Job(new JobConf(configuration));
    final CarbonTableInputFormat format = new CarbonTableInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertTrue(list.size() == 0);
  }

  @Test
  public void testNotEqualHiveFilter() throws IOException {
    ExprNodeDesc column = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "500");
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPNotEqual(), children);
    Configuration configuration=new Configuration();
    CarbonInputFormat.setFilterPredicates(configuration,new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node)));

    final Job job = new Job(new JobConf(configuration));
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertTrue(list.size() == 1);
  }

  @Test
  public void testOrHiveFilter() throws IOException {
    ExprNodeDesc column1 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    ExprNodeDesc constant1 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "500");
    List<ExprNodeDesc> children1 = Lists.newArrayList();
    children1.add(column1);
    children1.add(constant1);
    ExprNodeGenericFuncDesc node1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqual(), children1);
    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "4999999");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    List<ExprNodeDesc> children3 = Lists.newArrayList();
    ExprNodeGenericFuncDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqual(), children2);
    children3.add(node1);
    children3.add(node2);
    ExprNodeGenericFuncDesc node3=new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPOr(),children3);
    Configuration configuration=new Configuration();
    CarbonInputFormat.setFilterPredicates(configuration,new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node3)));
    final Job job = new Job(new JobConf(configuration));
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertEquals(1, list.size());
  }

  @Test
  public void testAndHiveFilter() throws IOException {
    ExprNodeDesc column1 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    ExprNodeDesc constant1 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "500");
    List<ExprNodeDesc> children1 = Lists.newArrayList();
    children1.add(column1);
    children1.add(constant1);
    ExprNodeGenericFuncDesc node1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqual(), children1);
    ExprNodeDesc column2 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "4999999");
    List<ExprNodeDesc> children2 = Lists.newArrayList();
    children2.add(column2);
    children2.add(constant2);
    List<ExprNodeDesc> children3 = Lists.newArrayList();
    ExprNodeGenericFuncDesc node2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqual(), children2);
    children3.add(node1);
    children3.add(node2);
    ExprNodeGenericFuncDesc node3=new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPAnd(),children3);
    Configuration configuration=new Configuration();
    CarbonInputFormat.setFilterPredicates(configuration,new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node3)));
    final Job job = new Job(new JobConf(configuration));
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertEquals(0, list.size());
  }

  @Test
  public void testNullHiveFilter() throws IOException {
    ExprNodeDesc column1 = new ExprNodeColumnDesc(TypeInfoFactory.booleanTypeInfo, "name", null, false);
    List<ExprNodeDesc> children1 = Lists.newArrayList();
    children1.add(column1);
    ExprNodeGenericFuncDesc node1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPNull(), children1);
    Configuration configuration=new Configuration();
    CarbonInputFormat.setFilterPredicates(configuration,new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node1)));
    final Job job = new Job(new JobConf(configuration));
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertEquals(0, list.size());
  }

  @Test
  public void testNotNullHiveFilter() throws IOException {
    ExprNodeDesc column1 = new ExprNodeColumnDesc(TypeInfoFactory.booleanTypeInfo, "name", null, false);
    List<ExprNodeDesc> children1 = Lists.newArrayList();
    children1.add(column1);
    ExprNodeGenericFuncDesc node1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPNotNull(), children1);
    Configuration configuration=new Configuration();
    CarbonInputFormat.setFilterPredicates(configuration,new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node1)));
    final Job job = new Job(new JobConf(configuration));
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertTrue(list.size() == 1);
  }

  @Test
  public void testInHiveFilter() throws IOException {
    ExprNodeDesc column1 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    List<ExprNodeDesc> children1 = Lists.newArrayList();
    ExprNodeDesc constant1 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "500");
    ExprNodeDesc constant2 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "600");
    ExprNodeDesc constant3 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "700");
    children1.add(column1);
    children1.add(constant1);
    children1.add(constant2);
    children1.add(constant3);
    ExprNodeGenericFuncDesc node1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFIn(), children1);
    Configuration configuration=new Configuration();
    CarbonInputFormat.setFilterPredicates(configuration,new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node1)));
    final Job job = new Job(new JobConf(configuration));
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertEquals(1, list.size());
  }

  @Test
  public void testEqualOrGreaterThanHiveFilter() throws IOException {
    ExprNodeDesc column1 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    List<ExprNodeDesc> children1 = Lists.newArrayList();
    ExprNodeDesc constant1 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "0");
    children1.add(column1);
    children1.add(constant1);
    ExprNodeGenericFuncDesc node1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), children1);
    Configuration configuration=new Configuration();
    CarbonInputFormat.setFilterPredicates(configuration,new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node1)));
    final Job job = new Job(new JobConf(configuration));
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertEquals(1, list.size());

  }

  @Test
  public void testEqualOrLessThanEqualsHiveFilter() throws IOException {
    ExprNodeDesc column1 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    List<ExprNodeDesc> children1 = Lists.newArrayList();
    ExprNodeDesc constant1 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "1000");
    children1.add(column1);
    children1.add(constant1);
    ExprNodeGenericFuncDesc node1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), children1);
    Configuration configuration=new Configuration();
    CarbonInputFormat.setFilterPredicates(configuration,new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node1)));
    final Job job = new Job(new JobConf(configuration));
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertEquals(1, list.size());

  }

  @Test
  public void testLessThanEqualsHiveFilter() throws IOException {
    ExprNodeDesc column1 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    List<ExprNodeDesc> children1 = Lists.newArrayList();
    ExprNodeDesc constant1 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "0");
    children1.add(column1);
    children1.add(constant1);
    ExprNodeGenericFuncDesc node1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), children1);
    Configuration configuration=new Configuration();
    CarbonInputFormat.setFilterPredicates(configuration,new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node1)));
    final Job job = new Job(new JobConf(configuration));
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertEquals(0, list.size());

  }

  @Test
  public void testGreaterThanHiveFilter() throws IOException {
    ExprNodeDesc column1 = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "id", null, false);
    List<ExprNodeDesc> children1 = Lists.newArrayList();
    ExprNodeDesc constant1 = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, "1001");
    children1.add(column1);
    children1.add(constant1);
    ExprNodeGenericFuncDesc node1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), children1);
    Configuration configuration=new Configuration();
    CarbonInputFormat.setFilterPredicates(configuration,new DataMapFilter(table, Hive2CarbonExpression.convertExprHive2Carbon(node1)));
    final Job job = new Job(new JobConf(configuration));
    final CarbonFileInputFormat format = new CarbonFileInputFormat();
    format.setTableInfo(job.getConfiguration(), table.getTableInfo());
    format.setTablePath(job.getConfiguration(), table.getTablePath());
    format.setTableName(job.getConfiguration(), table.getTableName());
    format.setDatabaseName(job.getConfiguration(), table.getDatabaseName());

    List<InputSplit> list= format.getSplits(job);
    Assert.assertEquals(0, list.size());

  }
}
