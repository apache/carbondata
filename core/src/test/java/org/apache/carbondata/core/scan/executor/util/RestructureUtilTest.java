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
package org.apache.carbondata.core.scan.executor.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.infos.MeasureInfo;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RestructureUtilTest {

  @Test public void testToGetUpdatedQueryDimension() {
    BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
    List<Encoding> encodingList = new ArrayList<Encoding>();
    encodingList.add(Encoding.DICTIONARY);
    ColumnSchema columnSchema1 = new ColumnSchema();
    columnSchema1.setColumnName("Id");
    columnSchema1.setDataType(DataTypes.STRING);
    columnSchema1.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema1.setEncodingList(encodingList);
    ColumnSchema columnSchema2 = new ColumnSchema();
    columnSchema2.setColumnName("Name");
    columnSchema2.setDataType(DataTypes.STRING);
    columnSchema2.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema2.setEncodingList(encodingList);
    ColumnSchema columnSchema3 = new ColumnSchema();
    columnSchema3.setColumnName("Age");
    columnSchema3.setDataType(DataTypes.INT);
    columnSchema3.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema3.setEncodingList(encodingList);
    ColumnSchema columnSchema4 = new ColumnSchema();
    columnSchema4.setColumnName("Salary");
    columnSchema4.setDataType(DataTypes.INT);
    columnSchema4.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema4.setEncodingList(encodingList);
    ColumnSchema columnSchema5 = new ColumnSchema();
    columnSchema5.setColumnName("Address");
    columnSchema5.setDataType(DataTypes.STRING);
    columnSchema5.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema5.setEncodingList(encodingList);

    CarbonDimension tableBlockDimension1 = new CarbonDimension(columnSchema1, 1, 1, 1, 1);
    CarbonDimension tableBlockDimension2 = new CarbonDimension(columnSchema2, 5, 5, 5, 5);
    List<CarbonDimension> tableBlockDimensions =
        Arrays.asList(tableBlockDimension1, tableBlockDimension2);

    CarbonDimension tableComplexDimension1 = new CarbonDimension(columnSchema3, 4, 4, 4, 4);
    CarbonDimension tableComplexDimension2 = new CarbonDimension(columnSchema4, 2, 2, 2, 2);
    List<CarbonDimension> tableComplexDimensions =
        Arrays.asList(tableComplexDimension1, tableComplexDimension2);

    ProjectionDimension queryDimension1 = new ProjectionDimension(tableBlockDimension1);
    ProjectionDimension queryDimension2 = new ProjectionDimension(tableComplexDimension2);
    ProjectionDimension
        queryDimension3 = new ProjectionDimension(new CarbonDimension(columnSchema5, 3, 3, 3, 3));

    List<ProjectionDimension> queryDimensions =
        Arrays.asList(queryDimension1, queryDimension2, queryDimension3);

    List<ProjectionDimension> result = null;
    result = RestructureUtil
        .createDimensionInfoAndGetCurrentBlockQueryDimension(blockExecutionInfo, queryDimensions,
            tableBlockDimensions, tableComplexDimensions);
    List<CarbonDimension> resultDimension = new ArrayList<>(result.size());
    for (ProjectionDimension queryDimension : result) {
      resultDimension.add(queryDimension.getDimension());
    }
    assertThat(resultDimension,
        is(equalTo(Arrays.asList(queryDimension1.getDimension(), queryDimension2.getDimension()))));
  }

  @Test public void testToGetAggregatorInfos() {
    ColumnSchema columnSchema1 = new ColumnSchema();
    columnSchema1.setColumnName("Id");
    columnSchema1.setDataType(DataTypes.STRING);
    columnSchema1.setColumnUniqueId(UUID.randomUUID().toString());
    ColumnSchema columnSchema2 = new ColumnSchema();
    columnSchema2.setColumnName("Name");
    columnSchema2.setDataType(DataTypes.STRING);
    columnSchema2.setColumnUniqueId(UUID.randomUUID().toString());
    ColumnSchema columnSchema3 = new ColumnSchema();
    columnSchema3.setColumnName("Age");
    columnSchema3.setDataType(DataTypes.STRING);
    columnSchema3.setColumnUniqueId(UUID.randomUUID().toString());

    CarbonMeasure carbonMeasure1 = new CarbonMeasure(columnSchema1, 1);
    CarbonMeasure carbonMeasure2 = new CarbonMeasure(columnSchema2, 2);
    CarbonMeasure carbonMeasure3 = new CarbonMeasure(columnSchema3, 3);
    carbonMeasure3.getColumnSchema().setDefaultValue("3".getBytes());
    List<CarbonMeasure> currentBlockMeasures = Arrays.asList(carbonMeasure1, carbonMeasure2);

    ProjectionMeasure queryMeasure1 = new ProjectionMeasure(carbonMeasure1);
    ProjectionMeasure queryMeasure2 = new ProjectionMeasure(carbonMeasure2);
    ProjectionMeasure queryMeasure3 = new ProjectionMeasure(carbonMeasure3);
    List<ProjectionMeasure> queryMeasures = Arrays.asList(queryMeasure1, queryMeasure2, queryMeasure3);
    BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
    RestructureUtil.createMeasureInfoAndGetCurrentBlockQueryMeasures(blockExecutionInfo, queryMeasures,
        currentBlockMeasures);
    MeasureInfo measureInfo = blockExecutionInfo.getMeasureInfo();
    boolean[] measuresExist = { true, true, false };
    assertThat(measureInfo.getMeasureExists(), is(equalTo(measuresExist)));
    Object[] defaultValues = { null, null, 3.0 };
    assertThat(measureInfo.getDefaultValues(), is(equalTo(defaultValues)));
  }
}
