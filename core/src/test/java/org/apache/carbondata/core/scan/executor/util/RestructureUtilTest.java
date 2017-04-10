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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.infos.MeasureInfo;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class RestructureUtilTest {

  @Test public void testToGetUpdatedQueryDimension() {
    BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
    List<Encoding> encodingList = new ArrayList<Encoding>();
    encodingList.add(Encoding.DICTIONARY);
    ColumnSchema columnSchema1 = new ColumnSchema();
    columnSchema1.setColumnName("Id");
    columnSchema1.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema1.setEncodingList(encodingList);
    ColumnSchema columnSchema2 = new ColumnSchema();
    columnSchema2.setColumnName("Name");
    columnSchema2.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema2.setEncodingList(encodingList);
    ColumnSchema columnSchema3 = new ColumnSchema();
    columnSchema3.setColumnName("Age");
    columnSchema3.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema3.setEncodingList(encodingList);
    ColumnSchema columnSchema4 = new ColumnSchema();
    columnSchema4.setColumnName("Salary");
    columnSchema4.setColumnUniqueId(UUID.randomUUID().toString());
    columnSchema4.setEncodingList(encodingList);
    ColumnSchema columnSchema5 = new ColumnSchema();
    columnSchema5.setColumnName("Address");
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

    QueryDimension queryDimension1 = new QueryDimension("Id");
    queryDimension1.setDimension(tableBlockDimension1);
    QueryDimension queryDimension2 = new QueryDimension("Name");
    queryDimension2.setDimension(tableComplexDimension2);
    QueryDimension queryDimension3 = new QueryDimension("Address");
    queryDimension3.setDimension(new CarbonDimension(columnSchema5, 3, 3, 3, 3));

    List<QueryDimension> queryDimensions =
        Arrays.asList(queryDimension1, queryDimension2, queryDimension3);

    List<QueryDimension> result = null;
    result = RestructureUtil
        .createDimensionInfoAndGetCurrentBlockQueryDimension(blockExecutionInfo, queryDimensions,
            tableBlockDimensions, tableComplexDimensions);
    List<CarbonDimension> resultDimension = new ArrayList<>(result.size());
    for (QueryDimension queryDimension : result) {
      resultDimension.add(queryDimension.getDimension());
    }
    assertThat(resultDimension,
        is(equalTo(Arrays.asList(queryDimension1.getDimension(), queryDimension2.getDimension()))));
  }

  @Test public void testToGetAggregatorInfos() {
    ColumnSchema columnSchema1 = new ColumnSchema();
    columnSchema1.setColumnName("Id");
    columnSchema1.setDataType(DataType.STRING);
    columnSchema1.setColumnUniqueId(UUID.randomUUID().toString());
    ColumnSchema columnSchema2 = new ColumnSchema();
    columnSchema2.setColumnName("Name");
    columnSchema2.setDataType(DataType.STRING);
    columnSchema2.setColumnUniqueId(UUID.randomUUID().toString());
    ColumnSchema columnSchema3 = new ColumnSchema();
    columnSchema3.setColumnName("Age");
    columnSchema3.setDataType(DataType.STRING);
    columnSchema3.setColumnUniqueId(UUID.randomUUID().toString());

    CarbonMeasure carbonMeasure1 = new CarbonMeasure(columnSchema1, 1);
    CarbonMeasure carbonMeasure2 = new CarbonMeasure(columnSchema2, 2);
    CarbonMeasure carbonMeasure3 = new CarbonMeasure(columnSchema3, 3);
    carbonMeasure3.getColumnSchema().setDefaultValue("3".getBytes());
    List<CarbonMeasure> currentBlockMeasures = Arrays.asList(carbonMeasure1, carbonMeasure2);

    QueryMeasure queryMeasure1 = new QueryMeasure("Id");
    queryMeasure1.setMeasure(carbonMeasure1);
    QueryMeasure queryMeasure2 = new QueryMeasure("Name");
    queryMeasure2.setMeasure(carbonMeasure2);
    QueryMeasure queryMeasure3 = new QueryMeasure("Age");
    queryMeasure3.setMeasure(carbonMeasure3);
    List<QueryMeasure> queryMeasures = Arrays.asList(queryMeasure1, queryMeasure2, queryMeasure3);
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
