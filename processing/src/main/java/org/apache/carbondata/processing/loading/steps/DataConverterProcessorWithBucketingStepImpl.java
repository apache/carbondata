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

package org.apache.carbondata.processing.loading.steps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.BucketingInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.RowConverter;
import org.apache.carbondata.processing.loading.partition.Partitioner;
import org.apache.carbondata.processing.loading.partition.impl.HashPartitionerImpl;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;

/**
 * Replace row data fields with dictionary values if column is configured dictionary encoded.
 * And nondictionary columns as well as complex columns will be converted to byte[].
 */
public class DataConverterProcessorWithBucketingStepImpl extends DataConverterProcessorStepImpl {

  private Partitioner<Object[]> partitioner;

  public DataConverterProcessorWithBucketingStepImpl(CarbonDataLoadConfiguration configuration,
      AbstractDataLoadProcessorStep child) {
    super(configuration, child);
  }

  @Override
  public void initialize() throws IOException {
    super.initialize();
    List<Integer> indexes = new ArrayList<>();
    List<ColumnSchema> columnSchemas = new ArrayList<>();
    DataField[] inputDataFields = getOutput();
    BucketingInfo bucketingInfo = configuration.getBucketingInfo();
    for (int i = 0; i < inputDataFields.length; i++) {
      for (int j = 0; j < bucketingInfo.getListOfColumns().size(); j++) {
        if (inputDataFields[i].getColumn().getColName()
            .equals(bucketingInfo.getListOfColumns().get(j).getColumnName())) {
          indexes.add(i);
          columnSchemas.add(inputDataFields[i].getColumn().getColumnSchema());
          break;
        }
      }
    }
    partitioner =
        new HashPartitionerImpl(indexes, columnSchemas, bucketingInfo.getNumberOfBuckets());
  }

  /**
   * Process the batch of rows as per the step logic.
   *
   * @param rowBatch
   * @return processed row.
   */
  protected CarbonRowBatch processRowBatch(CarbonRowBatch rowBatch, RowConverter localConverter) {
    while (rowBatch.hasNext()) {
      CarbonRow row = rowBatch.next();
      short bucketNumber = (short) partitioner.getPartition(row.getData());
      CarbonRow convertRow = localConverter.convert(row);
      convertRow.bucketNumber = bucketNumber;
      rowBatch.setPreviousRow(convertRow);
    }
    rowCounter.getAndAdd(rowBatch.getSize());
    // reuse the origin batch
    rowBatch.rewind();
    return rowBatch;
  }

}
