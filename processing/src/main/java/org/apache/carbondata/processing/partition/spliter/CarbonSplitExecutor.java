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

package org.apache.carbondata.processing.partition.spliter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.scan.result.iterator.PartitionSpliterRawResultIterator;
import org.apache.carbondata.core.util.DataTypeConverter;

/**
 * Used to read carbon blocks when add/split partition
 */
public class CarbonSplitExecutor extends AbstractCarbonQueryExecutor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonSplitExecutor.class.getName());

  public CarbonSplitExecutor(Map<String, TaskBlockInfo> segmentMapping, CarbonTable carbonTable) {
    this.segmentMapping = segmentMapping;
    this.carbonTable = carbonTable;
  }

  public List<PartitionSpliterRawResultIterator> processDataBlocks(
      String segmentId, DataTypeConverter converter)
      throws QueryExecutionException, IOException {
    List<TableBlockInfo> list = null;
    queryModel = new QueryModelBuilder(carbonTable)
        .projectAllColumns()
        .dataConverter(converter)
        .enableForcedDetailRawQuery()
        .build();
    List<PartitionSpliterRawResultIterator> resultList
        = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    TaskBlockInfo taskBlockInfo = segmentMapping.get(segmentId);
    Set<String> taskBlockListMapping = taskBlockInfo.getTaskSet();
    for (String task : taskBlockListMapping) {
      list = taskBlockInfo.getTableBlockInfoList(task);
      LOGGER.info("for task -" + task + "-block size is -" + list.size());
      queryModel.setTableBlockInfos(list);
      resultList.add(new PartitionSpliterRawResultIterator(executeBlockList(list)));
    }
    return resultList;
  }
}
