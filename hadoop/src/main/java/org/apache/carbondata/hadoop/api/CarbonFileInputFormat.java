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

package org.apache.carbondata.hadoop.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.indexstore.Blocklet;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMap;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsRecorder;
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.security.TokenCache;

/**
 * Input format of CarbonData file.
 *
 * @param <T>
 */
public class CarbonFileInputFormat<T> extends AbstractCarbonInputFormat<T> {

  private static final Log LOG = LogFactory.getLog(CarbonFileInputFormat.class);

  // a cache for carbon table, it will be used in task side
  private CarbonTable carbonTable;

  /**
   * Get the cached CarbonTable or create it by TableInfo in `configuration`
   */
  @Override protected CarbonTable getOrCreateCarbonTable(Configuration configuration)
      throws IOException {
    if (carbonTable == null) {
      // carbon table should be created either from deserialized table info (schema saved in
      // hive metastore) or by reading schema in HDFS (schema saved in HDFS)
      TableInfo tableInfo = getTableInfo(configuration);
      CarbonTable carbonTable;
      if (tableInfo != null) {
        carbonTable = CarbonTable.buildFromTableInfo(tableInfo);
      } else {
        throw new IOException("configuration " + TABLE_INFO + " not set.");
      }
      this.carbonTable = carbonTable;
      return carbonTable;
    } else {
      return this.carbonTable;
    }
  }

  private AbsoluteTableIdentifier getAbsoluteTableIdentifier(Configuration configuration)
      throws IOException {
    return getOrCreateCarbonTable(configuration).getAbsoluteTableIdentifier();
  }


  /**
   * Read data in one segment. For alter table partition statement
   * @param job
   * @return
   * @throws IOException
   */
  @Override public List<InputSplit> getSplits(JobContext job) throws IOException {
    AbsoluteTableIdentifier identifier = getAbsoluteTableIdentifier(job.getConfiguration());

    // TODO: get invalid timestamp list from configuration
    List<UpdateVO> invalidTimestampsList = new ArrayList<>();

    try {

      // process and resolve the expression
      Expression filter = getFilterPredicates(job.getConfiguration());
      CarbonTable carbonTable = getOrCreateCarbonTable(job.getConfiguration());
      // this will be null in case of corrupt schema file.
      if (null == carbonTable) {
        throw new IOException("Missing/Corrupt schema file for table.");
      }

      CarbonInputFormatUtil.processFilterExpression(filter, carbonTable);

      TableProvider tableProvider = new SingleTableProvider(carbonTable);

      FilterResolverIntf filterInterface =
          CarbonInputFormatUtil.resolveFilter(filter, identifier, tableProvider);
      // do block filtering and get split
      List<InputSplit> splits = getDataBlocksOfInputPath(job, identifier, filterInterface);
      // pass the invalid segment to task side in order to remove index entry in task side
      for (InputSplit split : splits) {
        ((CarbonInputSplit) split).setInvalidTimestampRange(invalidTimestampsList);
      }
      return splits;
    } catch (IOException e) {
      throw new RuntimeException("Can't get splits of the target segment ", e);
    }
  }

  /**
   * get data blocks of given segment
   */
  private List<InputSplit> getDataBlocksOfInputPath(JobContext job,
      AbsoluteTableIdentifier absoluteTableIdentifier, FilterResolverIntf resolver)
      throws IOException {

    QueryStatisticsRecorder recorder = CarbonTimeStatisticsFactory.createDriverRecorder();
    QueryStatistic statistic = new QueryStatistic();
    String[] inputDirs = getInputDir(job.getConfiguration());
    Map<String, String> options = new HashMap<String, String>(1);
    options.put(TableDataMap.SEGMENT_ID_AS_PATH, "true");
    List<String> segIds = new ArrayList<String>(inputDirs.length);
    for (String inputDir: inputDirs) {
      segIds.add(inputDir);
    }
    // get tokens for all the required FileSystem for table path
    TokenCache.obtainTokensForNamenodes(job.getCredentials(),
        new Path[] { new Path(absoluteTableIdentifier.getTablePath()) }, job.getConfiguration());

    TableDataMap blockletMap = DataMapStoreManager.getInstance()
        .getDataMap(absoluteTableIdentifier, BlockletDataMap.NAME,
            BlockletDataMapFactory.class.getName(), options);
    DataMapJob dataMapJob = getDataMapJob(job.getConfiguration());
    List<Blocklet> prunedBlocklets;
    if (dataMapJob != null) {
      DistributableDataMapFormat datamapDstr =
          new DistributableDataMapFormat(absoluteTableIdentifier, BlockletDataMap.NAME,
              segIds, BlockletDataMapFactory.class.getName(), true);
      prunedBlocklets = dataMapJob.execute(datamapDstr, resolver);
    } else {
      prunedBlocklets = blockletMap.prune(segIds, resolver);
    }

    List<InputSplit> resultFilterredBlocks = new ArrayList<>();
    for (Blocklet blocklet : prunedBlocklets) {
      resultFilterredBlocks.add(convertToCarbonInputSplit(blocklet));
    }
    statistic
        .addStatistics(QueryStatisticsConstants.LOAD_BLOCKS_DRIVER, System.currentTimeMillis());
    recorder.recordStatisticsForDriver(statistic, job.getConfiguration().get("query.id"));
    return resultFilterredBlocks;
  }
}
