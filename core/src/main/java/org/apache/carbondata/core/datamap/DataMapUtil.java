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

package org.apache.carbondata.core.datamap;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.indexstore.ExtendedBlocklet;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.SegmentManager;
import org.apache.carbondata.core.statusmanager.SegmentManagerHelper;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentsHolder;
import org.apache.carbondata.core.util.ObjectSerializationUtil;

import org.apache.hadoop.conf.Configuration;

public class DataMapUtil {

  private static final String DATA_MAP_DSTR = "mapreduce.input.carboninputformat.datamapdstr";

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataMapUtil.class.getName());

  /**
   * Creates instance for the DataMap Job class
   *
   * @param className
   * @return
   */
  public static Object createDataMapJob(String className) {
    try {
      return Class.forName(className).getDeclaredConstructors()[0].newInstance();
    } catch (Exception e) {
      LOGGER.error(e);
      return null;
    }
  }

  /**
   * This method sets the datamapJob in the configuration
   * @param configuration
   * @param dataMapJob
   * @throws IOException
   */
  public static void setDataMapJob(Configuration configuration, Object dataMapJob)
      throws IOException {
    if (dataMapJob != null) {
      String toString = ObjectSerializationUtil.convertObjectToString(dataMapJob);
      configuration.set(DATA_MAP_DSTR, toString);
    }
  }

  /**
   * get datamap job from the configuration
   * @param configuration job configuration
   * @return DataMap Job
   * @throws IOException
   */
  public static DataMapJob getDataMapJob(Configuration configuration) throws IOException {
    String jobString = configuration.get(DATA_MAP_DSTR);
    if (jobString != null) {
      return (DataMapJob) ObjectSerializationUtil.convertStringToObject(jobString);
    }
    return null;
  }

  /**
   * This method gets the datamapJob and call execute , this job will be launched before clearing
   * datamaps from driver side during drop table and drop datamap and clears the datamap in executor
   * side
   * @param carbonTable
   * @throws IOException
   */
  public static void executeDataMapJobForClearingDataMaps(CarbonTable carbonTable)
      throws IOException {
    String dataMapJobClassName = "org.apache.carbondata.spark.rdd.SparkDataMapJob";
    DataMapJob dataMapJob = (DataMapJob) createDataMapJob(dataMapJobClassName);
    String className = "org.apache.carbondata.core.datamap.DistributableDataMapFormat";
    SegmentsHolder segmentsHolder =
        new SegmentManager().getAllSegments(carbonTable.getAbsoluteTableIdentifier());
    List<Segment> validSegments = segmentsHolder.getValidSegments();
    List<Segment> invalidSegments = segmentsHolder.getInvalidSegments();
    DataMapExprWrapper dataMapExprWrapper = null;
    if (DataMapStoreManager.getInstance().getAllDataMap(carbonTable).size() > 0) {
      DataMapChooser dataMapChooser = new DataMapChooser(carbonTable);
      dataMapExprWrapper = dataMapChooser.getAllDataMapsForClear(carbonTable);
    } else {
      return;
    }
    DistributableDataMapFormat dataMapFormat =
        createDataMapJob(carbonTable, dataMapExprWrapper, validSegments, invalidSegments, null,
            className, true);
    dataMapJob.execute(dataMapFormat, null);
  }

  private static DistributableDataMapFormat createDataMapJob(CarbonTable carbonTable,
      DataMapExprWrapper dataMapExprWrapper, List<Segment> validsegments,
      List<Segment> invalidSegments, List<PartitionSpec> partitionsToPrune, String clsName,
      boolean isJobToClearDataMaps) {
    try {
      Constructor<?> cons = Class.forName(clsName).getDeclaredConstructors()[0];
      return (DistributableDataMapFormat) cons
          .newInstance(carbonTable, dataMapExprWrapper, validsegments, invalidSegments,
              partitionsToPrune, isJobToClearDataMaps);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * this method gets the datamapJob and call execute of that job, this will be launched for
   * distributed CG or FG
   * @return list of Extended blocklets after pruning
   */
  public static List<ExtendedBlocklet> executeDataMapJob(CarbonTable carbonTable,
      FilterResolverIntf resolver, List<Segment> validSegments,
      DataMapExprWrapper dataMapExprWrapper, DataMapJob dataMapJob,
      List<PartitionSpec> partitionsToPrune) throws IOException {
    String className = "org.apache.carbondata.core.datamap.DistributableDataMapFormat";
    List<Segment> invalidSegments =
        new SegmentManager().getInvalidSegments(carbonTable.getAbsoluteTableIdentifier())
            .getInvalidSegments();
    DistributableDataMapFormat dataMapFormat =
        createDataMapJob(carbonTable, dataMapExprWrapper, validSegments, invalidSegments,
            partitionsToPrune, className, false);
    List<ExtendedBlocklet> prunedBlocklets = dataMapJob.execute(dataMapFormat, resolver);
    // Apply expression on the blocklets.
    prunedBlocklets = dataMapExprWrapper.pruneBlocklets(prunedBlocklets);
    return prunedBlocklets;
  }

}
